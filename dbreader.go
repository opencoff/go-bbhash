// dbreader.go -- Constant DB built on top of the BBHash MPH
//
// Author: Sudhi Herle <sudhi@herle.net>
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.

package bbhash

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"crypto/sha512"
	"crypto/subtle"

	"github.com/hashicorp/golang-lru"
	"github.com/opencoff/go-fasthash"
)

// DBReader represents the query interface for a previously constructed
// constant database (built using NewDBWriter()). The only meaningful
// operation on such a database is Lookup().
type DBReader struct {
	bb *BBHash

	salt    uint64
	saltkey []byte

	cache *lru.ARCCache

	// memory mapped offset table
	offsets []uint64

	nkeys uint64

	fd *os.File
	fn string
}

// NewDBReader reads a previously construct database in file 'fn' and prepares
// it for querying. Records are opportunistically cached after reading from disk.
// We retain upto 'cache' number of records in memory (default 128).
func NewDBReader(fn string, cache int) (rd *DBReader, err error) {
	fd, err := os.Open(fn)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			fd.Close()
		}
	}()

	// Number of records to cache
	if cache <= 0 {
		cache = 128
	}

	rd = &DBReader{
		saltkey: make([]byte, 16),
		fd:      fd,
		fn:      fn,
	}

	var st os.FileInfo
	var hdr *header
	var n int

	st, err = fd.Stat()
	if err != nil {
		return nil, fmt.Errorf("%s: can't stat: %s", fn, err)
	}

	if st.Size() < (64 + 32) {
		return nil, fmt.Errorf("%s: file too small or corrupted", fn)
	}

	var hdrb [64]byte

	n, err = fd.Read(hdrb[:])
	if err != nil {
		return nil, fmt.Errorf("%s: can't read header: %s", fn, err)
	}
	if n != 64 {
		return nil, fmt.Errorf("%s: short read of header; exp 64, saw %d", fn, n)
	}

	hdr, err = rd.decodeHeader(hdrb[:], st.Size())
	if err != nil {
		return nil, err
	}

	err = rd.verifyChecksum(hdrb[:], hdr.offtbl, st.Size())
	if err != nil {
		return nil, err
	}

	// sanity check - even though we have verified the strong checksum
	tblsz := hdr.nkeys * 8
	if uint64(st.Size()) < (64 + 32 + tblsz) {
		return nil, fmt.Errorf("%s: corrupt header", fn)
	}

	rd.cache, err = lru.NewARC(cache)
	if err != nil {
		return nil, err
	}

	// Now, we are certain that the header, the offset-table and bbhash bits are
	// all valid and uncorrupted.

	// mmap the offset table and return.
	rd.offsets, err = mmapUint64(int(fd.Fd()), hdr.offtbl, int(hdr.nkeys), syscall.PROT_READ, syscall.MAP_PRIVATE)
	if err != nil {
		return nil, fmt.Errorf("%s: can't mmap offset table (off %d, sz %d): %s",
			fn, hdr.offtbl, hdr.nkeys*8, err)
	}

	// The hash table starts after the offset table.
	fd.Seek(int64(hdr.offtbl)+int64(hdr.nkeys*8), 0)
	rd.bb, err = UnmarshalBBHash(fd)
	if err != nil {
		return nil, fmt.Errorf("%s: can't unmarshal hash table: %s", fn, err)
	}

	rd.salt = hdr.salt
	rd.nkeys = hdr.nkeys

	binary.BigEndian.PutUint64(rd.saltkey[:8], rd.salt)
	binary.BigEndian.PutUint64(rd.saltkey[8:], ^rd.salt)

	return rd, nil
}

// TotalKeys returns the total number of distinct keys in the DB
func (rd *DBReader) TotalKeys() int {
	return len(rd.offsets)
}

// Close closes the db
func (rd *DBReader) Close() {
	munmapUint64(int(rd.fd.Fd()), rd.offsets)
	rd.fd.Close()
	rd.cache.Purge()
	rd.bb = nil
	rd.fd = nil
	rd.salt = 0
	rd.saltkey = nil
	rd.fn = ""
}


// Lookup looks up 'key' in the table and returns the corresponding value.
// If the key is not found, value is nil and returns false.
func (rd *DBReader) Lookup(key []byte) ([]byte, bool) {
	v, err := rd.Find(key)
	if err != nil {
		return nil, false
	}

	return v, true
}

// Find looks up 'key' in the table and returns the corresponding value.
// It returns an error if the key is not found or the disk i/o failed or
// the record checksum failed.
func (rd *DBReader) Find(key []byte) ([]byte, error) {
	h := fasthash.Hash64(rd.salt, key)

	if v, ok := rd.cache.Get(h); ok {
		r := v.(*record)
		return r.val, nil
	}

	// Not in cache. So, go to disk and find it.
	i := rd.bb.Find(h)
	if i == 0 {
		return nil, ErrNoKey
	}

	//fmt.Printf("key %s => %#x => %d\n", string(key), h, i)
	off := toLittleEndianUint64(rd.offsets[i-1])
	r, err := rd.decodeRecord(off)
	if err != nil {
		return nil, err
	}

	if r.hash != h {
		return nil, ErrNoKey
	}

	/*
		// XXX Do we need this?
		if subtle.ConstantTimeCompare(key, r.key) != 1 {
			return nil, ErrNoKey
		}
	*/

	rd.cache.Add(h, r)
	return r.val, nil
}

// Verify checksum of all metadata: offset table, bbhash bits and the file header.
func (rd *DBReader) verifyChecksum(hdrb []byte, offtbl uint64, sz int64) error {
	h := sha512.New512_256()
	h.Write(hdrb[:])

	// we now verify the offset table before decoding anything else or allocating
	// any memory.
	expsz := sz - int64(offtbl) - int64(32)

	rd.fd.Seek(int64(offtbl), 0)

	nw, err := io.CopyN(h, rd.fd, expsz)
	if err != nil {
		return fmt.Errorf("%s: i/o error: %s", rd.fn, err)
	}
	if nw != expsz {
		return fmt.Errorf("%s: partial read while verifying checksum, exp %d, saw %d", rd.fn, expsz, nw)
	}

	var expsum [32]byte

	// Read the trailer -- which is the expected checksum
	rd.fd.Seek(sz-32, 0)
	nr, err := rd.fd.Read(expsum[:])
	if err != nil {
		return fmt.Errorf("%s: i/o error: %s", rd.fn, err)
	}
	if nr != 32 {
		return fmt.Errorf("%s: partial read of checksum; exp 32, saw %d", rd.fn, nr)
	}

	csum := h.Sum(nil)
	if subtle.ConstantTimeCompare(csum[:], expsum[:]) != 1 {
		return fmt.Errorf("%s: checksum failure; exp %#x, saw %#x", rd.fn, expsum[:], csum[:])
	}

	rd.fd.Seek(int64(offtbl), 0)
	return nil
}

// entry condition: b is 64 bytes long.
func (rd *DBReader) decodeHeader(b []byte, sz int64) (*header, error) {
	if string(b[:4]) != "BBHH" {
		return nil, fmt.Errorf("%s: bad header", rd.fn)
	}

	be := binary.BigEndian
	h := &header{}
	i := 8

	h.salt = be.Uint64(b[i : i+8])
	i += 8
	h.nkeys = be.Uint64(b[i : i+8])
	i += 8
	h.offtbl = be.Uint64(b[i : i+8])

	if h.offtbl < 64 || h.offtbl >= uint64(sz-32) {
		return nil, fmt.Errorf("%s: corrupt header", rd.fn)
	}

	return h, nil
}

// read the next full record at offset 'off' - by seeking to that offset.
// calculate the record checksum, validate it and so on.
func (rd *DBReader) decodeRecord(off uint64) (*record, error) {
	_, err := rd.fd.Seek(int64(off), 0)
	if err != nil {
		return nil, err
	}

	var hdr [2 + 4 + 8]byte

	n, err := rd.fd.Read(hdr[:])
	if err != nil {
		return nil, err
	}
	if n != (2 + 4 + 8) {
		return nil, fmt.Errorf("%s: short read at off %d (exp 14, saw %d)", rd.fn, off, n)
	}

	be := binary.BigEndian
	klen := int(be.Uint16(hdr[:2]))
	vlen := int(be.Uint32(hdr[2:6]))

	if klen <= 0 || vlen <= 0 || klen > 65535 {
		return nil, fmt.Errorf("%s: key-len %d or value-len %d out of bounds", rd.fn, klen, vlen)
	}

	buf := make([]byte, klen+vlen)
	n, err = rd.fd.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != (klen + vlen) {
		return nil, fmt.Errorf("%s: short read at off %d (exp %d, saw %d)", rd.fn, off, klen+vlen, n)
	}

	x := &record{
		key:  buf[:klen],
		val:  buf[klen:],
		csum: be.Uint64(hdr[6:]),
	}

	csum := x.checksum(rd.saltkey, off)
	if csum != x.csum {
		return nil, fmt.Errorf("%s: corrupted record at off %d (exp %#x, saw %#x)", rd.fn, off, x.csum, csum)
	}

	x.hash = fasthash.Hash64(rd.salt, x.key)
	return x, nil
}

// ErrNoKey is returned when a key cannot be found in the DB
var ErrNoKey = errors.New("No such key")
