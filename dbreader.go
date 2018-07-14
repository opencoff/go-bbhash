// dbreader.go -- Constant DB built on top of the BBHash MPH
//
// Author: Sudhi Herle <sudhi@herle.net>
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.
package bbhash

import (
	"os"
	"fmt"
	"encoding/binary"

	//"github.com/dchest/siphash"
	"github.com/opencoff/go-fasthash"
)

type DBReader struct {
	bb *BBHash

	fd      *os.File
	salt    uint64
	saltkey []byte
}



// read the next full record at offset 'off' - by seeking to that offset.
// calculate the record checksum, validate it and so on.
func (r *DBReader) decodeRecord(off uint64) (*record, error) {
	_, err := r.fd.Seek(int64(off), 0)
	if err != nil {
		return nil, err
	}

	var hdr [2 + 4 + 8]byte

	n, err := r.fd.Read(hdr[:])
	if err != nil {
		return nil, err
	}
	if n != (2 + 4 + 8) {
		return nil, fmt.Errorf("short read at off %d (exp 14, saw %d)", off, n)
	}

	be := binary.BigEndian
	klen := int(be.Uint16(hdr[:2]))
	vlen := int(be.Uint32(hdr[2:6]))

	if klen <= 0 || vlen <= 0 || klen > 65535 {
		return nil, fmt.Errorf("key-len %d or value-len %d out of bounds", klen, vlen)
	}

	buf := make([]byte, klen+vlen)
	n, err = r.fd.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != (klen + vlen) {
		return nil, fmt.Errorf("short read at off %d (exp %d, saw %d)", off, klen+vlen, n)
	}

	x := &record{
		key:  buf[:klen],
		val:  buf[klen:],
		csum: be.Uint64(hdr[6:]),
	}

	csum := x.checksum(r.saltkey, off)
	if csum != x.csum {
		return nil, fmt.Errorf("corrupted record at off %d (exp %#x, saw %#x)", off, x.checksum, csum)
	}

	x.hash = fasthash.Hash64(r.salt, x.key)
	return x, nil
}
