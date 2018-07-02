// marshal.go - Marshal/Unmarshal for BBHash datastructure
//
// Implements the BBHash algorithm in: https://arxiv.org/abs/1702.03154
//
// Inspired by D Gryski's implementation of BBHash (https://github.com/dgryski/go-boomphf)
//
// (c) Sudhi Herle 2018
//
// License GPLv2

package bbhash

import (
	"bytes"
	"fmt"
	"io"

	"encoding/binary"
)

// MarshalBinary encodes the hash into a binary form suitable for durable storage.
// A subsequent call to UnmarshalBinary() will reconstruct the BBHash instance.
func (bb *BBHash) MarshalBinary(w io.Writer) error {

	// Header: 4 64-bit words:
	//   o version
	//   o n-bitvectors
	//   o salt
	//   o resv
	//
	// Body:
	//   o <n> bitvectors laid out consecutively

	var b bytes.Buffer
	var x [8]byte

	le := binary.LittleEndian

	le.PutUint64(x[:], 1) // version 1
	b.Write(x[:])

	le.PutUint64(x[:], uint64(len(bb.bits)))
	b.Write(x[:])

	le.PutUint64(x[:], bb.salt)
	b.Write(x[:])

	le.PutUint64(x[:], 0) // reserved byte
	b.Write(x[:])

	n, err := w.Write(b.Bytes())
	if err != nil {
		return err
	}
	if n != b.Len() {
		errShortWrite(n)
	}

	// Now, write the bitvectors themselves
	for _, bv := range bb.bits {
		err = bv.MarshalBinary(w)
		if err != nil {
			return err
		}
	}

	// We don't store the rank vector; we can re-compute it when we unmarshal
	// the bitvectors.

	return nil
}

// UnmarshalBBHash reads a previously marshalled binary stream from 'r' and recreates
// the in-memory instance of BBHash.
func UnmarshalBBHash(r io.Reader) (*BBHash, error) {
	var b [32]byte // 4 x 64-bit words of header

	n, err := r.Read(b[:])
	if err != nil {
		return nil, err
	}
	if n != 32 {
		return nil, errShortRead(n)
	}

	le := binary.LittleEndian

	v := le.Uint64(b[:8])
	if v != 1 {
		return nil, fmt.Errorf("bbhash: no support to un-marshal version %d", v)
	}

	v = le.Uint64(b[8:16])
	if v == 0 || v > uint64(MaxLevel) {
		return nil, fmt.Errorf("bbhash: invalid levels %d (max %d)", v, MaxLevel)
	}

	bb := &BBHash{
		bits: make([]*BitVector, v),
		salt: le.Uint64(b[16:24]),
	}

	for i := uint64(0); i < v; i++ {
		bv, err := UnmarshalBitVector(r)
		if err != nil {
			return nil, err
		}

		bb.bits[i] = bv
	}

	bb.preComputeRank()
	return bb, nil
}

func errShortWrite(n int) error {
	return fmt.Errorf("bbhash: incomplete write; exp 8, saw %d", n)
}

func errShortRead(n int) error {
	return fmt.Errorf("bbhash: incomplete read; exp 8, saw %d", n)
}
