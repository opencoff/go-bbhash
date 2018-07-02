// bitvector.go -- simple bitvector implementation
//
// (c) Sudhi Herle 2018
//
// License GPLv2

package bbhash

import (
	"encoding/binary"
	"fmt"
	"io"
)

// bitVector represents a bit vector in an efficient manner
type bitVector struct {
	v []uint64

	// XXX Other fields to pre-compute rank
}

// NewbitVector creates a bitvector to hold atleast 'size * g' bits.
// The value 'g' is an expansion factor (typically > 1.0). The resulting size
// is rounded-up to the next multiple of 64.
func NewbitVector(size uint, g float64) *bitVector {
	sz := uint(float64(size) * g)
	sz += 63
	sz /= 64
	bv := &bitVector{
		v: make([]uint64, sz),
	}

	return bv
}

// Size returns the number of bits in this bitvector
func (b *bitVector) Size() uint64 {
	return uint64(len(b.v) * 64)
}

// Words returns the number of words in the array
func (b *bitVector) Words() uint64 {
	return uint64(len(b.v))
}

// Set sets the bit 'i' in the bitvector
func (b *bitVector) Set(i uint64) {
	b.v[i/64] |= (1 << (i % 64))
}

// IsSet() returns true if the bit 'i' is set, false otherwise
func (b *bitVector) IsSet(i uint64) bool {
	w := b.v[i/64]
	w >>= (i % 64)
	return 1 == (uint(w) & 1)
}

// Reset() clears all the bits in the bitvector
func (b *bitVector) Reset() {
	for i := range b.v {
		b.v[i] = 0
	}
}

// ComputeRanks memoizes rank calculation for future rank queries
// One must not modify the bitvector after calling this function.
// Returns the population count of the bitvector.
func (b *bitVector) ComputeRank() uint64 {
	var p uint64

	for _, v := range b.v {
		p += popcount(v)
	}
	return p
}

// Rank calculates the rank on bit 'i'
// (Rank is the number of bits set before it).
// We actually return 1 less than the actual rank.
func (b *bitVector) Rank(i uint64) uint64 {
	x := i / 64
	y := i % 64

	var r uint64
	var k uint64

	for k = 0; k < x; k++ {
		r += popcount(b.v[k])
	}

	r += popcount(b.v[x] << (64 - y))
	return r
}

// Marshal writes the bitvector in a portable format to writer 'w'.
func (b *bitVector) MarshalBinary(w io.Writer) error {
	var x [8]byte

	le := binary.LittleEndian

	le.PutUint64(x[:], b.Words())

	n, err := w.Write(x[:])
	if err != nil {
		return err
	}
	if n != 8 {
		return errShortWrite(n)
	}

	for _, v := range b.v {
		le.PutUint64(x[:], v)
		n, err := w.Write(x[:])
		if err != nil {
			return err
		}
		if n != 8 {
			return errShortWrite(n)
		}
	}
	return nil
}


// MarshalBinarySize returns the size in bytes when this bitvector is marshaled.
func (b *bitVector) MarshalBinarySize() uint64 {
	return 8 * (1 + b.Words())
}


// UnmarshalbitVector reads a previously encoded bitvector and reconstructs
// the in-memory version.
func UnmarshalbitVector(r io.Reader) (*bitVector, error) {
	var x [8]byte
	le := binary.LittleEndian

	n, err := r.Read(x[:])
	if err != nil {
		return nil, err
	}
	if n != 8 {
		return nil, errShortRead(n)
	}

	bvlen := le.Uint64(x[:])
	if bvlen == 0 || bvlen > (1<<32) {
		return nil, fmt.Errorf("bitvect length %d is invalid", bvlen)
	}

	b := &bitVector{
		v: make([]uint64, bvlen),
	}

	for i := uint64(0); i < bvlen; i++ {
		n, err := r.Read(x[:])
		if err != nil {
			return nil, err
		}
		if n != 8 {
			return nil, errShortRead(n)
		}

		b.v[i] = le.Uint64(x[:])
	}

	return b, nil
}

// population count - from Hacker's Delight
func popcount(x uint64) uint64 {
	x -= (x >> 1) & 0x5555555555555555
	x = (x>>2)&0x3333333333333333 + x&0x3333333333333333
	x += x >> 4
	x &= 0x0f0f0f0f0f0f0f0f
	x *= 0x0101010101010101
	return x >> 56
}
