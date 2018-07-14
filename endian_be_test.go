// endian_be_test.go -- test suite for endian-convertors:
// Run this on Big-endian machines!

// +build ppc64 mips mips64

package bbhash

import (
	"testing"
)

func TestEndianOnBE(t *testing.T) {
	assert := newAsserter(t)    // this is in bitvector_test.go


	a0 := uint32(0xabcd1234)
	b0 := ToBigEndianUint32(a0)
	assert(a0 == b0, "uint32 %d != %d", a0, b0)

	a1 := uint64(0xabcd1234baadf00d)
	b1 := ToBigEndianUint64(a1)
	assert(a1 == b1, "uint64 %d != %d", a1, b1)

	a2 := uint16(0xabcd)
	b2 := ToBigEndianUint16(a2)
	assert(a2 == b2, "uint16 %d != %d", a2, b2)

	b0 = ToLittleEndianUint32(a0)
	assert(b0 == 0x3412cdab, "uint32-be %d != %d", a0, b0)

	b1 = ToLittleEndianUint64(a1)
	assert(b1 == 0x0df0adba3412cdab, "uint64-be %d != %d", a1, b1)

	b2 = ToLittleEndianUint16(a2)
	assert(b2 == 0xcdab, "uint16 %d != %d", a2, b2)
}
