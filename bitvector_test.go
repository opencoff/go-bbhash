// bitvector_test.go -- test suite for bitvector
//

package bbhash

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
)

func newAsserter(t *testing.T) func(cond bool, msg string, args ...interface{}) {
	return func(cond bool, msg string, args ...interface{}) {
		if cond {
			return
		}

		_, file, line, ok := runtime.Caller(1)
		if !ok {
			file = "???"
			line = 0
		}

		s := fmt.Sprintf(msg, args...)
		t.Fatalf("%s: %d: Assertion failed: %s\n", file, line, s)
	}
}

func Test0(t *testing.T) {
	assert := newAsserter(t)

	bv := NewbitVector(100, 1.0)
	assert(bv.Size() == 128, "size mismatch; exp 128, saw %d", bv.Size())

	var i uint64
	for i = 0; i < bv.Size(); i++ {
		if 1 == (i & 1) {
			bv.Set(i)
		}
	}

	for i = 0; i < bv.Size(); i++ {
		if 1 == (i & 1) {
			assert(bv.IsSet(i), "%d not set", i)
		} else {
			assert(!bv.IsSet(i), "%d is set", i)
		}
	}
}

func TestMarshal(t *testing.T) {
	assert := newAsserter(t)

	var b bytes.Buffer

	bv := NewbitVector(100, 1.0)
	assert(bv.Size() == 128, "size mismatch; exp 128, saw %d", bv.Size())

	var i uint64
	for i = 0; i < bv.Size(); i++ {
		if 1 == (i & 1) {
			bv.Set(i)
		}
	}

	bv.MarshalBinary(&b)
	expsz := 8 * (1 + bv.Words())
	assert(uint64(b.Len()) == expsz, "marshal size incorrect; exp %d, saw %d", expsz, b.Len())

	bn, err := UnmarshalbitVector(&b)
	assert(err == nil, "unmarshal failed: %s", err)
	assert(bn.Size() == bv.Size(), "unmarshal size error; exp %d, saw %d", bv.Size(), bn.Size())

	for i = 0; i < bv.Size(); i++ {
		if bv.IsSet(i) {
			assert(bn.IsSet(i), "unmarshal %d is unset", i)
		} else {
			assert(!bn.IsSet(i), "unmarshal %d is set", i)
		}
	}

}
