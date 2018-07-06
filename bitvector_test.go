// bitvector_test.go -- test suite for bitvector

package bbhash

import (
	"bytes"
	"fmt"
	"runtime"
	"testing"
	"math/rand"
	"sync"
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

	bv := newbitVector(100, 1.0)
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


// Test concurrent bitvector stuff
func TestConcurrentRandom(t *testing.T) {
	assert := newAsserter(t)
	ncpu := runtime.NumCPU() * 2

	br := newbitVector(1000, 1.0)
	bw := newbitVector(1000, 1.0)
	n := br.Size()

	for i := uint64(0); i < n; i++ {
		if 1 == (i & 1) {
			br.Set(i)
		}
	}

	verify := make([][]uint64, ncpu)
	var w sync.WaitGroup
	w.Add(ncpu)
	for i := 0; i < ncpu; i++ {
		go func(i int, a, b *bitVector) {
			defer w.Done()

			n := uint64(a.Size()) * 16
			idx := make([]uint64, 0, n)
			sz := a.Size()

			for j := uint64(0); j < n; j++ {
				r := rand.Uint64() % sz
				if a.IsSet(r) {
					b.Set(r)
					idx = append(idx, r)
				}
			}

			verify[i] = idx
		}(i, br, bw)
	}

	w.Wait()

	// Now every entry in verify is set.
	for _, v := range verify {
		for _, k := range v {
			assert(bw.IsSet(k), "%d is not set", k)
		}
	}
}


func TestConcurrent(t *testing.T) {
	assert := newAsserter(t)
	ncpu := runtime.NumCPU() * 1

	br := newbitVector(1000, 1.0)
	bw := newbitVector(1000, 1.0)
	n := br.Size()

	for i := uint64(0); i < n; i++ {
		if 1 == (i & 1) {
			br.Set(i)
		}
	}

	var w sync.WaitGroup
	w.Add(ncpu)
	for i := 0; i < ncpu; i++ {
		go func(i int, a, b *bitVector) {
			defer w.Done()

			n := uint64(a.Size())
			for j := uint64(0); j < n; j++ {
				if a.IsSet(j) {
					b.Set(j)
				}
			}
		}(i, br, bw)
	}

	w.Wait()

	// Now every entry in verify is set.
	for i := uint64(0); i < n; i++ {
		if br.IsSet(i) {
			assert(bw.IsSet(i), "%d is not set", i)
		}
	}
}

func TestMarshal(t *testing.T) {
	assert := newAsserter(t)

	var b bytes.Buffer

	bv := newbitVector(100, 1.0)
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

	bn, err := unmarshalbitVector(&b)
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

