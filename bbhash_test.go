// bbhash_test.go -- test suite for bbhash

package bbhash

import (
	"bytes"
	"testing"

	"github.com/opencoff/go-fasthash"
)

var keyw = []string{
	"expectoration",
	"mizzenmastman",
	"stockfather",
	"pictorialness",
	"villainous",
	"unquality",
	"sized",
	"Tarahumari",
	"endocrinotherapy",
	"quicksandy",
}

func TestSimple(t *testing.T) {
	assert := newAsserter(t)

	keys := make([]uint64, len(keyw))

	for i, s := range keyw {
		h := fasthash.Hash64(0xdeadbeefbaadf00d, []byte(s))
		keys[i] = h
	}

	b, err := New(2.0, keys)
	assert(err == nil, "construction failed: %s", err)

	for i, k := range keys {
		j := b.Find(k)
		assert(j > 0, "can't find key %d: %#x", i, k)
		assert(j <= uint64(len(keys)), "key %d <%#x> mapping %d out-of-bounds", i, k, j)
	}
}

func TestBBMarshal(t *testing.T) {
	assert := newAsserter(t)

	keys := make([]uint64, len(keyw))

	for i, s := range keyw {
		keys[i] = fasthash.Hash64(0xdeadbeefbaadf00d, []byte(s))
	}

	b, err := New(2.0, keys)
	assert(err == nil, "construction failed: %s", err)

	var buf bytes.Buffer

	err = b.MarshalBinary(&buf)
	assert(err == nil, "marshal failed: %s", err)

	t.Logf("marshal size: %d bytes\n", b.MarshalBinarySize())

	b2, err := UnmarshalBBHash(&buf)
	assert(err == nil, "unmarshal failed: %s", err)

	assert(len(b.bits) == len(b2.bits), "rank-vector len mismatch (exp %d, saw %d)",
		len(b.bits), len(b2.bits))

	assert(len(b.ranks) == len(b2.ranks), "rank-helper len mismatch (exp %d, saw %d)",
		len(b.ranks), len(b2.ranks))

	assert(b.salt == b2.salt, "salt mismatch (exp %#x, saw %#x)", b.salt, b2.salt)

	for i := range b.bits {
		av := b.bits[i]
		bv := b2.bits[i]

		assert(av.Size() == bv.Size(), "level-%d, bitvector len mismatch (exp %d, saw %d)",
			i, av.Size(), bv.Size())

		var j uint64
		for j = 0; j < av.Words(); j++ {
			assert(av.v[j] == bv.v[j], "level-%d: bitvector content mismatch (exp %#x, saw %#x)",
				i, av.v[j], bv.v[j])
		}
	}

	for i := range b.ranks {
		ar := b.ranks[i]
		br := b2.ranks[i]

		assert(ar == br, "level-%d: rank mismatch (exp %d, saw %d)", i, ar, br)
	}

	for i, k := range keys {
		x := b.Find(k)
		y := b2.Find(k)
		assert(x > 0, "can't find key %d: %#x", i, x)
		assert(x <= uint64(len(keys)), "key %d <%#x> mapping %d out-of-bounds", i, k, x)

		assert(y > 0, "b2: can't find key %d: %#x", i, y)
		assert(y <= uint64(len(keys)), "b2: key %d <%#x> mapping %d out-of-bounds", i, k, y)

		assert(x == y, "b and b2 mapped key %d <%#x>: %d vs. %d", i, k, x, y)
	}

}
