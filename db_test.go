// db_test.go -- test suite for dbreader/dbwriter

package bbhash

import (
	"fmt"
	"os"
	"testing"
	"flag"

	"github.com/opencoff/go-fasthash"
)

var keep bool

func init() {
	flag.BoolVar(&keep, "keep", false, "Keep test DB")
}

func TestDB(t *testing.T) {
	assert := newAsserter(t)

	vals := make([][]byte, len(keyw))
	keys := make([][]byte, len(keyw))

	for i, s := range keyw {
		h := fasthash.Hash64(0xdeadbeefbaadf00d, []byte(s))
		vals[i] = []byte(fmt.Sprintf("%#x", h))
		keys[i] = []byte(s)
	}

	fn := fmt.Sprintf("%s/mph%d.db", os.TempDir(), rand64())

	wr, err := NewDBWriter(fn)
	assert(err == nil, "can't create db: %s", err)

	defer func() {
		if keep {
			t.Logf("DB in %s retained after test\n", fn)
		} else {
			os.Remove(fn)
		}
	}()

	n, err := wr.AddKeyVals(keys, vals)
	assert(err == nil, "can't add key-val: %s", err)

	assert(int(n) == len(keys), "fewer keys added; exp %d, saw %d", len(keys), n)

	err = wr.Freeze(2.0)
	assert(err == nil, "freeze failed: %s", err)

	rd, err := NewDBReader(fn, 10)
	assert(err == nil, "read failed: %s", err)

	for i, k := range keys {
		v := vals[i]

		s, err := rd.Find(k)
		assert(err == nil, "can't find key %s: %s", k, err)

		assert(string(s) == string(v), "key %s: value mismatch; exp %s, saw %s", k, v, string(s))
	}
}
