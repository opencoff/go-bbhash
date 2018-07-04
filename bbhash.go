// bbhash.go - fast minimal perfect hashing for massive key sets
//
// Implements the BBHash algorithm in: https://arxiv.org/abs/1702.03154
//
// Inspired by D Gryski's implementation of BBHash (https://github.com/dgryski/go-boomphf)
//
// (c) Sudhi Herle 2018
//
// License GPLv2

// Package bbhash implements BBHash - a new algorithm for creating fast, minimal perfect hash
// functions as described in: https://arxiv.org/abs/1702.03154
package bbhash

import (
	"bytes"
	"fmt"

	"crypto/rand"
	"encoding/binary"
	"runtime"
	"sync"
)

// BBHash represents a computed minimal perfect hash for a given set of keys.
type BBHash struct {
	sync.Mutex

	bits  []*bitVector
	ranks []uint64
	salt  uint64
}


// state used by go-routines when we concurrentize the algorithm
type state struct {
	sync.Mutex

	A *bitVector
	coll *bitVector
	redo  []uint64

	lvl  uint

	bb *BBHash

	wg sync.WaitGroup
	g  float64  // gamma
}

// Gamma is an expansion factor for each of the bitvectors we build.
// Empirically, 2.0 is found to be a good balance between speed and
// space usage. See paper for more details.
const Gamma float64 = 2.0

// Maximum number of attempts (level) at making a perfect hash function.
// Per the paper, each successive level exponentially reduces the
// probability of collision.
const MaxLevel uint = 200

// Minimum number of keys before we use a concurrent algorithm
const MinParallelKeys int = 20000

// New creates a new minimal hash function to represent the keys in 'keys'.
// Once the construction is complete, callers can use "Find()" to find the
// unique mapping for each key in 'keys'.
func New(g float64, keys []uint64) (*BBHash, error) {
	s := newState(len(keys), g)
	err := s.singleThread(keys)
	if err != nil {
		return nil, err
	}
	return s.bb, nil
}


// NewConcurrent creates a new minimal hash function to represent the ekeys in 'keys'.
// This gives callers explicit control over when to use a concurrent algorithm vs. serial.
func NewConcurrent(g float64, keys []uint64) (*BBHash, error) {
	s := newState(len(keys), g)
	err := s.concurrent(keys)
	if err != nil {
		return nil, err
	}
	return s.bb, nil
}


// Find returns a unique integer representing the minimal hash for key 'k'.
// The return value is meaningful ONLY for keys in the original key set (provided
// at the time of construction of the minimal-hash).
// If the key is in the original key-set
func (bb *BBHash) Find(k uint64) uint64 {
	for lvl, bv := range bb.bits {
		i := hash(k, bb.salt, uint(lvl)) % bv.Size()

		if !bv.IsSet(i) {
			continue
		}

		rank := 1 + bb.ranks[lvl] + bv.Rank(i)
		return rank
	}

	return 0
}

// setup state for serial or concurrent execution
func newState(nkeys int, g float64) *state {
	sz := uint(nkeys)

	bb := &BBHash{
		salt: rand64(),
	}

	s := &state{
		A:    newbitVector(sz, g),
		coll: newbitVector(sz, g),
		redo: make([]uint64, 0, sz),
		bb:   bb,
		g:    g,
	}
	return s
}


// single-threaded serial invocation of the BBHash algorithm
func (s *state) singleThread(keys []uint64) error {
	A := s.A

	for len(keys) > 0 {
		preprocess(s, keys)
		A.Reset()
		assign(s, keys)

		s.bb.append(A)

		if s.redoLen() == 0 {
			break
		}

		keys, A = s.resetRedo()

		if s.lvl > MaxLevel {
			return fmt.Errorf("can't find minimal perf hash after %d tries", s.lvl)
		}
	}
	s.bb.preComputeRank()
	return nil
}


// run the BBHash algorithm concurrently on a sharded set of keys.
// entry: len(keys) > MinParallelKeys
func (s *state) concurrent(keys []uint64) error {

	ncpu := runtime.NumCPU()

	for len(keys) > 0 {
		nkey := uint64(len(keys))
		z := nkey / uint64(ncpu)
		r := nkey % uint64(ncpu)

		// Pre-process keys and detect colliding entries
		s.wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			x := z * uint64(i)
			y := x + z
			if i == (ncpu-1) { y += r }
			go func() {
				preprocess(s, keys[x:y])
				s.wg.Done()
			}()
		}

		// synchronization point
		s.wg.Wait()

		// Assignment step
		s.A.Reset()
		s.wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			x := z * uint64(i)
			y := x + z
			if i == (ncpu-1) { y += r }
			go func() {
				assign(s, keys[x:y])
				s.wg.Done()
			}()
		}

		// synchronization point #2
		s.wg.Wait()
		s.bb.append(s.A)
		if s.redoLen() == 0 {
			break
		}

		keys, _ = s.resetRedo()
		if s.lvl > MaxLevel {
			return fmt.Errorf("can't find minimal perf hash after %d tries", s.lvl)
		}

		// Now, see if we have enough keys to concurrentize
		if len(keys) < MinParallelKeys {
			return s.singleThread(keys)
		}
	}

	s.bb.preComputeRank()

	return nil
}


// pre-process to detect colliding bits; concurrentificated
// We have a synchronization point at the end of this loop
func preprocess(s *state, keys []uint64) {
	A := s.A
	coll := s.coll
	bb := s.bb
	salt := bb.salt
	sz := A.Size()
	for _, k := range keys {
		i := hash(k, salt, s.lvl) % sz

		if coll.IsSet(i) {
			continue
		}
		if A.IsSet(i) {
			coll.Set(i)
			continue
		}
		A.Set(i)
	}
}

// phase-2 -- assign non-colliding bits; this too can be concurrentized
func assign(s *state, keys []uint64) {
	A := s.A
	coll := s.coll
	bb := s.bb
	salt := bb.salt
	sz := A.Size()
	for _, k := range keys {
		i := hash(k, salt, s.lvl) % sz

		if coll.IsSet(i) {
			s.addRedo(k)
			continue
		}
		A.Set(i)
	}

}

// Stringer interface for BBHash
func (bb BBHash) String() string {
	var b bytes.Buffer

	b.WriteString(fmt.Sprintf("BBHash: salt %#x; %d levels\n", bb.salt, len(bb.bits)))

	for i, bv := range bb.bits {
		b.WriteString(fmt.Sprintf("  %d: %d bits\n", i, bv.Size()))
	}

	return b.String()
}


// Precompute ranks for each level so we can answer queries quickly.
func (bb *BBHash) preComputeRank() {
	var pop uint64
	bb.ranks = make([]uint64, len(bb.bits))

	// We omit the first level in rank calculation; this avoids a special
	// case in Find() when we are looking at elements in level-0.
	for l, bv := range bb.bits {
		bb.ranks[l] = pop
		pop += bv.ComputeRank()
	}
}


// append to bits
func (bb *BBHash) append(a *bitVector) {
	bb.Lock()
	bb.bits = append(bb.bits, a)
	bb.Unlock()
}


// add k to redo list
func (s *state) addRedo(k uint64) {
	s.Lock()
	s.redo = append(s.redo, k)
	s.Unlock()
}

func (s *state) redoLen() int {
	s.Lock()
	n := len(s.redo)
	s.Unlock()

	return n
}

// Reset the redo list and go to the next level of bitmap.
// returns the list of keys we need to redo.
func (s *state) resetRedo() ([]uint64, *bitVector) {
	s.Lock()

	keys := s.redo
	s.redo = s.redo[:0]
	s.A = newbitVector(uint(len(keys)), s.g)
	s.coll.Reset()
	s.lvl++

	s.Unlock()

	return keys, s.A
}


// One round of Zi Long Tan's superfast hash
func hash(key, salt uint64, lvl uint) uint64 {
	const m uint64 = 0x880355f21e6d1965
	var h uint64 = m

	h ^= mix(key)
	h = (h << lvl) | (h >> (64 - lvl))
	h *= m
	return mix(h) ^ salt
}

// compression function for fasthash
func mix(h uint64) uint64 {
	h ^= h >> 23
	h *= 0x2127599bf4325c37
	h ^= h >> 47
	return h
}

func rand64() uint64 {
	var b [8]byte

	n, err := rand.Read(b[:])
	if err != nil || n != 8 {
		panic("rand read failure")
	}
	return binary.BigEndian.Uint64(b[:])
}
