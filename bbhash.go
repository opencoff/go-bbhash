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
	"sync"
)

// BBHash represents a computed minimal perfect hash for a given set of keys.
type BBHash struct {
	bits  []*bitVector
	ranks []uint64
	salt  uint64
}

// state used by go-routines when we concurrentize the algorithm
type state struct {
	sync.Mutex

	A    *bitVector
	coll *bitVector
	redo []uint64

	lvl uint

	bb *BBHash

	g float64 // gamma
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

	for {
		//fmt.Printf("lvl %d: %d keys\n", s.lvl, len(keys))
		preprocess(s, keys)
		A.Reset()
		assign(s, keys)

		keys, A = s.nextLevel()
		if keys == nil {
			break
		}

		if s.lvl > MaxLevel {
			return fmt.Errorf("can't find minimal perf hash after %d tries", s.lvl)
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
	salt := s.bb.salt
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
// the redo-list can be local until we finish scanning all the keys.
// XXX "A" could also be kept local and finally merged via bitwise-union.
func assign(s *state, keys []uint64) {
	A := s.A
	coll := s.coll
	salt := s.bb.salt
	sz := A.Size()
	redo := make([]uint64, 0, len(keys)/4)
	for _, k := range keys {
		i := hash(k, salt, s.lvl) % sz

		if coll.IsSet(i) {
			redo = append(redo, k)
			continue
		}
		A.Set(i)
	}

	if len(redo) > 0 {
		s.appendRedo(redo)
	}
}

// add the local copy of 'redo' list to the central list.
func (s *state) appendRedo(k []uint64) {
	k = shuffle(k)

	s.Lock()
	s.redo = append(s.redo, k...)
	s.Unlock()
}

// append the current A to the bits vector and begin new iteration
// return new keys and a new A
func (s *state) nextLevel() ([]uint64, *bitVector) {
	s.Lock()
	defer s.Unlock()

	s.bb.bits = append(s.bb.bits, s.A)
	s.A = nil

	keys := s.redo
	if len(keys) == 0 {
		return nil, nil
	}

	s.redo = s.redo[:0]
	s.A = newbitVector(uint(len(keys)), s.g)
	s.coll.Reset()
	s.lvl++
	return keys, s.A
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

// One round of Zi Long Tan's superfast hash
func hash(key, salt uint64, lvl uint) uint64 {
	const m uint64 = 0x880355f21e6d1965
	var h uint64 = m

	h ^= mix(key)
	h *= m
	h ^= mix(salt)
	h *= m
	h = (h << lvl) | (h >> (64 - lvl))
	h = mix(h)
	return h
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

// Fisher-yates shuffle
func shuffle(k []uint64) []uint64 {
	var i, n uint64
	n = uint64(len(k))
	for i = n - 1; i > 0; i-- {
		j := rand64() % (i + 1)
		u := k[i]
		k[i] = k[j]
		k[j] = u
	}
	return k
}
