// concurrent.go - fast minimal perfect hashing for massive key sets: concurrent building of table
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
	"fmt"

	"runtime"
	"sync"
)

// run the BBHash algorithm concurrently on a sharded set of keys.
// entry: len(keys) > MinParallelKeys
func (s *state) concurrent(keys []uint64) error {

	ncpu := runtime.NumCPU()
	A := s.A

	for {
		nkey := uint64(len(keys))
		z := nkey / uint64(ncpu)
		r := nkey % uint64(ncpu)

		var wg sync.WaitGroup

		// Pre-process keys and detect colliding entries
		wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			i := i
			x := z * uint64(i)
			y := x + z
			if i == (ncpu - 1) {
				y += r
			}
			go func(x, y uint64) {
				//printf("lvl %d: cpu %d; Pre-process shard %d:%d", s.lvl, i, x, y)
				preprocess(s, keys[x:y])
				wg.Done()
			}(x, y)
		}

		// synchronization point
		wg.Wait()

		// Assignment step
		A.Reset()
		wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			i := i
			x := z * uint64(i)
			y := x + z
			if i == (ncpu - 1) {
				y += r
			}
			go func(x, y uint64) {
				//printf("lvl %d: cpu %d; Assign shard %d:%d", s.lvl, i, x, y)
				assign(s, keys[x:y])
				wg.Done()
			}(x, y)
		}

		// synchronization point #2
		wg.Wait()
		keys, A = s.nextLevel()
		if keys == nil {
			break
		}

		// Now, see if we have enough keys to concurrentize
		if len(keys) < MinParallelKeys {
			return s.singleThread(keys)
		}

		if s.lvl > MaxLevel {
			return fmt.Errorf("can't find minimal perf hash after %d tries", s.lvl)
		}

	}

	s.bb.preComputeRank()

	return nil
}
