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

func (s *state) concurrent(okeys []uint64) error {

	keys := okeys
	for {
		n := len(keys)
		p := n / 2

		k0 := keys[:p]
		k1 := keys[p:]


		fmt.Printf("lvl %d: %d keys => k0: 0-%d, k1: %d-%d\n", s.lvl, n, p, p, n)
		var wg sync.WaitGroup

		wg.Add(2)
		go func() {
			preprocess(s, k0)
			wg.Done()
		}()
		go func() {
			preprocess(s, k1)
			wg.Done()
		}()

		wg.Wait()

		s.A.Reset()

		var wg2 sync.WaitGroup
		wg2.Add(2)
		go func() {
			assign(s, k0)
			wg2.Done()
		}()
		go func() {
			assign(s, k1)
			wg2.Done()
		}()

		wg2.Wait()
		keys, _ = s.appendA()
		if keys == nil {
			break
		}

		// Now, see if we have enough keys to concurrentize
		if len(keys) < MinParallelKeys {
			return s.singleThread(keys)
		}

		if s.lvl >= MaxLevel {
			return fmt.Errorf("can't find minimal perf hash after %d tries", s.lvl)
		}
	}

	s.bb.preComputeRank()
	return nil
}


// run the BBHash algorithm concurrently on a sharded set of keys.
// entry: len(keys) > MinParallelKeys
func (s *state) concurrentx(keys []uint64) error {

	ncpu := runtime.NumCPU()

	for {
		nkey := uint64(len(keys))
		z := nkey / uint64(ncpu)
		r := nkey % uint64(ncpu)

		// Pre-process keys and detect colliding entries
		s.wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			i := i
			x := z * uint64(i)
			y := x + z
			if i == (ncpu-1) { y += r }
			go func(x, y uint64) {
				fmt.Printf("cpu %d; Pre-Process shard %d:%d\n", i, x, y)
				preprocess(s, keys[x:y])
				s.wg.Done()
			}(x, y)
		}

		// synchronization point
		s.wg.Wait()

		// Assignment step
		s.A.Reset()
		s.wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			i := i
			x := z * uint64(i)
			y := x + z
			if i == (ncpu-1) { y += r }
			go func(x, y uint64) {
				fmt.Printf("cpu %d; Assign shard %d:%d\n", i, x, y)
				assign(s, keys[x:y])
				s.wg.Done()
			}(x, y)
		}

		// synchronization point #2
		s.wg.Wait()
		keys, _ = s.appendA()
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



