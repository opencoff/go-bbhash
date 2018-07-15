// mmap.go -- mmap a slice of ints/uints from a file
//
// Author: Sudhi Herle <sudhi@herle.net>
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.
package bbhash

import (
	"reflect"
	"syscall"
	"unsafe"
)

// map 'n' uint64s at offset 'off'
func MmapUint64(fd int, off uint64, n int, prot, flags int) ([]uint64, error) {
	sz := n * 8

	// XXX Will this grow the file if needed?
	ba, err := syscall.Mmap(fd, int64(off), sz, prot, flags)
	if err != nil {
		return nil, err
	}

	bh := (*reflect.SliceHeader)(unsafe.Pointer(&ba))
	var v []uint64

	// XXX Will addr get garbage collected? It shouldn't!
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&v))
	sh.Data = bh.Data
	sh.Len = n
	sh.Cap = n

	return v, nil
}
