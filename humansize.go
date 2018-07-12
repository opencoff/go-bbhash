// humansize.go - Print sizes in human readable form
//
// (c) Sudhi Herle 2018
//
// License GPLv2

package bbhash

import (
	"fmt"
)

const (
	_byte = 1 << (iota * 10)
	_kB
	_MB
	_GB
	_TB
	_PB
	_EB
)

func humansize(sz uint64) string {

	var a, b uint64
	var s string

	switch {
	case sz >= _EB:
		a = sz / _EB
		b = sz % _EB
		s = "EB"
	case sz >= _PB:
		a = sz / _PB
		b = sz % _PB
		s = "PB"
	case sz >= _TB:
		a = sz / _TB
		b = sz % _TB
		s = "TB"
	case sz >= _GB:
		a = sz / _GB
		b = sz % _GB
		s = "GB"
	case sz >= _MB:
		a = sz / _MB
		b = sz % _MB
		s = "MB"
	case sz >= _kB:
		a = sz / _kB
		b = sz % _kB
		s = "kB"

	default:
		return fmt.Sprintf("%d B", sz)
	}

	if b > 0 {
		z := fmt.Sprintf("%d", b)
		return fmt.Sprintf("%d.%2.2s %s", a, z, s)
	}

	return fmt.Sprintf("%d %s", a, s)
}
