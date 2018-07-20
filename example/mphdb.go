// mphdb.go -- Build a Constant DB based on BBHash MPH
//
// Author: Sudhi Herle <sudhi@herle.net>
//
// This software does not come with any express or implied
// warranty; it is provided "as is". No claim  is made to its
// suitability for any purpose.

// mphdb.go is an example of using bbhash:DBWriter() and DBReader.
// One can construct the on-disk MPH DB using a variety of input:
//   - white space delimited text file: first field is key, second field is value
//   - Comma Separated text file (CSV): first field is key, second field is value
//
// Sometimes, bbhash gets into a pathological state while constructing MPH out of very
// large data sets. This can be alleviated by using a larger "gamma". mphdb tries to
// bump the gamma to "4.0" whenever we have more than 1M keys.

package main

import (
	"fmt"
	"os"
	"strings"

	B "github.com/opencoff/go-bbhash"

	flag "github.com/ogier/pflag"
)

type value struct {
	hash uint64
	key  string
	val  string
}

var Gamma float64	// bbhash 'gamma' factor
var Verify bool		// if set, verify a previously constructed DB

func main() {
	usage := fmt.Sprintf("%s [options] OUTPUT [INPUT ...]", os.Args[0])

	flag.Float64VarP(&Gamma, "gamma", "g", 2.0, "Bitfield expansion factor")
	flag.BoolVarP(&Verify, "verify", "V", false, "Verify a constant DB")
	flag.Usage = func() {
		fmt.Printf("mphdb - create constant DB from txt or CSV files using MPH\nUsage: %s\n", usage)
		flag.PrintDefaults()
	}

	flag.Parse()
	args := flag.Args()

	if len(args) < 1 {
		die("No output file name!\nUsage: %s\n", usage)
	}

	fn := args[0]
	args = args[1:]

	if Verify {
		db, err := B.NewDBReader(fn, 1000)
		if err != nil {
			die("Can't read %s: %s", fn, err)
		}

		fmt.Printf("%s: %d records\n", fn, db.TotalKeys())
		db.Close()
		return
	}

	db, err := B.NewDBWriter(fn)
	if err != nil {
		die("can't create MPH DB: %s", err)
	}

	var n uint64
	if len(args) > 0 {
		for _, f := range args {
			switch {
			case strings.HasSuffix(f, ".txt"):
				n, err = db.AddTextFile(f, " \t")

			case strings.HasSuffix(f, ".csv"):
				n, err = db.AddCSVFile(f, ',', '#', 0, 1)

			default:
				warn("Don't know how to add %s", f)
				continue
			}

			if err != nil {
				warn("can't add %s: %s", f, err)
				continue
			}

			fmt.Printf("+ %s: %d records\n", f, n)
		}
	} else {
		n, err = db.AddTextStream(os.Stdin, " \t")
		if err != nil {
			db.Abort()
			die("can't add STDIN: %s", err)
		}

		fmt.Printf("+ <STDIN>: %d records\n", n)
	}

	g := Gamma
	if db.TotalKeys() >= 1000000 {
		if g < 3.5 {
			warn("Bumping Gamma to 4.0 to guarantee creation of MPH ..\n")
			g = 4.0
		}
	}

	err = db.Freeze(g)
	if err != nil {
		die("can't write db %s: %s", fn, err)
	}
}

// die with error
func die(f string, v ...interface{}) {
	warn(f, v...)
	os.Exit(1)
}

func warn(f string, v ...interface{}) {
	z := fmt.Sprintf("%s: %s", os.Args[0], f)
	s := fmt.Sprintf(z, v...)
	if n := len(s); s[n-1] != '\n' {
		s += "\n"
	}

	os.Stderr.WriteString(s)
	os.Stderr.Sync()
}

// vim: ft=go:sw=4:ts=4:noexpandtab:tw=78:
