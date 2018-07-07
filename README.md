[![GoDoc](https://godoc.org/github.com/opencoff/go-bbhash?status.svg)](https://godoc.org/github.com/opencoff/go-bbhash)

# go-bbhash - Fast, Scalable Minimal Perfect Hash for Large Sets

## What is it?
A library to create, marshal, unmarshal and query minimal perfect hash functions
over very large key sets.

This is an implementation of [this paper](https://arxiv.org/abs/1702.03154). It is in part
inspired by Damien Gryski's [Boomphf](https://github.com/dgryski/go-boomphf).

*NOTE* Minimal Perfect Hash functions take a fixed input and
generate a mapping to lookup the items in constant time. In
particular, they are NOT a replacement for a traditional hash-table;
i.e., it may yield false-positives when queried using keys not
present during construction. In concrete terms:

   Let S = {k0, k1, ... kn}  be your input key set.

   If H: S -> {0, .. n} is a minimal perfect hash function, then
   H(kx) for kx NOT in S may yield an integer result (indicating
   that kx was successfully "looked up").

Thus, if users of a minimal-perfect-hash library are unsure of the
input being passed to such a `Lookup()` function, they should add an
additional comparison against the actual key to verify.

## Usage
Assuming you have read your keys, hashed them into `uint64`, this is how you can use the library:

```go

	bb, err := bbhash.New(2.0, keys)
	if err != nil { panic(err) }

	// Now, call Find() with each key to gets its unique mapping.
    // Note: Find() returns values in the range closed-interval [1, len(keys)]
	for i, k := range keys {
        j := bb.Find(k)
		fmt.Printf("%d: %#x maps to %d\n", i, k, j)
	}

```

### Storing the generated minimal hash to disk
This implementation has the ability to marshal/unmarshal the
generated Minimal Perfect Hash to disk via two functions:

* `Marshal()` -- marshal an instance of generated bbhash into a
  supplied `io.Writer`.

  ```go

        bb, err := bbhash.New(2.0, keys)
        if err != nil { panic(err) }

        var buf bytes.Buffer

        err = bb.Marshal(&buf)
        if err != nil { panic(err) }

        // Now, write buf.Bytes() to persistent storage.
  ```

* `UnmarshalBBHash()` -- unmarshal data from an `io.Reader` into an instance
  of `bbhash`. This function is a package level function.

  ```go

    bb, err := bbhash.UnmarshalBBHash(ioreader)
    if err != nil { panic(err) }

    // Now use 'bb' as an initialized instance of BBHash.
  ```


Note that the marshaling routine does NOT add any sort of checksum
to protect/verify the integrity of the marshaled data. It is upto
the caller to add such checks.

## Implementation Notes

* Keys are `uint64`, for all other types, use a good hash function such as Metrohash or
  similar to turn your keys into `uint64`.

* When first constructed, it returns a mapping of the old keys to its perfect-hash analog.

* The perfect-hash index for each key is "1" based (i.e., it is in the closed interval
  `[1, len(keys)]`.


## License
GPL v2.0
