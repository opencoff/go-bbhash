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

	// Now, bb.Map[] is setup with the right perfect-hash mapping for each key.
	for i, k := range keys {
		fmt.Printf("%d: %#x maps to %d\n", i, k, bb.Map[i])
	}

```

## Implementation Notes

* Keys are `uint64`, for all other types, use a good hash function such as Metrohash or
  similar to turn your keys into `uint64`.

* When first constructed, it returns a mapping of the old keys to its perfect-hash analog.

* The perfect-hash index for each key is "1" based (i.e., it is in the closed interval
  `[1, len(keys)]`.

* The generated perfect hash db can be marshaled and stored in durable storage. The callers
  are responsible for using a checksum or other means to guarantee the integrity of the 
  marshaled data.


## License
GPL v2.0
