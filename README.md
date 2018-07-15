[![GoDoc](https://godoc.org/github.com/opencoff/go-bbhash?status.svg)](https://godoc.org/github.com/opencoff/go-bbhash)

# go-bbhash - Fast, Scalable Minimal Perfect Hash for Large Sets

## What is it?
A library to create, marshal, unmarshal and query minimal perfect hash functions
over very large key sets.

This is an implementation of [this paper](https://arxiv.org/abs/1702.03154). It is in part
inspired by Damien Gryski's [Boomphf](https://github.com/dgryski/go-boomphf).

The library exposes the following types:

- `BBHash`: Represents an instance of a minimal perfect hash
  function as described in the paper above.
- `DBWriter`: Used to construct a constant database of key-value
  pairs - where the lookup of a given key is doing in constant time
  using `BBHash`. Essentially, this type serializes a collection
  of key-value pairs using `BBHash` as the underlying index.
- `DBReader`: Used for looking up key-values from a previously
  constructed Database.

*NOTE* Minimal Perfect Hash functions take a fixed input and
generate a mapping to lookup the items in constant time. In
particular, they are NOT a replacement for a traditional hash-table;
i.e., it may yield false-positives when queried using keys not
present during construction. In concrete terms:

   Let S = {k0, k1, ... kn}  be your input key set.

   If H: S -> {0, .. n} is a minimal perfect hash function, then
   H(kx) for kx NOT in S may yield an integer result (indicating
   that kx was successfully "looked up").

Thus, if users of `BBHash` are unsure of the input being passed to such a
`Lookup()` function, they should add an additional comparison against
the actual key to verify. Look at `dbreader.go:Find()` for an
example.

## Basic Usage of BBHash
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

## Writing a DB Once, but lookup many times
One can construct an on-disk constant-time lookup using `BBHash` as
the underlying indexing mechanism. Such a DB is useful in situations
where the key/value pairs are NOT changed frequently; i.e.,
read-dominant workloads. The typical pattern in such situations is
to build the constant-DB _once_ for efficient retrieval and do
lookups multiple times.

### Step-1: Construct the DB from multiple sources
For example, let us suppose that file *a.txt* and *b.csv* have lots
of key,value pairs. We will build a constant DB using this.

```go

    wr, err := bbhash.NewDBWriter("file.db")
    if err != nil { panic(err) }

    // add a.txt and a.csv to this db

    // txt file delimited by white space;
    // first token is the key, second token is the value
    n, err := wr.AddTextFile("a.txt", " \t")
    if err != nil { panic(err) }
    fmt.Printf("a.txt: %d records added\n", n)

    // CSV file - comma delimited
    // lines starting with '#' are considered comments
    // field 0 is the key; and field 1 is the value.
    // The first line is assumed to be a header and ignored.
    n, err := wr.AddCSVFile("b.csv", ',', '#', 0, 1)
    if err != nil { panic(err) }
    fmt.Printf("b.csv: %d records added\n", n)

    // Now, freeze the DB and write to disk.
    // We will use a larger "gamma" value to increase chances of
    // finding a minimal perfect hash function.
    err = wr.Freeze(3.0)
    if err != nil { panic(err) }
```

Now, `file.db` has the key/value pairs from the two input files
stored in an efficient format for constant-time retrieval.

### Step-2: Looking up Key in the DB
Continuing the above example, suppose that you want to use the
constructed DB for repeated lookups of various keys and retrieve
their corresponding values:

```go

    // read 'file.db' previously constructed and cache upto 10,000
    // records in memory.
    rd, err := bbhash.NewDBReader("file.db", 10000)
    if err != nil { panic(err) }
```

Now, given a key `k`, we can use `rd` to lookup the corresponding
value:

```go

    val, err := rd.Find(k)

    if err != nil {
        if err == bbhash.ErrNoKey {
            fmt.Printf("Key %x is not in the DB\n", k)
        } else {
            fmt.Printf("Error: %s\n", err)
        }
    }

    fmt.Printf("Key %x => Value %x\n", k, val)
```

## Implementation Notes

* For constructing the BBHash, keys are `uint64`; the DBWriter
  implementation uses Zi Long Tan's superfast hash function to
  transform arbitary bytes to uint64.

* The perfect-hash index for each key is "1" based (i.e., it is in the closed
  interval `[1, len(keys)]`.


## License
GPL v2.0
