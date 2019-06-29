
srcs = $(wildcard *.go)
mphdb_srcs = $(wildcard example/mphdb.go)

all: mphdb

mphdb: $(srcs) $(mphdb_srcs)
	go build -o $@ ./example/mphdb.go


test: $(srcs)
	go test

.PHONY: clean realclean

clean realclean:
	-rm -f mphdb
