#! /usr/bin/env python

# Generate random hostname,IPAddr pairs for one or more subnets
#
# (c) 2018 Sudhi Herle <sw-at-herle.net>
# License GPLv2

import os, sys, os.path
import random, string, itertools
from os.path import basename

Z = basename(sys.argv[0])

def main():
    if len(sys.argv) < 2:
        die("Too few arguments!\nUsage: %s subnet [subnet..]", Z)

    wl = wordlist(3)
    random.shuffle(wl)
    i = 0

    out = []
    for s in sys.argv[1:]:
        try:
            a0 = ipv4(s)
        except Exception as ex:
            warn("can't parse subnet %s: %s", s, str(ex))
            continue

        j = 0
        for a in a0:
            host = "%s-%.4d" % (wl[j], i)
            out.append("%s %s" % (host, a.addrstr()))
            j += 1
            i += 1
            if j >= len(wl): j = 0

    random.shuffle(out)
    print("\n".join(out))
def wordlist(wlen):
    """Generate all words of length at least 'wlen'"""

    initial_consonants = (set(string.ascii_lowercase) - set('aeiou')
                          # remove those easily confused with others
                          - set('qxc')
                          # add some crunchy clusters
                          | set(['bl', 'br', 'cl', 'cr', 'dr', 'fl',
                                 'fr', 'gl', 'gr', 'kr', 'ki', 'ky',
                                 'pl', 'pr', 'sk', 'sr',
                                 'sl', 'sm', 'sn', 'sp', 'st', 'str',
                                 'sw', 'tr'])
                          )

    final_consonants = (set(string.ascii_lowercase) - set('aeiou')
                        # confusable
                        - set('qxcsj')
                        # crunchy clusters
                        | set(['ct', 'ft', 'mp', 'nd', 'ng', 'nk', 'nt',
                               'pt', 'sk', 'sp', 'ss', 'st'])
                        )

    vowels = 'aeiou' # we'll keep this simple

    # each syllable is consonant-vowel-consonant "pronounceable"
    z = map(''.join, itertools.product(initial_consonants, 
                                           vowels, 
                                           final_consonants))

    return [ x for x in z if len(x) >= wlen ]

class ipv4:
    """A Useful IPv4 address class.

    IPv4 Addresses can be constructed like so:

        >>> ipv4("128.99.33.43")
        >>> ipv4("128.99.33.43/18")
        >>> ipv4("128.99.33.43/255.255.255.248")

    Once constructed, the individual methods can be used to do a variety
    of operations:

        - Return CIDR address representation
        - Return range of addresses (an iterator)
        - Return netmask in standard (dotted-quad) or CIDR notation
        - Return first and last addresses of range
        - Return network encapsulating the address
        - Return number of addresses in range
    
    """

    _max = 32

    def _parse(self, st="0.0.0.0"):
        """Parse an ipv4 address string"""

        if type(st) == int:
            if st > 0xffffffff:
                raise ValueError("Malformed IP address '%l'" % st)

            return st & 0xffffffff

        try:
            x = int(st)
            if x > 0xffffffff:
                raise ValueError("Malformed IP address '%s'" % st)
            return x & 0xffffffff
        except:
            pass

        v = st.split('.')
        if len(v) != 4:
            raise ValueError("Malformed IP address '%s'" % st)

        try:
            vi = map(int, v)
        except:
            raise ValueError("Malformed IP address '%s'" % st)

        z = 0
        for x in vi:
            if x > 255 or x < 0:
                raise ValueError("Malformed IP address '%s'" % st)
            z = (z << 8) | x

        return z & 0xffffffff

    def _parse_mask(self, st):
        """Intelligently grok a netmask"""

        if type(st) == int:

            if st > self._max:
                return st
            else:
                return prefix_to_ipv4(st)

        if st.find('.') < 0:
            try:
                v = int(st)
            except:
                raise ValueError("Malformed netmask '%s'" % st)

            if v > self._max:
                raise ValueError("Too many bits in netmask '%s'" %
            st)

            return prefix_to_ipv4(v)
        else:
            return self._parse(st)

    def tostr(cls, v):
        return _tostr(v)


    def _masklen(self, v):
        """Return cidr mask - number of set bits """

        return ipv4_to_prefix(v)


    def __init__(self, addr, mask=None):
        """Construct an IPv4 address"""

        if type(addr) != type(""):
            addr = repr(addr)

        if mask is None:
            str = addr
            i = str.find('/')
            if i < 0:
                self._addr = self._parse(str)
                self._mask = self._parse_mask("32")
            else:
                self._addr = self._parse(str[0:i])
                self._mask = self._parse_mask(str[i+1:])
        else:
            self._addr = self._parse(addr)
            self._mask = self._parse_mask(mask)

        self._cidr = self._masklen(self._mask)
        #print "addr=%lx mask=%lx cidrbits=%d" % (self._addr,
                #self._mask, self._cidr)


    def __repr__(self):
        return "%s/%d" % (_tostr(self._addr), self._cidr)


    def __cmp__(self, other):
        """Return -1 if self < other; 0 if self == other; 1 if self > other"""
        a = self._addr
        b = other._addr
        x = a - b
        if x < 0:
            y = -1
        elif x > 0:
            y = +1
        else:
            y = 0
        return y

    def __iter__(self):
        """Return iterator for range represented by this CIDR"""
        return _ipv4iter(self)

    def __hash__(self):
        """Return int usable as a key to dict"""
        return int(self._addr & 0x7fffffff)

    # Accessor methods
    def cidr(self):
        return self.__repr__()

    def first(self):
        """Return the first IP address of the range"""
        net = 0xffffffff & (self._addr & self._mask)
        return ipv4(net, self._mask)

    def count(self):
        """Count number of addresses in the range"""
        f = self._addr
        l = 0xffffffff & (self._addr | ~self._mask)
        return l - f

    def last(self):
        """Return the last IP address of the range"""
        l = 0xffffffff & (self._addr | ~self._mask)
        return ipv4(l, self._mask)

    def standard(self):
        return "%s/%s" % (_tostr(self._addr), _tostr(self._mask))

    def addr(self):
        return self._addr

    def netmask(self):
        return self._mask

    def netmask_cidr(self):
        return self._masklen(self._mask)


    def addrstr(self):
        return _tostr(self._addr)

    def net(self):
        """Return network number of this address+mask"""
        return 0xffffffff & (self._addr & self._mask)


    def is_member_of(self, net):
        """Return true if IP is member of network 'net'"""
        fp = getattr(net, "netmask_cidr", None)
        if fp is None:
            net = ipv4(net)

        mynet    = self._addr & net._mask;
        theirnet = net._addr  & net._mask;
        return mynet == theirnet

    def network(self):
        net = 0xffffffff & (self._addr & self._mask)
        return "%s/%s" % (_tostr(net), _tostr(self._mask))

    def network_cidr(self):
        net = 0xffffffff & (self._addr & self._mask)
        return "%s/%d" % (_tostr(net), self._cidr)

    tostr = classmethod(tostr)


def ipv4_to_prefix(n):
    """Convert IPv4 address 'a' (in integer representation) into prefix format."""

    if n == 0xffffffff: return 32
    if n == 0:          return 0

    for i in range(32):
        if n & 1: return 32-i
        n >>= 1


def prefix_to_ipv4(n):
    """Convert a 32-bit network prefix length into a IPv4 address"""
    ones = 0xffffffff
    return ones ^ (ones >> n)

def _tostr(v):
    """Convert IPv4 to dotted quad string"""
    v0 =  v & 0xff
    v1 = (v >> 8)  & 0xff
    v2 = (v >> 16) & 0xff
    v3 = (v >> 24) & 0xff
    return "%s.%s.%s.%s" % (v3, v2, v1, v0)

class _ipv4iter(object):
    """An IPv4 address iterator"""
    def __init__(self, addr):
        self.mask = addr.netmask()
        self.last = addr.net()
        self.cur  = addr.addr()

    def __iter__(self):
        return self

    def __next__(self):
        """Return the next address after self.cur"""

        m = 0xffffffff & (self.cur & self.mask)
        if m != self.last:
            raise StopIteration

        n = self.cur
        self.cur +=  1
        return ipv4(n, self.mask)

def warn(fmt, *args):
    """Show error message and die if doex > 0"""
    sfmt = "%s: %s" % (Z, fmt)
    if args:
        sfmt = sfmt % args

    if not sfmt.endswith('\n'):
        sfmt += '\n'

    sys.stdout.flush()
    sys.stderr.write(sfmt)
    sys.stderr.flush()

def die(fmt, *args):
    warn(fmt, *args)
    sys.exit(1)


main()
