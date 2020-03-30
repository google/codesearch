// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"encoding/binary"
	"math/bits"
)

type deltaReader struct {
	ix *Index
	d  []byte
	b  uint64
	nb uint
}

func (r *deltaReader) init(ix *Index, data []byte) {
	r.ix = ix
	r.d = data
	r.b = 0
	r.nb = 0
}

func (r *deltaReader) clearBits() {
	r.b = 0
	r.nb = 0
}

func (r *deltaReader) next() int {
	if r.ix.is64 {
		return r.next64() - 1
	}
	delta64, n := binary.Uvarint(r.d)
	r.d = r.d[n:]
	if n <= 0 || uint64(int(delta64)) != delta64 {
		r.ix.corrupt()
	}
	return int(delta64)
}

func (r *deltaReader) next64() int {
	for r.b == 0 {
		if len(r.d) == 0 || r.nb > 64-8 {
			r.ix.corrupt()
		}
		r.b |= uint64(r.d[0]) << r.nb
		r.nb += 8
		r.d = r.d[1:]
	}
	lg := uint(bits.TrailingZeros64(r.b))
	r.b >>= lg + 1
	r.nb -= lg + 1
	for r.nb < lg {
		if len(r.d) == 0 || r.nb > 64-8 {
			r.ix.corrupt()
		}
		r.b |= uint64(r.d[0]) << r.nb
		r.nb += 8
		r.d = r.d[1:]
	}
	v := 1<<lg | r.b&(1<<lg-1)
	r.b >>= lg
	r.nb -= lg
	return int(v)
}

type byteWriter interface {
	writeByte(byte)
	write([]byte)
}

type deltaWriter struct {
	out byteWriter
	buf [10]byte
	b   uint64
	nb  uint
}

func (w *deltaWriter) init(out byteWriter) {
	w.out = out
	w.b = 0
	w.nb = 0
}

func (w *deltaWriter) write(x int) {
	if !writeOldIndex {
		w.writeBits(x + 1)
		return
	}

	n := binary.PutUvarint(w.buf[:], uint64(x))
	w.out.write(w.buf[:n])
}

func (w *deltaWriter) writeBits(x int) {
	if x <= 0 {
		panic("bad gamma write")
	}
	lg := uint(bits.Len(uint(x))) - 1
	w.b |= (1 << lg) << w.nb
	w.nb += lg + 1
	if w.nb >= 8 {
		w.flushBits()
	}
	w.b |= uint64(x&(1<<lg-1)) << w.nb
	w.nb += lg
	if w.nb >= 8 {
		w.flushBits()
	}
}

func (w *deltaWriter) flushBits() {
	for w.nb >= 8 {
		w.out.writeByte(byte(w.b))
		w.b >>= 8
		w.nb -= 8
	}
}

func (w *deltaWriter) flush() {
	w.flushBits()
	if w.nb > 0 {
		w.out.writeByte(byte(w.b))
	}
	w.b = 0
	w.nb = 0
}
