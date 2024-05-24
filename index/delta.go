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

const deltaZeroEnc = 16

func (r *deltaReader) next() int {
	if r.ix.version == 2 {
		i := r.next64()
		if i == deltaZeroEnc {
			i = 0
		} else if i > deltaZeroEnc {
			i--
		}
		return i
	}
	delta64, n := binary.Uvarint(r.d)
	r.d = r.d[n:]
	if n <= 0 || uint64(int(delta64)) != delta64 {
		r.ix.corrupt()
	}
	return int(delta64)
}

func (r *deltaReader) next64() int {
	lg := uint(0)
	for r.b == 0 {
		if len(r.d) == 0 || lg+r.nb > 65 {
			r.ix.corrupt()
		}
		lg += r.nb
		r.b = uint64(r.d[0])
		r.nb = 8
		r.d = r.d[1:]
	}
	nb := uint(bits.TrailingZeros64(r.b))
	lg += nb
	r.b >>= nb + 1
	r.nb -= nb + 1
	x := uint64(1 << lg)
	nb = 0
	for r.nb < lg {
		x |= r.b << nb
		nb += r.nb
		lg -= r.nb
		if len(r.d) == 0 || nb > 64 {
			r.ix.corrupt()
		}
		r.b = uint64(r.d[0])
		r.nb = 8
		r.d = r.d[1:]
	}
	x |= (r.b & (1<<lg - 1)) << nb
	r.b >>= lg
	r.nb -= lg
	return int(x)
}

type deltaWriter struct {
	out *Buffer
	buf [10]byte
	b   uint64
	nb  uint
}

func (w *deltaWriter) init(out *Buffer) {
	w.out = out
	w.b = 0
	w.nb = 0
}

func (w *deltaWriter) Write(x int) {
	if writeVersion == 2 {
		if x == 0 {
			x = deltaZeroEnc
		} else if x >= deltaZeroEnc {
			x++
		}
		w.writeBits(uint64(x))
		return
	}

	n := binary.PutUvarint(w.buf[:], uint64(x))
	w.out.Write(w.buf[:n])
}

func (w *deltaWriter) writeBits(x uint64) {
	if int64(x) <= 0 {
		panic("bad gamma write")
	}
	lg := uint(bits.Len64(x)) - 1
	x &= 1<<lg - 1
	w.nb += lg
	if w.nb >= 8 {
		w.flushBits()
	}
	w.b |= 1 << w.nb
	w.nb++
	if lg > 32 {
		w.b |= uint64(uint32(x)) << w.nb
		w.nb += 32
		x >>= 32
		w.flushBits()
		lg -= 32
	}
	w.b |= x << w.nb
	w.nb += lg
	if w.nb >= 8 {
		w.flushBits()
	}
}

func (w *deltaWriter) flushBits() {
	for w.nb >= 8 {
		w.out.WriteByte(byte(w.b))
		w.b >>= 8
		w.nb -= 8
	}
}

func (w *deltaWriter) Flush() {
	w.flushBits()
	if w.nb > 0 {
		w.out.WriteByte(byte(w.b))
	}
	w.b = 0
	w.nb = 0
}
