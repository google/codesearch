// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

type buffer []byte

func (b *buffer) writeByte(x byte) { *b = append(*b, x) }
func (b *buffer) write(x []byte)   { *b = append(*b, x...) }

func TestDelta(t *testing.T) {
	old := writeVersion
	defer func() {
		writeVersion = old
	}()

	for v := 1; v <= 2; v++ {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			writeVersion = v
			vals := []int{0, 1, 2, 3, 1, 2, 3, 4, 5, 6, 10000, 1, 2, 3}
			var w deltaWriter
			b := new(Buffer)
			w.init(b)
			for _, v := range vals {
				w.Write(v)
			}
			w.Flush()

			writeVersion++ // make sure reader doesn't look
			var r deltaReader
			r.init(&Index{version: v}, b.buf)
			for i, v := range vals {
				d := r.next()
				if d != v {
					t.Fatalf("at #%d: next() = %v, want %v", i, d, v)
				}
			}
			r.clearBits()
			if len(r.d) > 0 {
				t.Fatalf("leftover data")
			}
		})
	}
}

func TestFileList64(t *testing.T) {
	old := writeVersion
	defer func() {
		writeVersion = old
	}()
	writeVersion = 2

	vals := []int{0, 1, 2, 3, 4, 5}
	var w deltaWriter
	b := new(Buffer)
	w.init(b)
	last := -1
	for _, v := range vals {
		w.Write(v - last)
		last = v
	}
	w.Write(0)
	w.Flush()

	f := fileList64(vals...)
	if f != string(b.buf) {
		t.Errorf("deltaWriter=%x but fileList64=%x", string(b.buf), f)
	}
}

func TestGammaWriter(t *testing.T) {
	const N = 10000
	var w deltaWriter
	b := new(Buffer)
	w.init(b)
	for i := range N {
		w.Write(i + 1)
	}
	w.Flush()

	var r deltaReader
	r.init(&Index{version: 2}, b.buf)
	for i := range N {
		j := r.next()
		if j != i+1 {
			t.Fatalf("read() = %d, want %d", j, i+1)
		}
	}
}

func TestGammaWriterRand(t *testing.T) {
	const N = 10000
	var pcg rand.PCG
	var w deltaWriter
	b := new(Buffer)
	w.init(b)
	pcg.Seed(1, 1)
	for range N {
		x := pcg.Uint64()
		i := int(x & (1<<(1+(x>>58)) - 1))
		if i <= 0 {
			i = 1
		}
		w.Write(i)
	}
	w.Flush()

	pcg.Seed(1, 1)
	var r deltaReader
	r.init(&Index{version: 2}, b.buf)
	for seq := range N {
		x := pcg.Uint64()
		i := int(x & (1<<(1+(x>>58)) - 1))
		if i <= 0 {
			i = 1
		}
		j := r.next()
		if j != i {
			t.Fatalf("read(#%d) = %d, want %d", seq, j, i)
		}
	}
}
