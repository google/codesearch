// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"fmt"
	"testing"
)

type buffer []byte

func (b *buffer) writeByte(x byte) { *b = append(*b, x) }
func (b *buffer) write(x []byte)   { *b = append(*b, x...) }

func TestDelta(t *testing.T) {
	old := writeOldIndex
	defer func() {
		writeOldIndex = old
	}()

	for size := 32; size <= 64; size += 32 {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			writeOldIndex = size == 32
			vals := []int{0, 1, 2, 3, 1, 2, 3, 4, 5, 6, 10000, 1, 2, 3}
			var w deltaWriter
			var b buffer
			w.init(&b)
			for _, v := range vals {
				w.write(v)
			}
			w.flush()

			writeOldIndex = !writeOldIndex // make sure reader doesn't look
			var r deltaReader
			r.init(&Index{is64: size == 64}, b)
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
	old := writeOldIndex
	defer func() {
		writeOldIndex = old
	}()
	writeOldIndex = false

	vals := []int{0, 1, 2, 3, 4, 5}
	var w deltaWriter
	var b buffer
	w.init(&b)
	last := -1
	for _, v := range vals {
		w.write(v - last)
		last = v
	}
	w.write(0)
	w.flush()

	f := fileList64(vals...)
	if f != string(b) {
		t.Errorf("deltaWriter=%x but fileList64=%x", string(b), f)
	}
}
