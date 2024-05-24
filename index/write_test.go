// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"archive/zip"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

func init() {
	panicOnCorrupt = true
}

var trivialFiles = map[string]string{
	"f0":       "\n\n",
	"file1":    "\na\n",
	"the/file": "\nab\n",
	"file3":    "\nabc\n",
	"afile4":   "\ndabc\n",
	"file5":    "\nxyzw\n",
}

var trivialIndexV1 = join(
	// header
	"csearch index 1\n",

	// list of paths
	"\x00",

	// list of names
	"afile4\x00",
	"f0\x00",
	"file1\x00",
	"file3\x00",
	"file5\x00",
	"the/file\x00",
	"\x00",

	// list of posting lists
	"\na\n", fileList(2), // file1
	"\nab", fileList(3, 5), // file3, thefile2
	"\nda", fileList(0), // afile4
	"\nxy", fileList(4), // file5
	"ab\n", fileList(5), // thefile2
	"abc", fileList(0, 3), // afile4, file3
	"bc\n", fileList(0, 3), // afile4, file3
	"dab", fileList(0), // afile4
	"xyz", fileList(4), // file5
	"yzw", fileList(4), // file5
	"zw\n", fileList(4), // file5
	"\xff\xff\xff", fileList(),

	// name index
	u32(0),
	u32(6+1),
	u32(6+1+2+1),
	u32(6+1+2+1+5+1),
	u32(6+1+2+1+5+1+5+1),
	u32(6+1+2+1+5+1+5+1+5+1),
	u32(6+1+2+1+5+1+5+1+5+1+8+1),

	// posting list index,
	"\na\n", u32(1), u32(0),
	"\nab", u32(2), u32(5),
	"\nda", u32(1), u32(5+6),
	"\nxy", u32(1), u32(5+6+5),
	"ab\n", u32(1), u32(5+6+5+5),
	"abc", u32(2), u32(5+6+5+5+5),
	"bc\n", u32(2), u32(5+6+5+5+5+6),
	"dab", u32(1), u32(5+6+5+5+5+6+6),
	"xyz", u32(1), u32(5+6+5+5+5+6+6+5),
	"yzw", u32(1), u32(5+6+5+5+5+6+6+5+5),
	"zw\n", u32(1), u32(5+6+5+5+5+6+6+5+5+5),
	"\xff\xff\xff", u32(0), u32(5+6+5+5+5+6+6+5+5+5+5),

	// trailer
	u32(16),
	u32(16+1),
	u32(16+1+38),
	u32(16+1+38+62),
	u32(16+1+38+62+28),

	"\ncsearch trailr\n",
)

var trivialIndexV2 = join(
	// header
	"csearch index 2\n",

	// list of paths (empty)

	// list of names
	pad(16,
		"\x00\x06afile4",
		"\x00\x02f0",
		"\x01\x04ile1",
		"\x04\x013",
		"\x04\x015",
		"\x00\x08the/file",
	),

	// list of posting lists
	pad(16,
		"\na\n", fileList64(2), // file1; 1-byte file list
		"\nab", fileList64(3, 5), // file3, thefile2; 2-byte file list
		"\nda", fileList64(0), // afile4; 1-byte file list
		"\nxy", fileList64(4), // file5; 1-byte file list
		"ab\n", fileList64(5), // thefile2; 1-byte file list
		"abc", fileList64(0, 3), // afile4, file3; 2-byte file list
		"bc\n", fileList64(0, 3), // afile4, file3; 2-byte file list
		"dab", fileList64(0), // afile4; 1-byte file list
		"xyz", fileList64(4), // file5; 1-byte file list
		"yzw", fileList64(4), // file5; 1-byte file list
		"zw\n", fileList64(4), // file5; 1-byte file list
		"\xff\xff\xff", fileList64(),
	),

	// name index
	pad(16,
		u64(0),
	),

	// posting list index block
	pad(postBlockSize,
		"\na\n", uv(1), uv(0),
		"\nab", uv(2), uv(5),
		"\nda", uv(1), uv(6),
		"\nxy", uv(1), uv(5),
		"ab\n", uv(1), uv(5),
		"abc", uv(2), uv(5),
		"bc\n", uv(2), uv(5),
		"dab", uv(1), uv(5),
		"xyz", uv(1), uv(5),
		"yzw", uv(1), uv(5),
		"zw\n", uv(1), uv(5),
		"\xff\xff\xff", uv(0), uv(5),
	),

	// trailer
	u64(0x10), // offset to list of paths
	u64(0),    // number of paths
	u64(0x10), // offset to list of names
	u64(6),    // number of names
	u64(0x40), // offset to posting lists
	u64(12),   // number of posting lists / trigrams
	u64(0x80), // offset to name index
	u64(0x90), // offset to posting index

	"\ncsearch trlr 2\n",
)

func pad(n int, list ...string) string {
	s := strings.Join(list, "")
	frag := len(s) % n
	if frag != 0 {
		s += strings.Repeat("\x00", n-frag)
	}
	return s
}

func uv(n int) string {
	var buf [binary.MaxVarintLen64]byte
	n = binary.PutUvarint(buf[:], uint64(n))
	return string(buf[:n])
}

func join(s ...string) string {
	return strings.Join(s, "")
}

func u32(x uint32) string {
	var buf [4]byte
	buf[0] = byte(x >> 24)
	buf[1] = byte(x >> 16)
	buf[2] = byte(x >> 8)
	buf[3] = byte(x)
	return string(buf[:])
}

func u64(x uint64) string {
	return u32(uint32(x>>32)) + u32(uint32(x))
}

func fileList(list ...int) string {
	var buf []byte

	last := -1
	for _, x := range list {
		delta := x - last
		for delta >= 0x80 {
			buf = append(buf, byte(delta)|0x80)
			delta >>= 7
		}
		buf = append(buf, byte(delta))
		last = x
	}
	buf = append(buf, 0)
	return string(buf)
}

func fileList64(list ...int) string {
	var b uint64
	var nb uint

	last := -1
	for _, x := range list {
		delta := x - last
		if delta >= deltaZeroEnc {
			delta++
		}
		last = x
		nbit := 0
		for delta > 1<<(nbit+1)-1 {
			nbit++
		}
		nb += uint(nbit)
		b |= 1 << nb
		nb++
		delta &^= 1 << nbit
		b |= uint64(delta) << nb
		nb += uint(nbit)
	}
	nb += 4
	b |= 1 << nb
	nb++
	nb += 4
	if nb > 64 {
		panic("fileList64: too long")
	}

	var buf []byte
	for nb > 8 {
		buf = append(buf, byte(b))
		b >>= 8
		nb -= 8
	}
	buf = append(buf, byte(b))
	return string(buf)
}

type stringFile struct {
	*strings.Reader
	name string
	size int64
}

func (f *stringFile) Stat() (os.FileInfo, error) {
	return f, nil
}

func (f *stringFile) Name() string       { return f.name }
func (f *stringFile) Size() int64        { return f.size }
func (f *stringFile) Mode() os.FileMode  { return 0444 }
func (f *stringFile) ModTime() time.Time { return time.Time{} }
func (f *stringFile) IsDir() bool        { return false }
func (f *stringFile) Sys() interface{}   { return nil }

func apply[In, Out any](f func(In) Out, xs []In) []Out {
	var ys []Out
	for _, x := range xs {
		ys = append(ys, f(x))
	}
	return ys
}

func buildFlushIndex(out string, roots []string, doFlush bool, fileData map[string]string) {
	ix := Create(out)
	ix.Zip = true

	ix.AddRoots(apply(MakePath, roots))
	var files []string
	for name := range fileData {
		files = append(files, name)
	}
	sort.Strings(files)
	for i, name := range files {
		file := &stringFile{
			strings.NewReader(fileData[name]),
			name,
			int64(len(fileData[name])),
		}
		ix.Add(name, file)
		if doFlush && i == len(files)/2 {
			ix.flushPost()
		}
	}
	if doFlush {
		ix.flushPost()
	}
	ix.Flush()
}

func buildIndex(name string, roots []string, fileData map[string]string) {
	buildFlushIndex(name, roots, false, fileData)
}

func testTrivialWrite(t *testing.T, doFlush bool) {
	old := writeVersion
	defer func() {
		writeVersion = old
	}()

	for v := 1; v <= 2; v++ {
		t.Run(fmt.Sprint("V", v), func(t *testing.T) {
			writeVersion = v
			f, _ := os.CreateTemp("", "index-test")
			defer os.Remove(f.Name())
			out := f.Name()
			buildFlushIndex(out, nil, doFlush, trivialFiles)

			data, err := os.ReadFile(out)
			if err != nil {
				t.Fatalf("reading _test/index.triv: %v", err)
			}
			var want []byte
			if v == 1 {
				want = []byte(trivialIndexV1)
			} else {
				want = []byte(trivialIndexV2)
			}
			if !bytes.Equal(data, want) {
				i := 0
				for i < len(data) && i < len(want) && data[i] == want[i] {
					i++
				}
				t.Fatalf("mismatch at offset %#x:\nhave:\n%s\nwant:\n%s", i, hex.Dump(data), hex.Dump(want))
			}
		})
	}
}

func TestTrivialWrite(t *testing.T) {
	testTrivialWrite(t, false)
}

func TestTrivialWriteDisk(t *testing.T) {
	testTrivialWrite(t, true)
}

func TestHeap(t *testing.T) {
	h := &postHeap{}
	es := []postEntry{7, 4, 3, 2, 4}
	for _, e := range es {
		h.addMem([]postEntry{e})
	}
	if len(h.ch) != len(es) {
		t.Fatalf("wrong heap size: %d, want %d", len(h.ch), len(es))
	}
	for a, b := h.next(), h.next(); b.trigram() != invalidTrigram; a, b = b, h.next() {
		if a > b {
			t.Fatalf("%d should <= %d", a, b)
		}
	}
}

func TestZip(t *testing.T) {
	var buf bytes.Buffer
	w := zip.NewWriter(&buf)
	files := []string{
		"a/x", "hello world",
		"a/y", "goodbye world",
		"b/www", "world wide indeed",
		"b/xx", "no, not now",
		"b/yy", "first potatoes, now liberty?",
		"c/ab", "give me all the potatoes",
		"c/de", "or give me death now",
		"cc", "come to the aid of his potatoes",
	}
	for i := 0; i < len(files); i += 2 {
		ww, err := w.Create(files[i])
		if err != nil {
			t.Fatal(err)
		}
		ww.Write([]byte(files[i+1]))
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	f1, _ := os.CreateTemp("", "index-test")
	defer os.Remove(f1.Name())
	out1 := f1.Name()
	buildIndex(out1, []string{"x.zip"}, map[string]string{"x.zip": buf.String()})

	ix := Open(out1)

	checkFiles(t, ix,
		"x.zip\x01a/x",
		"x.zip\x01a/y",
		"x.zip\x01b/www",
		"x.zip\x01b/xx",
		"x.zip\x01b/yy",
		"x.zip\x01c/ab",
		"x.zip\x01c/de",
		"x.zip\x01cc",
	)

	checkPosting(t, ix, "all", 5)
	checkPosting(t, ix, "wor", 0, 1, 2)
	checkPosting(t, ix, "now", 3, 4, 6)
	checkPosting(t, ix, "pot", 4, 5, 7)
}
