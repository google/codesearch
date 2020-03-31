// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"archive/zip"
	"bytes"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"testing"
	"time"
)

var trivialFiles = map[string]string{
	"f0":       "\n\n",
	"file1":    "\na\n",
	"thefile2": "\nab\n",
	"file3":    "\nabc\n",
	"afile4":   "\ndabc\n",
	"file5":    "\nxyzw\n",
}

var trivialIndex32 = join(
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
	"thefile2\x00",
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

var trivialIndex64 = join(
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
	"thefile2\x00",
	"\x00",

	// list of posting lists
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

	// name index
	u64(0),
	u64(6+1),
	u64(6+1+2+1),
	u64(6+1+2+1+5+1),
	u64(6+1+2+1+5+1+5+1),
	u64(6+1+2+1+5+1+5+1+5+1),
	u64(6+1+2+1+5+1+5+1+5+1+8+1),

	// posting list index,
	"\na\n", u64(1), u64(0),
	"\nab", u64(2), u64(4),
	"\nda", u64(1), u64(4+5),
	"\nxy", u64(1), u64(4+5+4),
	"ab\n", u64(1), u64(4+5+4+4),
	"abc", u64(2), u64(4+5+4+4+4),
	"bc\n", u64(2), u64(4+5+4+4+4+5),
	"dab", u64(1), u64(4+5+4+4+4+5+5),
	"xyz", u64(1), u64(4+5+4+4+4+5+5+4),
	"yzw", u64(1), u64(4+5+4+4+4+5+5+4+4),
	"zw\n", u64(1), u64(4+5+4+4+4+5+5+4+4+4),
	"\xff\xff\xff", u64(0), u64(4+5+4+4+4+5+5+4+4+4+4),

	"\x00\x00", // padding

	// trailer
	u64(16),
	u64(16+1),
	u64(16+1+38),
	u64(16+1+38+51),
	u64(16+1+38+51+56),

	"\ncsearch trlr64\n",
)

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
		delta := x - last + 1
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
	b |= 1 << nb // trailing zero (encoded as 1)
	nb++
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

func buildFlushIndex(out string, paths []string, doFlush bool, fileData map[string]string) {
	ix := Create(out)
	ix.Zip = true
	ix.AddPaths(paths)
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

func buildIndex(name string, paths []string, fileData map[string]string) {
	buildFlushIndex(name, paths, false, fileData)
}

func testTrivialWrite(t *testing.T, doFlush bool) {
	old := writeOldIndex
	defer func() {
		writeOldIndex = old
	}()

	for size := 32; size <= 64; size += 32 {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			writeOldIndex = size == 32
			f, _ := ioutil.TempFile("", "index-test")
			defer os.Remove(f.Name())
			out := f.Name()
			buildFlushIndex(out, nil, doFlush, trivialFiles)

			data, err := ioutil.ReadFile(out)
			if err != nil {
				t.Fatalf("reading _test/index.triv: %v", err)
			}
			var want []byte
			if size == 32 {
				want = []byte(trivialIndex32)
			} else {
				want = []byte(trivialIndex64)
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

	f1, _ := ioutil.TempFile("", "index-test")
	defer os.Remove(f1.Name())
	out1 := f1.Name()
	buildIndex(out1, []string{"x.zip"}, map[string]string{"x.zip": buf.String()})

	ix := Open(out1)

	checkFiles(t, ix,
		"x.zip#a/x",
		"x.zip#a/y",
		"x.zip#b/www",
		"x.zip#b/xx",
		"x.zip#b/yy",
		"x.zip#c/ab",
		"x.zip#c/de",
		"x.zip#cc",
	)

	checkPosting(t, ix, "all", 5)
	checkPosting(t, ix, "wor", 0, 1, 2)
	checkPosting(t, ix, "now", 3, 4, 6)
	checkPosting(t, ix, "pot", 4, 5, 7)
}
