// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"bytes"
	"encoding/binary"
	"iter"
	"os"
	"strings"
)

// A Path is a Path stored in the index,
// either in the root list or the file list.
//
// Paths stored in the index are ordered
// using the [Path.cmp] method.
type Path struct {
	s string
}

func MakePath(s string) Path {
	return Path{s}
}

func (p Path) String() string {
	return p.s
}

func (p Path) HasPathPrefix(parent Path) bool {
	return strings.HasPrefix(p.s, parent.s) &&
		(p.s == parent.s ||
			p.s[len(parent.s)] == '/' ||
			p.s[len(parent.s)] == os.PathSeparator)
}

// Compare returns the comparison of p and q.
// It is analogous to strings.Compare(p, q)
// but x/y is ordered before x.foo by treating
// slashes as if they had byte value 0.
// On Windows, backslashes are treated as equal to slashes.
func (p Path) Compare(q Path) int {
	for i := range min(len(p.s), len(q.s)) {
		pi := p.s[i]
		qi := q.s[i]
		if pi == '/' || pi == os.PathSeparator {
			pi = 0
		}
		if qi == '/' || qi == os.PathSeparator {
			qi = 0
		}
		if pi != qi {
			return int(pi) - int(qi)
		}
	}
	return len(p.s) - len(q.s)
}

type PathWriter struct {
	data    *Buffer
	index   *Buffer
	version int
	group   int
	start   int
	n       int
	last    Path
}

func NewPathWriter(data, index *Buffer, version, group int) *PathWriter {
	if version != 1 && version != 2 {
		panic("bad PathWriter version")
	}
	return &PathWriter{
		data:    data,
		index:   index,
		version: version,
		group:   group,
		start:   data.Offset(),
	}
}

func (w *PathWriter) Write(p Path) {
	if w.version == 1 {
		if w.index != nil {
			w.index.WriteUint(w.data.Offset() - w.start)
		}
		w.data.WriteString(p.s)
		w.data.WriteByte(0)
		return
	}

	pre := 0
	if w.group == 0 && w.n == 0 || w.group > 0 && w.n%w.group == 0 {
		if w.index != nil {
			w.index.WriteUint(w.data.Offset() - w.start)
		}
	} else {
		for pre < len(w.last.s) && pre < len(p.s) && w.last.s[pre] == p.s[pre] {
			pre++
		}
	}
	w.data.WriteVarint(pre)
	w.data.WriteVarint(len(p.s) - pre)
	w.data.WriteString(string(p.s[pre:]))
	w.last = p
	w.n++
}

// Count returns the number of paths written to w.
func (w *PathWriter) Count() int {
	return w.n
}

// Collect iterates over paths and writes all the paths to w.
func (w *PathWriter) Collect(paths iter.Seq[Path]) {
	for p := range paths {
		w.Write(p)
	}
}

type PathReader struct {
	version int
	data    []byte
	path    Path
	n       int
	limit   int
}

func NewPathReader(version int, data []byte, limit int) *PathReader {
	if version != 1 && version != 2 {
		panic("bad PathWriter version")
	}
	r := &PathReader{
		version: version,
		data:    data,
		limit:   limit,
	}
	r.Next()
	return r
}

func (r *PathReader) All() iter.Seq[Path] {
	return func(yield func(Path) bool) {
		if !r.Valid() {
			return
		}
		for yield(r.Path()) && r.Next() {
			continue
		}
	}
}

func (r *PathReader) Valid() bool {
	return r.path.s != ""
}

func (r *PathReader) Next() bool {
	if r.limit == 0 {
		r.path.s = ""
		return false
	}
	if r.limit > 0 {
		r.limit--
	}
	if r.version == 1 {
		i := bytes.IndexByte(r.data, '\x00')
		if i <= 0 {
			r.path.s = ""
			return false
		}
		r.path.s, r.data = string(r.data[:i]), r.data[i+1:]
		return true
	}

	pre, w := binary.Uvarint(r.data)
	if w <= 0 || pre > uint64(len(r.path.s)) {
		r.path.s = ""
		return false
	}
	r.data = r.data[w:]

	n, w := binary.Uvarint(r.data)
	if w <= 0 || n > uint64(len(r.data)-w) {
		r.path.s = ""
		return false
	}
	r.data = r.data[w:]
	r.path.s = r.path.s[:pre] + string(r.data[:n])
	r.data = r.data[n:]
	return true
}

func (r *PathReader) Path() Path {
	return r.path
}

func (r *PathReader) NumPaths() int {
	return r.n
}
