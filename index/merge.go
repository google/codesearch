// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

// Merging indexes.
//
// To merge two indexes A and B (newer) into a combined index C:
//
// Load the path list from B and determine for each path the docid ranges
// that it will replace in A.
//
// Read A's and B's name lists together, merging them into C's name list.
// Discard the identified ranges from A during the merge.  Also during the merge,
// record the mapping from A's docids to C's docids, and also the mapping from
// B's docids to C's docids.  Both mappings can be summarized in a table like
//
//	10-14 map to 20-24
//	15-24 is deleted
//	25-34 maps to 40-49
//
// The number of ranges will be at most the combined number of paths.
// Also during the merge, write the name index to a temporary file as usual.
//
// Now merge the posting lists (this is why they begin with the trigram).
// During the merge, translate the docid numbers to the new C docid space.
// Also during the merge, write the posting list index to a temporary file as usual.
//
// Copy the name index and posting list index into C's index and write the trailer.
// Rename C's index onto the new index.

import (
	"os"
	"strings"
)

// An idrange records that the half-open interval [lo, hi) maps to [new, new+hi-lo).
type idrange struct {
	lo, hi, new int
}

type postIndex struct {
	tri    uint32
	count  int
	offset int
}

var writeOldIndex = false

// Merge creates a new index in the file dst that corresponds to merging
// the two indices src1 and src2.  If both src1 and src2 claim responsibility
// for a path, src2 is assumed to be newer and is given preference.
func Merge(dst, src1, src2 string) {
	ix1 := Open(src1)
	ix2 := Open(src2)
	paths1 := ix1.Paths()
	paths2 := ix2.Paths()

	// Build docid maps.
	var i1, i2, new int
	var map1, map2 []idrange
	for _, path := range paths2 {
		// Determine range shadowed by this path.
		old := i1
		for i1 < ix1.numName && ix1.Name(i1) < path {
			i1++
		}
		lo := i1
		limit := path[:len(path)-1] + string(path[len(path)-1]+1)
		for i1 < ix1.numName && ix1.Name(i1) < limit {
			i1++
		}
		hi := i1

		// Record range before the shadow.
		if old < lo {
			map1 = append(map1, idrange{old, lo, new})
			new += lo - old
		}

		// Determine range defined by this path.
		// Because we are iterating over the ix2 paths,
		// there can't be gaps, so it must start at i2.
		if i2 < ix2.numName && ix2.Name(i2) < path {
			panic("merge: inconsistent index")
		}
		lo = i2
		for i2 < ix2.numName && ix2.Name(i2) < limit {
			i2++
		}
		hi = i2
		if lo < hi {
			map2 = append(map2, idrange{lo, hi, new})
			new += hi - lo
		}
	}

	if i1 < ix1.numName {
		map1 = append(map1, idrange{i1, ix1.numName, new})
		new += ix1.numName - i1
	}
	if i2 < ix2.numName {
		panic("merge: inconsistent index")
	}
	numName := new

	ix3 := bufCreate(dst)
	ix3.writeString(magic)

	// Merged list of paths.
	pathData := ix3.offset()
	mi1 := 0
	mi2 := 0
	last := "\x00" // not a prefix of anything
	for mi1 < len(paths1) || mi2 < len(paths2) {
		var p string
		if mi2 >= len(paths2) || mi1 < len(paths1) && paths1[mi1] <= paths2[mi2] {
			p = paths1[mi1]
			mi1++
		} else {
			p = paths2[mi2]
			mi2++
		}
		if strings.HasPrefix(p, last) {
			continue
		}
		last = p
		ix3.writeString(p)
		ix3.writeString("\x00")
	}
	ix3.writeString("\x00")

	// Merged list of names.
	nameData := ix3.offset()
	nameIndexFile := bufCreate("")
	new = 0
	mi1 = 0
	mi2 = 0
	for new < numName {
		if mi1 < len(map1) && map1[mi1].new == new {
			for i := map1[mi1].lo; i < map1[mi1].hi; i++ {
				name := ix1.Name(i)
				nameIndexFile.writeUint(ix3.offset() - nameData)
				ix3.writeString(name)
				ix3.writeString("\x00")
				new++
			}
			mi1++
		} else if mi2 < len(map2) && map2[mi2].new == new {
			for i := map2[mi2].lo; i < map2[mi2].hi; i++ {
				name := ix2.Name(i)
				nameIndexFile.writeUint(ix3.offset() - nameData)
				ix3.writeString(name)
				ix3.writeString("\x00")
				new++
			}
			mi2++
		} else {
			panic("merge: inconsistent index")
		}
	}
	scale := 8
	if writeOldIndex {
		scale = 4
	}
	if new*scale != nameIndexFile.offset() {
		panic("merge: inconsistent index")
	}
	nameIndexFile.writeUint(ix3.offset())

	// Merged list of posting lists.
	postData := ix3.offset()
	var r1 postMapReader
	var r2 postMapReader
	var w postDataWriter
	r1.init(ix1, map1)
	r2.init(ix2, map2)
	w.init(ix3, true)
	for {
		if r1.trigram < r2.trigram {
			w.trigram(r1.trigram)
			for r1.nextId() {
				w.fileid(r1.fileid)
			}
			r1.nextTrigram()
			w.endTrigram()
		} else if r2.trigram < r1.trigram {
			w.trigram(r2.trigram)
			for r2.nextId() {
				w.fileid(r2.fileid)
			}
			r2.nextTrigram()
			w.endTrigram()
		} else {
			if r1.trigram == ^uint32(0) {
				break
			}
			w.trigram(r1.trigram)
			r1.nextId()
			r2.nextId()
			for r1.fileid != -1 || r2.fileid != -1 {
				if uint(r1.fileid) < uint(r2.fileid) {
					w.fileid(r1.fileid)
					r1.nextId()
				} else if uint(r2.fileid) < uint(r1.fileid) {
					w.fileid(r2.fileid)
					r2.nextId()
				} else {
					panic("merge: inconsistent index")
				}
			}
			r1.nextTrigram()
			r2.nextTrigram()
			w.endTrigram()
		}
	}

	// Name index
	nameIndex := ix3.offset()
	copyFile(ix3, nameIndexFile)

	// Posting list index
	postIndex := ix3.offset()
	copyFile(ix3, w.postIndexFile)

	ix3.writeUint(pathData)
	ix3.writeUint(nameData)
	ix3.writeUint(postData)
	ix3.writeUint(nameIndex)
	ix3.writeUint(postIndex)
	if writeOldIndex {
		ix3.writeString(trailerMagic32)
	} else {
		ix3.writeString(trailerMagic64)
	}
	ix3.flush()

	os.Remove(nameIndexFile.name)
	os.Remove(w.postIndexFile.name)
}

type postMapReader struct {
	ix      *Index
	idmap   []idrange
	triNum  int
	trigram uint32
	count   int
	offset  int
	oldid   int
	fileid  int
	i       int
	delta   deltaReader
}

func (r *postMapReader) init(ix *Index, idmap []idrange) {
	r.ix = ix
	r.idmap = idmap
	r.trigram = ^uint32(0)
	r.load()
}

func (r *postMapReader) nextTrigram() {
	r.triNum++
	r.load()
}

func (r *postMapReader) load() {
	if r.triNum >= r.ix.numPost {
		r.trigram = ^uint32(0)
		r.count = 0
		r.fileid = -1
		return
	}
	r.trigram, r.count, r.offset = r.ix.listAt(r.triNum * r.ix.postEntrySize)
	if r.count == 0 {
		r.fileid = -1
		return
	}
	r.delta.init(r.ix, r.ix.slice(r.ix.postData+r.offset+3, -1))
	r.oldid = -1
	r.i = 0
}

func (r *postMapReader) nextId() bool {
	for r.count > 0 {
		r.count--
		delta := r.delta.next()
		if delta <= 0 {
			r.ix.corrupt()
		}
		r.oldid += delta
		for r.i < len(r.idmap) && r.idmap[r.i].hi <= r.oldid {
			r.i++
		}
		if r.i >= len(r.idmap) {
			r.count = 0
			break
		}
		if r.oldid < r.idmap[r.i].lo {
			continue
		}
		r.fileid = r.idmap[r.i].new + r.oldid - r.idmap[r.i].lo
		return true
	}

	r.fileid = -1
	return false
}

type postDataWriter struct {
	out           *bufWriter
	postIndexFile *bufWriter
	base          int
	count         int
	offset        int
	last          int
	t             uint32
	delta         deltaWriter
}

func (w *postDataWriter) init(out *bufWriter, doIndex bool) {
	w.out = out
	w.base = out.offset()
	w.postIndexFile = nil
	w.delta.init(out)
	if doIndex {
		w.postIndexFile = bufCreate("")
	}
}

func (w *postDataWriter) trigram(t uint32) {
	w.offset = w.out.offset()
	w.count = 0
	w.t = t
	w.last = -1
}

func (w *postDataWriter) fileid(id int) {
	if w.count == 0 {
		w.out.writeTrigram(w.t)
	}
	w.delta.write(id - w.last)
	w.last = id
	w.count++
}

func (w *postDataWriter) endTrigram() {
	if w.count == 0 {
		return
	}
	w.delta.write(0)
	w.delta.flush()
	if w.postIndexFile != nil {
		w.postIndexFile.writeTrigram(w.t)
		w.postIndexFile.writeUint(w.count)
		w.postIndexFile.writeUint(w.offset - w.base)
	}
}
