// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

// Merging indexes.
//
// To merge two indexes A and B (newer) into a combined index C:
//
// Load the root list from B and determine for each root the fileid ranges
// that it will replace in A.
//
// Read A's and B's name lists together, merging them into C's name list.
// Discard the identified ranges from A during the merge.  Also during the merge,
// record the mapping from A's fileids to C's fileids, and also the mapping from
// B's fileids to C's fileids.  Both mappings can be summarized in a table like
//
//	10-14 map to 20-24
//	15-24 is deleted
//	25-34 maps to 40-49
//
// The number of ranges will be at most the combined number of roots.
// Also during the merge, write the name index to a temporary file as usual.
//
// Now merge the posting lists (this is why they begin with the trigram).
// During the merge, translate the fileid numbers to the new C fileid space.
// Also during the merge, write the posting list index to a temporary file as usual.
//
// Copy the name index and posting list index into C's index and write the trailer.
// Rename C's index onto the new index.

import (
	"encoding/binary"
	"fmt"
	"os"
)

// An idrange records that the half-open interval [lo, hi) maps to [new, new+hi-lo).
type idrange struct {
	lo, hi, new int
}

// writeVersion is the index version that IndexWriter and Merge should write.
// We only write older versions during testing.
var writeVersion = 2

// Merge creates a new index in the file dst that corresponds to merging
// the two indices src1 and src2.  If both src1 and src2 claim responsibility
// for a path, src2 is assumed to be newer and is given preference.
func Merge(dst, src1, src2 string) {
	ix1 := Open(src1)
	ix2 := Open(src2)

	// Build fileid maps.
	var i1, i2, new int
	var map1, map2 []idrange
	names1 := ix1.NamesAt(0, ix1.numName)
	names2 := ix2.NamesAt(0, ix2.numName)
	name1 := names1.Path()
	name2 := names2.Path()
	for root := range ix2.Roots().All() {
		// Determine range shadowed by this path.
		old := i1
		for i1 < ix1.numName && name1.Compare(root) < 0 {
			names1.Next()
			name1 = names1.Path()
			i1++
		}
		lo := i1

		// limit is the path where the scan should stop.
		// If root is foo, we want to scan foo/anything
		// but not food or fop. Slash compares equal to \x00,
		// and if foo is a zip file, foo\x01 is a file in it,
		// so use foo\x02 as the limit.
		limit := MakePath(root.String() + "\x02")
		for i1 < ix1.numName && name1.Compare(limit) < 0 {
			names1.Next()
			name1 = names1.Path()
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
		if i2 < ix2.numName && name2.Compare(root) < 0 {
			fmt.Fprintf(os.Stderr, "IX %v %v %d %d %q=%q < %q %v\n", ix1.version, ix2.version, i2, ix2.numName, ix2.Name(i2), name2, root, ix2.version)
			panic("merge: inconsistent index")
		}
		lo = i2
		for i2 < ix2.numName && name2.Compare(limit) < 0 {
			names2.Next()
			name2 = names2.Path()
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

	ix := bufCreate(dst)
	if writeVersion == 1 {
		ix.WriteString(magicV1)
	} else {
		ix.WriteString(magicV2)
	}

	// Merged list of paths.
	pathData := ix.Offset()
	last := MakePath("\xFF") // not a prefix of anything
	writeVersion = 2
	paths := NewPathWriter(ix, nil, writeVersion, 0)
	p1 := ix1.Roots()
	p2 := ix2.Roots()
	for p1.Valid() || p2.Valid() {
		var p Path
		if !p2.Valid() || p1.Valid() && p1.Path().Compare(p2.Path()) <= 0 {
			p = p1.Path()
			p1.Next()
		} else {
			p = p2.Path()
			p2.Next()
		}
		if p.HasPathPrefix(last) {
			continue
		}
		last = p
		paths.Write(p)
	}
	if writeVersion == 1 {
		paths.Write(MakePath(""))
	}

	// Merged list of names.
	ix.Align(16)
	nameData := ix.Offset()
	nameIndexFile := bufCreate("")
	start := ix.Offset()
	names := NewPathWriter(ix, nameIndexFile, writeVersion, nameGroupSize)
	m1 := map1
	m2 := map2
	for names.Count() != numName {
		switch {
		case len(m1) > 0 && m1[0].new == names.Count():
			names.Collect(ix1.Names(m1[0].lo, m1[0].hi))
			m1 = m1[1:]
		case len(m2) > 0 && m2[0].new == names.Count():
			names.Collect(ix2.Names(m2[0].lo, m2[0].hi))
			m2 = m2[1:]
		default:
			panic("merge: inconsistent index")
		}
	}
	if writeVersion == 1 {
		nameIndexFile.WriteUint(ix.Offset() - start)
		ix.WriteByte(0)
	}

	var want int
	if writeVersion == 1 {
		want = (names.Count() + 1) * 4
	} else {
		want = (names.Count() + nameGroupSize - 1) / nameGroupSize * 8
	}
	if nameIndexFile.Offset() != want {
		panic("merge: inconsistent index")
	}

	// Merged list of posting lists.
	ix.Align(16)
	postData := ix.Offset()
	var r1 postMapReader
	var r2 postMapReader
	var w postDataWriter
	r1.init(ix1, map1)
	r2.init(ix2, map2)
	postIndexFile := bufCreate("")
	w.init(ix, postIndexFile)
	old1, old2 := uint32(0), uint32(0)
	for {
		if !(r1.trigram > old1 || r2.trigram > old2) {
			panic("no progress")
		}
		old1, old2 = r1.trigram, r2.trigram
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
			w.trigram(r1.trigram)
			if r1.trigram == ^uint32(0) {
				w.endTrigram()
				break
			}
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
	if len(w.block) > 0 {
		w.flush()
	}

	// Name index
	ix.Align(16)
	nameIndex := ix.Offset()
	copyFile(ix, nameIndexFile)

	// Posting list index
	ix.Align(16)
	postIndex := ix.Offset()
	copyFile(ix, postIndexFile)

	// Trailer
	ix.Align(16)
	ix.WriteUint(pathData)
	if writeVersion == 2 {
		ix.WriteUint(paths.Count())
	}
	ix.WriteUint(nameData)
	if writeVersion == 2 {
		ix.WriteUint(names.Count())
	}
	ix.WriteUint(postData)
	if writeVersion == 2 {
		ix.WriteUint(w.numTrigram)
	}
	ix.WriteUint(nameIndex)
	ix.WriteUint(postIndex)

	if writeVersion == 1 {
		ix.WriteString(trailerMagicV1)
	} else {
		ix.WriteString(trailerMagicV2)
	}
	ix.Flush()

	os.Remove(nameIndexFile.name)
	os.Remove(w.postIndexFile.name)
}

type postMapReader struct {
	ix        *Index
	idmap     []idrange
	trigram   uint32
	count     int
	offset    int
	oldid     int
	fileid    int
	i         int
	delta     deltaReader
	block     []byte
	nextBlock int
	triNum    int
}

func (r *postMapReader) init(ix *Index, idmap []idrange) {
	r.ix = ix
	r.idmap = idmap
	r.trigram = ^uint32(0)
	r.nextBlock = 0
	r.triNum = -1
	r.load(true)
}

func (r *postMapReader) nextTrigram() {
	r.load(false)
}

func (r *postMapReader) load(force bool) {
	if !force && r.trigram == ^uint32(0) {
		return
	}
	r.triNum++
	if r.triNum >= r.ix.numPost {
		r.trigram = ^uint32(0)
		r.count = 0
		r.fileid = -1
		return
	}

	if r.ix.version == 1 {
		r.trigram, r.count, r.offset = r.ix.postIndexEntry(r.triNum)
	} else {
		b := r.block
		if b == nil || len(b) < 3 || b[0] == 0 && b[1] == 0 && b[2] == 0 {
			r.block = r.ix.slice(r.ix.postIndex+r.nextBlock, postBlockSize)
			r.nextBlock += postBlockSize
			b = r.block
			r.offset = 0
		}
		r.trigram = uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		b = b[3:]
		n1, l := binary.Uvarint(b)
		if l <= 0 {
			r.ix.corrupt()
		}
		b = b[l:]
		n2, l := binary.Uvarint(b)
		if l <= 0 {
			r.ix.corrupt()
		}
		b = b[l:]
		r.count = int(n1)
		r.offset += int(n2)
		r.block = b
	}
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
