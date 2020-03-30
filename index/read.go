// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

// Index format.
//
// An index stored on disk has the format:
//
//	"csearch index 1\n"
//	list of paths
//	list of names
//	list of posting lists
//	name index
//	posting list index
//	trailer
//
// The list of paths is a sorted sequence of NUL-terminated file or directory names.
// The index covers the file trees rooted at those paths.
// The list ends with an empty name ("\x00").
//
// The list of names is a sorted sequence of NUL-terminated file names.
// The initial entry in the list corresponds to file #0,
// the next to file #1, and so on.  The list ends with an
// empty name ("\x00").
//
// The list of posting lists are a sequence of posting lists.
// Each posting list has the form:
//
//	trigram [3]
//	deltas [v]...
//
// The trigram gives the 3 byte trigram that this list describes.  The
// delta list is a sequence of Fibonacci-coded deltas between file
// IDs, ending with a zero delta.  For example, the delta list [2,5,1,1,0]
// encodes the file ID list 1, 6, 7, 8.  The delta list [0] would
// encode the empty file ID list, but empty posting lists are usually
// not recorded at all.  The list of posting lists ends with an entry
// with trigram "\xff\xff\xff" and a delta list consisting a single zero.
//
// The indexes enable efficient random access to the lists.  The name
// index is a sequence of 8-byte big-endian values listing the byte
// offset in the name list where each name begins.  The posting list
// index is a sequence of index entries describing each successive
// posting list.  Each index entry has the form:
//
//	trigram [3]
//	file count [8]
//	offset [8]
//
// Index entries are only written for the non-empty posting lists,
// so finding the posting list for a specific trigram requires a
// binary search over the posting list index.  In practice, the majority
// of the possible trigrams are never seen, so omitting the missing
// ones represents a significant storage savings.
//
// The trailer has the form:
//
//	offset of path list [8]
//	offset of name list [8]
//	offset of posting lists [8]
//	offset of name index [8]
//	offset of posting list index [8]
//	"\ncsearch trlr64\n"
//
// The code has never checked the index header, so version changes
// must be made by modifying the trailer.
//
//
// Old 32-bit Version
//
// An older 32-bit format had the following differences:
//
//  - The trailer was "\ncsearch trailr\n"
//  - The offsets in the trailer were 4-byte instead of 8-byte.
//  - The name index was 4-byte values instead of 8-byte.
//  - The trigram index entry used 4-byte offsets instead of 8-byte.
//
// The older format also used byte-wise uvarint encoding to store
// posting list deltas.
//
// At the time of conversion, one local .csearchindex file had the
// following file region sizes:
//
//		         16 header
//		      7,710 path list
//		 26,619,905 name list
//		553,134,142 posting lists
//		  1,602,912 name index
//		 11,015,125 posting list index
//		         36 trailer
//		-----------
//		592,379,846 total
//
// The 64-bit version of this file would have instead:
//
//		         16 header
//		      7,710 path list
//		 26,619,905 name list
//		553,134,142 posting lists
//		  3,205,824 name index
//		 19,026,125 posting list index
//		         36 trailer
//		-----------
//		601,993,758 total
//
// Overall, the 64-bit offsets cause a 1.6% increase in size
// for a large index.
//
// The 64-bit version of this file would have instead:
//
//		         16 header
//		      7,710 path list
//		 26,619,905 name list
//		340,836,441 posting lists
//		  3,205,824 name index
//		 19,026,125 posting list index
//		         36 trailer
//		-----------
//		389,696,057 total
//
// Overall, the 64-bit offsets caused a 1.6% increase in size
// for a large index, but the smaller posting lists caused
// a 34.2% reduction in the size, for an overall 32.6% reduction.

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

const (
	magic          = "csearch index 1\n"
	trailerMagic32 = "\ncsearch trailr\n"
	trailerMagic64 = "\ncsearch trlr64\n"
)

// An Index implements read-only access to a trigram index.
type Index struct {
	Verbose       bool
	data          mmapData
	is64          bool
	postEntrySize int
	pathData      int
	nameData      int
	postData      int
	nameIndex     int
	postIndex     int
	numName       int
	numPost       int
}

const (
	postEntrySize32 = 3 + 4 + 4
	postEntrySize64 = 3 + 8 + 8
)

func Open(file string) *Index {
	mm := mmap(file)
	ix := &Index{data: mm}
	if len(mm.d) < len(trailerMagic32) {
		corrupt()
	}

	magic := string(mm.d[len(mm.d)-len(trailerMagic32):])
	var n int
	switch magic {
	default:
		corrupt()

	case trailerMagic32:
		n = len(mm.d) - len(trailerMagic32) - 5*4
		if n < 0 {
			corrupt()
		}
		ix.postEntrySize = postEntrySize32
		ix.pathData = ix.uint32(n)
		ix.nameData = ix.uint32(n + 4)
		ix.postData = ix.uint32(n + 8)
		ix.nameIndex = ix.uint32(n + 12)
		ix.postIndex = ix.uint32(n + 16)
		ix.numName = (ix.postIndex-ix.nameIndex)/4 - 1

	case trailerMagic64:
		ix.is64 = true
		n = len(mm.d) - len(trailerMagic64) - 5*8
		if n < 0 {
			corrupt()
		}
		ix.postEntrySize = postEntrySize64
		ix.pathData = ix.uint64(n)
		ix.nameData = ix.uint64(n + 8)
		ix.postData = ix.uint64(n + 16)
		ix.nameIndex = ix.uint64(n + 24)
		ix.postIndex = ix.uint64(n + 32)
		ix.numName = (ix.postIndex-ix.nameIndex)/8 - 1
	}
	ix.numPost = (n - ix.postIndex) / ix.postEntrySize

	return ix
}

// slice returns the slice of index data starting at the given byte offset.
// If n >= 0, the slice must have length at least n and is truncated to length n.
func (ix *Index) slice(off int, n int) []byte {
	if off < 0 {
		corrupt()
	}
	if n < 0 {
		return ix.data.d[off:]
	}
	if off+n < off || off+n > len(ix.data.d) {
		corrupt()
	}
	return ix.data.d[off : off+n]
}

// uint32 returns the uint32 value at the given offset in the index data.
func (ix *Index) uint32(off int) int {
	v := binary.BigEndian.Uint32(ix.slice(off, 4))
	if int(v) < 0 {
		corrupt()
	}
	return int(v)
}

// uint64 returns the uint64 value at the given offset in the index data.
func (ix *Index) uint64(off int) int {
	v := binary.BigEndian.Uint64(ix.slice(off, 8))
	if int(v) < 0 || uint64(int(v)) != v {
		corrupt()
	}
	return int(v)
}

// Paths returns the list of indexed paths.
func (ix *Index) Paths() []string {
	off := ix.pathData
	var x []string
	for {
		s := ix.str(off)
		if len(s) == 0 {
			break
		}
		x = append(x, string(s))
		off += len(s) + 1
	}
	return x
}

// NameBytes returns the name corresponding to the given fileid.
func (ix *Index) NameBytes(fileid int) []byte {
	var off int
	if ix.is64 {
		off = ix.uint64(ix.nameIndex + 8*fileid)
	} else {
		off = ix.uint32(ix.nameIndex + 4*fileid)
	}
	return ix.str(ix.nameData + off)
}

func (ix *Index) str(off int) []byte {
	str := ix.slice(off, -1)
	i := bytes.IndexByte(str, '\x00')
	if i < 0 {
		corrupt()
	}
	return str[:i]
}

// Name returns the name corresponding to the given fileid.
func (ix *Index) Name(fileid int) string {
	return string(ix.NameBytes(fileid))
}

// listAt returns the index list entry at the given offset.
func (ix *Index) listAt(off int) (trigram uint32, count, offset int) {
	d := ix.slice(ix.postIndex+off, ix.postEntrySize)
	trigram = uint32(d[0])<<16 | uint32(d[1])<<8 | uint32(d[2])
	if ix.is64 {
		count = int(binary.BigEndian.Uint64(d[3:]))
		offset = int(binary.BigEndian.Uint64(d[3+8:]))
	} else {
		count = int(binary.BigEndian.Uint32(d[3:]))
		offset = int(binary.BigEndian.Uint32(d[3+4:]))
	}
	if count < 0 || offset < 0 {
		corrupt()
	}
	return
}

func (ix *Index) dumpPosting() {
	for i := 0; i < ix.numPost; i++ {
		t, count, offset := ix.listAt(i * ix.postEntrySize)
		log.Printf("%#x: %d at %d", t, count, offset)
	}
}

func (ix *Index) findList(trigram uint32) (count, offset int) {
	// binary search
	d := ix.slice(ix.postIndex, ix.postEntrySize*ix.numPost)
	i := sort.Search(ix.numPost, func(i int) bool {
		i *= ix.postEntrySize
		t := uint32(d[i])<<16 | uint32(d[i+1])<<8 | uint32(d[i+2])
		return t >= trigram
	})
	if i >= ix.numPost {
		return 0, 0
	}
	t, count, offset := ix.listAt(i * ix.postEntrySize)
	if t != trigram {
		return 0, 0
	}
	return count, offset
}

type postReader struct {
	ix       *Index
	count    int
	offset   int
	fileid   int
	d        []byte
	restrict []int
}

func (r *postReader) init(ix *Index, trigram uint32, restrict []int) {
	count, offset := ix.findList(trigram)
	if count == 0 {
		return
	}
	r.ix = ix
	r.count = count
	r.offset = offset
	r.fileid = -1
	r.d = ix.slice(ix.postData+offset+3, -1)
	r.restrict = restrict
}

func (r *postReader) max() int {
	return int(r.count)
}

func (r *postReader) next() bool {
	for r.count > 0 {
		r.count--
		delta64, n := binary.Uvarint(r.d)
		delta := int(delta64)
		if n <= 0 || delta <= 0 || uint64(delta) != delta64 {
			corrupt()
		}
		r.d = r.d[n:]
		r.fileid += delta
		if r.restrict != nil {
			i := 0
			for i < len(r.restrict) && r.restrict[i] < r.fileid {
				i++
			}
			r.restrict = r.restrict[i:]
			if len(r.restrict) == 0 || r.restrict[0] != r.fileid {
				continue
			}
		}
		return true
	}
	// list should end with terminating 0 delta
	if r.d != nil && (len(r.d) == 0 || r.d[0] != 0) {
		corrupt()
	}
	r.fileid = -1
	return false
}

func (ix *Index) PostingList(trigram uint32) []int {
	return ix.postingList(trigram, nil)
}

func (ix *Index) postingList(trigram uint32, restrict []int) []int {
	var r postReader
	r.init(ix, trigram, restrict)
	x := make([]int, 0, r.max())
	for r.next() {
		x = append(x, r.fileid)
	}
	return x
}

func (ix *Index) PostingAnd(list []int, trigram uint32) []int {
	return ix.postingAnd(list, trigram, nil)
}

func (ix *Index) postingAnd(list []int, trigram uint32, restrict []int) []int {
	var r postReader
	r.init(ix, trigram, restrict)
	x := list[:0]
	i := 0
	for r.next() {
		fileid := r.fileid
		for i < len(list) && list[i] < fileid {
			i++
		}
		if i < len(list) && list[i] == fileid {
			x = append(x, fileid)
			i++
		}
	}
	return x
}

func (ix *Index) PostingOr(list []int, trigram uint32) []int {
	return ix.postingOr(list, trigram, nil)
}

func (ix *Index) postingOr(list []int, trigram uint32, restrict []int) []int {
	var r postReader
	r.init(ix, trigram, restrict)
	x := make([]int, 0, len(list)+r.max())
	i := 0
	for r.next() {
		fileid := r.fileid
		for i < len(list) && list[i] < fileid {
			x = append(x, list[i])
			i++
		}
		x = append(x, fileid)
		if i < len(list) && list[i] == fileid {
			i++
		}
	}
	x = append(x, list[i:]...)
	return x
}

func (ix *Index) PostingQuery(q *Query) []int {
	return ix.postingQuery(q, nil)
}

func (ix *Index) postingQuery(q *Query, restrict []int) (ret []int) {
	var list []int
	switch q.Op {
	case QNone:
		// nothing
	case QAll:
		if restrict != nil {
			return restrict
		}
		list = make([]int, ix.numName)
		for i := range list {
			list[i] = i
		}
		return list
	case QAnd:
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])
			if list == nil {
				list = ix.postingList(tri, restrict)
			} else {
				list = ix.postingAnd(list, tri, restrict)
			}
			if len(list) == 0 {
				return nil
			}
		}
		for _, sub := range q.Sub {
			if list == nil {
				list = restrict
			}
			list = ix.postingQuery(sub, list)
			if len(list) == 0 {
				return nil
			}
		}
	case QOr:
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])
			if list == nil {
				list = ix.postingList(tri, restrict)
			} else {
				list = ix.postingOr(list, tri, restrict)
			}
		}
		for _, sub := range q.Sub {
			list1 := ix.postingQuery(sub, restrict)
			list = mergeOr(list, list1)
		}
	}
	return list
}

func mergeOr(l1, l2 []int) []int {
	var l []int
	i := 0
	j := 0
	for i < len(l1) || j < len(l2) {
		switch {
		case j == len(l2) || (i < len(l1) && l1[i] < l2[j]):
			l = append(l, l1[i])
			i++
		case i == len(l1) || (j < len(l2) && l1[i] > l2[j]):
			l = append(l, l2[j])
			j++
		case l1[i] == l2[j]:
			l = append(l, l1[i])
			i++
			j++
		}
	}
	return l
}

func corrupt() {
	// TODO ix.corrupt with right file name
	log.Fatal("corrupt index: remove " + File())
}

// An mmapData is mmap'ed read-only data from a file.
type mmapData struct {
	f *os.File
	d []byte
}

// mmap maps the given file into memory.
func mmap(file string) mmapData {
	f, err := os.Open(file)
	if err != nil {
		log.Fatal(err)
	}
	return mmapFile(f)
}

// File returns the name of the index file to use.
// It is either $CSEARCHINDEX or $HOME/.csearchindex.
func File() string {
	f := os.Getenv("CSEARCHINDEX")
	if f != "" {
		return f
	}
	var home string
	home = os.Getenv("HOME")
	if runtime.GOOS == "windows" && home == "" {
		home = os.Getenv("USERPROFILE")
	}
	return filepath.Clean(home + "/.csearchindex")
}
