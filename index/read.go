// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

// Index Format
//
// An index stored on disk has the format:
//
//	"csearch index 2\n"
//	list of roots
//	list of names
//	list of posting lists
//	name index
//	posting list index
//	trailer
//
// The list of roots and list of names are sorted (by Path.Cmp)
// sequences of prefix-compressed paths. Each path is encoded
// as a varint number of prefix bytes to copy from the previous
// path, a varint number of suffix bytes that follow, and the
// suffix bytes. For example, the two path sequnce
// {"abcdef", "abcx"} is encoded as [0 6 abcdef 3 1 x].
//
// In the name list, every 16th name has a forced prefix
// length of 0, so that random access is possible by starting
// at one of these entries. The name index lists the offset
// of every 16th name.
//
// The list of posting lists is a sequence of posting lists.
// Each posting list has the form:
//
//	trigram [3]
//	deltas [v]...
//
// The trigram gives the 3 byte trigram that this list describes.  The
// delta list is a sequence of [γ-coded] deltas between file IDs,
// ending with a zero delta.  For example, the delta list [2,5,1,1,0]
// encodes the file ID list 1, 6, 7, 8.  The delta list [0] would
// encode the empty file ID list, but empty posting lists are usually
// not recorded at all.  The list of posting lists ends with an entry
// with trigram "\xff\xff\xff" and a delta list consisting a single zero.
// In the γ-encoding, which cannot represent 0, 0 encodes as 31,
// and all values v ≥ 31 encode as v+1.
//
// The indexes enable efficient random access to the lists.
//
// The name index is a sequence of 8-byte big-endian values listing the
// byte offset in the name list where every 16th name begins.
//
// The posting list index is a sequence of index entries describing
// each successive posting list.  Each index entry has the form:
//
//	trigram [3]
//	file count [v]
//	offset [v]
//
// The file count and offset are varint-encoded, breaking random
// access to the posting list index. To restore that, any index
// entry that would otherwise cross a 128-byte boundary is preceded
// by zeroed padding bytes up to the boundary. The overall index
// is also zero-padded to a multiple of 128 bytes.
// The offsets in each 128-byte chunk are delta-encoded starting
// from a base offset of 0.
//
// Index entries are only written for the non-empty posting lists,
// so finding the posting list for a specific trigram requires a
// binary search over the posting list index. To find an entry
// in the index for a given trigram, binary search on the 128-byte
// sections to find the 128-byte entry where it would be,
// and then linear search in the 128-byte section.
//
// In practice, the majority of the possible trigrams are never
// seen, so omitting the missing ones represents a significant
// storage savings.
//
// The trailer has the form:
//
//	offset of root list [8]
//	number of roots [8]
//	offset of name list [8]
//	number of names [8]
//	offset of posting lists [8]
//	number of posting lists [8]
//	offset of name index [8]
//	offset of posting list index [8]
//	"\ncsearch trlr 2\n"
//
// The code has never checked the index header, so version changes
// must be made by modifying the trailer.
// Old 32-bit Version
//
// An older 32-bit format had the following differences:
//
//  - The header was "csearch index 1\n".
//  - The trailer was "\ncsearch trailr\n".
//  - All the 8-byte values were 4-byte values.
//  - The root and names lists were not prefix-compressed nor
//    varint-delimited. Instead, they were as a sequence of
//    NUL-terminated paths, with a final empty path marking
//    the end of the list.
//  - The name index had an entry for every name, not every 16th name.
//  - The trailer did not contain "number of roots".
//  - The trailer did not contain "number of names".
//  - The posting list deltas were uvarint-coded instead of γ-coded.
//
// At the time of conversion, indexing Linux git at v6.9-9880-gdaa121128a2d
// with the old index format had the following file region sizes:
//
//		         16 header
//		         22 path list
//		  5,082,088 name list
//		147,703,290 posting lists
//		    337,656 name index
//		  4,636,335 posting list index
//		         36 trailer
//		-----------
//		157,759,443 total
//
// The 64-bit version of this file had instead:
//
//		         16 header
//		         32 path list
//		  1,170,592 name list
//		 85,872,880 posting lists
//		     42,208 name index
//		  2,292,480 posting list index
//		         80 trailer
//		-----------
//		 89,378,288 total (including padding)
//
// Overall, the tighter encoding in the 64-bit-friendly version
// yields a >40% reduction in index size.
//
// For an index of 1.6 TB of Go module zip files, the direct 64-bit
// extension of the v1 index used 162 GB, while the tighter encodings
// reduced the index to 84 GB:
//
//	name list:      28.6 GB   ->  4.0 GB
//	posting lists: 132.7 GB   -> 80.3 GB
//	name index:      1.1 GB   ->  0.07 GB
//	posting index:   0.050 GB ->  0.016 GB
//
// [γ-coded]: https://en.wikipedia.org/wiki/Elias_gamma_coding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"iter"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
)

const (
	magicV1        = "csearch index 1\n"
	magicV2        = "csearch index 2\n"
	trailerMagicV1 = "\ncsearch trailr\n"
	trailerMagicV2 = "\ncsearch trlr 2\n"

	postBlockSize = 256 // posting index entries are packed into 256-byte blocks
	nameGroupSize = 16  // names are prefix-compressed in groups of 16

	postIndexEntrySizeV1 = 3 + 4 + 4
)

// An Index implements read-only access to a trigram index.
type Index struct {
	Verbose      bool
	name         string
	data         mmapData
	version      int
	pathData     int
	numPath      int
	nameData     int
	postData     int
	nameIndex    int
	numName      int
	postIndex    int
	numPost      int
	numPostBlock int
}

func (ix *Index) PrintStats() {
	fmt.Printf("%d path list (%d paths)\n", ix.nameData-ix.pathData, ix.numPath)
	fmt.Printf("%d name list (%d names)\n", ix.postData-ix.nameData, ix.numName)
	fmt.Printf("%d posting lists (%d trigrams)\n", ix.nameIndex-ix.postData, ix.numPost)
	fmt.Printf("%d name index\n", ix.postIndex-ix.nameIndex)
	fmt.Printf("%d posting index\n", ix.numPostBlock*postBlockSize)
}

func Open(file string) *Index {
	mm := mmap(file)
	ix := &Index{name: file, data: mm}
	if len(mm.d) < len(trailerMagicV1) {
		ix.corrupt()
	}

	magic := string(mm.d[len(mm.d)-len(trailerMagicV1):])
	var n int
	switch magic {
	default:
		ix.corrupt()

	case trailerMagicV1:
		ix.version = 1
		n = len(mm.d) - len(trailerMagicV1) - 5*4
		if n < 0 {
			ix.corrupt()
		}
		ix.pathData = ix.uint32(n)
		ix.nameData = ix.uint32(n + 4)
		ix.postData = ix.uint32(n + 8)
		ix.nameIndex = ix.uint32(n + 12)
		ix.postIndex = ix.uint32(n + 16)
		ix.numName = (ix.postIndex-ix.nameIndex)/4 - 1
		ix.numPost = (n - ix.postIndex) / postIndexEntrySizeV1
		ix.numPath = -1

	case trailerMagicV2:
		ix.version = 2
		n = len(mm.d) - len(trailerMagicV2) - 8*8
		if n < 0 {
			ix.corrupt()
		}
		ix.pathData = ix.uint64(n)
		ix.numPath = ix.uint64(n + 1*8)
		ix.nameData = ix.uint64(n + 2*8)
		ix.numName = ix.uint64(n + 3*8)
		ix.postData = ix.uint64(n + 4*8)
		ix.numPost = ix.uint64(n + 5*8)
		ix.nameIndex = ix.uint64(n + 6*8)
		ix.postIndex = ix.uint64(n + 7*8)
		ix.numPostBlock = (n - ix.postIndex) / postBlockSize
	}

	return ix
}

// slice returns the slice of index data starting at the given byte offset.
// If n >= 0, the slice must have length at least n and is truncated to length n.
func (ix *Index) slice(off int, n int) []byte {
	if off < 0 {
		ix.corrupt()
	}
	if n < 0 {
		return ix.data.d[off:]
	}
	if off+n < off || off+n > len(ix.data.d) {
		ix.corrupt()
	}
	return ix.data.d[off : off+n]
}

// uint32 returns the uint32 value at the given offset in the index data.
func (ix *Index) uint32(off int) int {
	v := binary.BigEndian.Uint32(ix.slice(off, 4))
	if int(v) < 0 {
		ix.corrupt()
	}
	return int(v)
}

// uint64 returns the uint64 value at the given offset in the index data.
func (ix *Index) uint64(off int) int {
	v := binary.BigEndian.Uint64(ix.slice(off, 8))
	if int(v) < 0 || uint64(int(v)) != v {
		ix.corrupt()
	}
	return int(v)
}

// Roots returns the list of indexed roots.
func (ix *Index) Roots() *PathReader {
	return NewPathReader(ix.version, ix.slice(ix.pathData, ix.nameData-ix.pathData), ix.numPath)
}

// Name returns the name corresponding to the given fileid.
func (ix *Index) Name(fileid int) Path {
	return ix.NamesAt(fileid, fileid+1).Path()
}

// NameAt returns a PathReader returning the names for
// fileids in the range [min, max).
func (ix *Index) NamesAt(min, max int) *PathReader {
	if min >= ix.numName {
		return NewPathReader(1, nil, 0)
	}
	limit := max - min
	var off int
	if ix.version == 1 {
		off = ix.uint32(ix.nameIndex + min*4)
	} else {
		off = ix.uint64(ix.nameIndex + min/nameGroupSize*8)
		limit += min % nameGroupSize
	}
	names := NewPathReader(ix.version, ix.slice(ix.nameData+off, ix.postData-(ix.nameData+off)), limit)
	if ix.version == 2 {
		for range min % nameGroupSize {
			names.Next()
		}
	}
	return names
}

func (ix *Index) Names(lo, hi int) iter.Seq[Path] {
	r := ix.NamesAt(lo, hi)
	if r.Valid() {
		r.limit = hi - lo - 1
	}
	return r.All()
}

func (ix *Index) str(off int) []byte {
	str := ix.slice(off, -1)
	i := bytes.IndexByte(str, '\x00')
	if i < 0 {
		ix.corrupt()
	}
	return str[:i]
}

// listAt returns the i'th posting index list entry.
// It is only valid for version 1 indexes.
func (ix *Index) postIndexEntry(i int) (trigram uint32, count, offset int) {
	if ix.version != 1 {
		panic("postIndexEntry misuse")
	}
	d := ix.slice(ix.postIndex+i*postIndexEntrySizeV1, postIndexEntrySizeV1)
	trigram = uint32(d[0])<<16 | uint32(d[1])<<8 | uint32(d[2])
	if ix.version == 1 {
		count = int(binary.BigEndian.Uint32(d[3:]))
		offset = int(binary.BigEndian.Uint32(d[3+4:]))
	} else {
		count = int(binary.BigEndian.Uint64(d[3:]))
		offset = int(binary.BigEndian.Uint64(d[3+8:]))
	}
	if count < 0 || offset < 0 {
		ix.corrupt()
	}
	return
}

func (ix *Index) findList(trigram uint32) (count, offset int) {
	if ix.version == 2 {
		return ix.findListV2(trigram)
	}
	// binary search
	d := ix.slice(ix.postIndex, ix.numPost*postIndexEntrySizeV1)
	i := sort.Search(ix.numPost, func(i int) bool {
		i *= postIndexEntrySizeV1
		t := uint32(d[i])<<16 | uint32(d[i+1])<<8 | uint32(d[i+2])
		return t >= trigram
	})
	if i >= ix.numPost {
		return 0, 0
	}
	t, count, offset := ix.postIndexEntry(i)
	if t != trigram {
		return 0, 0
	}
	return count, offset
}

func (ix *Index) findListV2(trigram uint32) (count, offset int) {
	// binary search to find first posting block too late for trigram
	b := ix.slice(ix.postIndex, ix.numPostBlock*postBlockSize)
	i := sort.Search(ix.numPostBlock, func(i int) bool {
		i *= postBlockSize
		t := uint32(b[i])<<16 | uint32(b[i+1])<<8 | uint32(b[i+2])
		return t > trigram
	})
	if i == 0 {
		return 0, 0
	}

	// walk block to find trigram
	b = b[(i-1)*postBlockSize : i*postBlockSize]
	for len(b) >= 3 {
		t := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
		if t == 0 {
			break
		}
		count, n1 := binary.Uvarint(b[3:])
		if n1 < 0 {
			ix.corrupt()
		}
		o, n2 := binary.Uvarint(b[3+n1:])
		if n2 < 0 {
			ix.corrupt()
		}
		offset += int(o)
		if t == trigram {
			return int(count), offset
		}
		b = b[3+n1+n2:]
	}
	return 0, 0
}

type postReader struct {
	ix       *Index
	count    int
	offset   int
	fileid   int
	restrict []int
	delta    deltaReader
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
	r.delta.init(r.ix, ix.slice(ix.postData+offset+3, -1))
	r.restrict = restrict
}

func (r *postReader) max() int {
	return int(r.count)
}

func (r *postReader) next() bool {
	if r.ix == nil {
		return false
	}
	for r.count > 0 {
		r.count--
		delta := r.delta.next()
		if delta <= 0 {
			r.ix.corrupt()
		}
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
	if r.delta.next() != 0 {
		r.ix.corrupt()
	}
	r.delta.clearBits()
	r.fileid = -1
	return false
}

type allPostReader struct {
	trigram uint32
	fileid  int
	is64    bool
	delta   deltaReader
}

func (r *allPostReader) init(ix *Index, data []byte) {
	r.delta.init(ix, data)
	r.trigram = invalidTrigram
}

func (r *allPostReader) next() (postEntry, bool) {
	for {
		if r.trigram == invalidTrigram {
			d := r.delta.d
			if len(d) == 0 {
				return 0, false
			}
			if len(d) < 3 {
				log.Fatalf("internal error: invalid temporary file")
			}
			r.trigram = uint32(d[0])<<16 | uint32(d[1])<<8 | uint32(d[2])
			d = d[3:]
			r.fileid = -1
			r.delta.d = d
		}
		delta := r.delta.next()
		if delta == 0 {
			r.delta.clearBits()
			r.trigram = invalidTrigram
			continue
		}
		r.fileid += delta
		return makePostEntry(r.trigram, r.fileid), true
	}
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

var panicOnCorrupt = false

func (ix *Index) corrupt() {
	if panicOnCorrupt {
		panic("corrupt index")
	}
	log.Fatal("corrupt index: remove " + ix.name)
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

// TODO look in parent directories for index
// TODO cindex -init

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
