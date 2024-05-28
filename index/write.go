// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"archive/zip"
	"cmp"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/google/codesearch/sparse"
)

// Index writing.  See read.go for details of on-disk format.
//
// It would suffice to make a single large list of (trigram, file#) pairs
// while processing the files one at a time, sort that list by trigram,
// and then create the posting lists from subsequences of the list.
// However, we do not assume that the entire index fits in memory.
// Instead, we sort and flush the list to a new temporary file each time
// it reaches its maximum in-memory size, and then at the end we
// create the final posting lists by merging the temporary files as we
// read them back in.
//
// It would also be useful to be able to create an index for a subset
// of the files and then merge that index into an existing one.  This would
// allow incremental updating of an existing index when a directory changes.
// But we have not implemented that.

// An IndexWriter creates an on-disk index corresponding to a set of files.
type IndexWriter struct {
	LogSkip bool // log information about skipped files
	Verbose bool // log status using package log
	Zip     bool // index content of zip files

	trigram *sparse.Set // trigrams for the current file
	buf     [32]byte    // scratch buffer

	roots []Path

	names      *PathWriter
	nameData   *Buffer // temp file holding list of names
	nameLen    int     // number of bytes written to nameData
	nameIndex  *Buffer // temp file holding name index
	numName    int     // number of names written
	nameLast   Path    // last name in list
	totalBytes int64

	post       []postEntry // list of (trigram, file#) pairs
	postFile   *Buffer     // flushed post entries
	postEnds   []int
	postIndex  *Buffer // temp file holding posting list index
	numTrigram int

	inbuf []byte  // input buffer
	main  *Buffer // main index file
}

const npost = 64 << 20 / 8 // 64 MB worth of post entries

// Create returns a new IndexWriter that will write the index to file.
func Create(file string) *IndexWriter {
	ix := &IndexWriter{
		trigram:   sparse.NewSet(1 << 24),
		nameData:  bufCreate(""),
		nameIndex: bufCreate(""),
		postFile:  bufCreate(""),
		postIndex: bufCreate(""),
		main:      bufCreate(file),
		post:      make([]postEntry, 0, npost),
		inbuf:     make([]byte, 1<<20),
	}
	ix.names = NewPathWriter(ix.nameData, ix.nameIndex, writeVersion, nameGroupSize)
	return ix
}

// isValidName reports whether name is a valid name to store in the index.
// We reject all control characters (bytes < ' ' aka 0x20)
// because we use them for framing in the name format.
func isValidName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if name[i] < ' ' {
			return false
		}
	}
	return true
}

// A postEntry is an in-memory (trigram, file#) pair.
type postEntry uint64

const invalidTrigram = uint32(1<<24 - 1)

func (p postEntry) trigram() uint32 {
	return uint32(p >> 40)
}

func (p postEntry) fileid() int {
	id := uint64(p << 24 >> 24)
	if uint64(int(id)) != id || int(id) < 0 {
		log.Fatalf("more than 2^31 files on a 32-bit system")
	}
	return int(id)
}

func makePostEntry(trigram uint32, fileid int) postEntry {
	// Note that this encoding is known to the trigram and fileid method above,
	// but also to sortPost below.
	if fileid>>40 > 0 {
		log.Fatalf("more than 2^40 files")
	}
	return postEntry(trigram)<<40 | postEntry(fileid)
}

// Tuning constants for detecting text files.
// A file is assumed not to be text files (and thus not indexed)
// if it contains an invalid UTF-8 sequences, if it is longer than maxFileLength
// bytes, if it contains a line longer than maxLineLen bytes,
// or if it contains more than maxTextTrigrams distinct trigrams.
const (
	maxFileLen      = 1 << 30
	maxLineLen      = 2000
	maxTextTrigrams = 20000
)

// AddRoots adds the given roots to the index's list of roots.
func (ix *IndexWriter) AddRoots(roots []Path) {
	ix.roots = append(ix.roots, roots...)
}

// AddFile adds the file with the given name (opened using os.Open)
// to the index.  It logs errors using package log.
func (ix *IndexWriter) AddFile(name string) error {
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	return ix.Add(name, f)
}

// Add adds the file f to the index under the given name.
// It logs errors using package log.
func (ix *IndexWriter) Add(name string, f io.Reader) error {
	if !isValidName(name) {
		for _, f := range strings.Split(name, string(filepath.Separator)) {
			if !isValidName(f) {
				return fmt.Errorf("malformed name %q", f)
			}
		}
		return fmt.Errorf("malformed name %q", name)
	}

	if strings.HasSuffix(name, ".zip") && ix.Zip {
		f, ok := f.(interface {
			io.ReaderAt
			Stat() (os.FileInfo, error)
		})
		if !ok {
			goto NoZip
		}
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() {
			goto NoZip
		}
		r, err := zip.NewReader(f, info.Size())
		if err != nil {
			return err
		}
		files := slices.Clone(r.File)
		slices.SortFunc(files, func(x, y *zip.File) int {
			for i := 0; i < len(x.Name) && i < len(y.Name); i++ {
				if x.Name[i] == y.Name[i] {
					continue
				}
				if x.Name[i] == '/' {
					return -1
				}
				if y.Name[i] == '/' {
					return +1
				}
				return cmp.Compare(x.Name[i], y.Name[i])
			}
			return cmp.Compare(len(x.Name), len(y.Name))
		})
		for _, file := range files {
			r, err := file.Open()
			if err != nil {
				println("no3", name)

				log.Printf("%s: %v", r, err)
				continue
			}
			ix.add(name+"\x01"+file.Name, r)
			r.Close()
		}
		return err
	}

NoZip:
	return ix.add(name, f)
}

func (ix *IndexWriter) add(name string, f io.Reader) error {
	ix.trigram.Reset()
	var (
		c       = byte(0)
		i       = 0
		buf     = ix.inbuf[:0]
		tv      = uint32(0)
		n       = int64(0)
		linelen = 0
	)
	for {
		tv = (tv << 8) & (1<<24 - 1)
		if i >= len(buf) {
			n, err := f.Read(buf[:cap(buf)])
			if n == 0 {
				if err != nil {
					if err == io.EOF {
						break
					}
					return err
				}
				return fmt.Errorf("%s: 0-length read", name)
			}
			buf = buf[:n]
			i = 0
		}
		c = buf[i]
		i++
		tv |= uint32(c)
		if n++; n >= 3 {
			ix.trigram.Add(tv)
		}
		if c == 0 {
			if ix.LogSkip {
				log.Printf("%s: contains NUL, ignoring\n", name)
			}
			return nil
		}
		if !validUTF8((tv>>8)&0xFF, tv&0xFF) {
			if ix.LogSkip {
				log.Printf("%s: invalid UTF-8, ignoring\n", name)
			}
			return nil
		}
		if n > maxFileLen {
			if ix.LogSkip {
				log.Printf("%s: too long, ignoring\n", name)
			}
			return nil
		}
		if linelen++; linelen > maxLineLen {
			if ix.LogSkip {
				log.Printf("%s: very long lines, ignoring\n", name)
			}
			return nil
		}
		if c == '\n' {
			linelen = 0
		}
	}
	if ix.trigram.Len() > maxTextTrigrams {
		if ix.LogSkip {
			log.Printf("%s: too many trigrams, probably not text, ignoring\n", name)
		}
		return nil
	}
	ix.totalBytes += n

	if ix.Verbose {
		log.Printf("%d %d %s\n", n, ix.trigram.Len(), name)
	}

	fileid := ix.addName(MakePath(name))
	for _, trigram := range ix.trigram.Dense() {
		if len(ix.post) >= cap(ix.post) {
			ix.flushPost()
		}
		ix.post = append(ix.post, makePostEntry(trigram, fileid))
	}
	return nil
}

// Flush flushes the index entry to the target file.
func (ix *IndexWriter) Flush() {
	if writeVersion == 1 {
		ix.addName(Path{})
	}

	var off [8]int
	if writeVersion == 1 {
		ix.main.WriteString(magicV1)
	} else {
		ix.main.WriteString(magicV2)
	}

	// Path list.
	off[0] = ix.main.Offset()
	roots := NewPathWriter(ix.main, nil, writeVersion, 0)
	roots.Collect(slices.Values(ix.roots))
	if writeVersion == 1 {
		roots.Write(Path{})
	}
	off[1] = roots.Count()
	ix.main.Align(16)

	// Name list.
	off[2] = ix.main.Offset()
	copyFile(ix.main, ix.nameData)
	off[3] = ix.numName
	ix.main.Align(16)

	// Posting lists.
	off[4] = ix.main.Offset()
	ix.mergePost(ix.main)
	off[5] = ix.numTrigram
	ix.main.Align(16)

	// Name index.
	off[6] = ix.main.Offset()
	copyFile(ix.main, ix.nameIndex) // (numName+15)/16 entries
	ix.main.Align(16)

	// Posting index.
	off[7] = ix.main.Offset()
	copyFile(ix.main, ix.postIndex) // to end of file

	if writeVersion == 1 {
		ix.main.WriteUint(off[0])           // offset of root list
		ix.main.WriteUint(off[2])           // offset of name list
		ix.main.WriteUint(off[4])           // offset of posting lists
		ix.main.WriteUint(off[6])           // offset of name index
		ix.main.WriteUint(off[7])           // offset of posting index
		ix.main.WriteString(trailerMagicV1) // TODO rename
	} else {
		for _, v := range off {
			ix.main.WriteUint(v)
		}
		ix.main.WriteString(trailerMagicV2)
	}

	os.Remove(ix.nameData.name)
	os.Remove(ix.postFile.name)
	os.Remove(ix.nameIndex.name)
	os.Remove(ix.postIndex.name)

	log.Printf("%d data bytes, %d index bytes", ix.totalBytes, ix.main.Offset())

	ix.main.Flush()
}

func copyFile(dst, src *Buffer) {
	dst.Flush()
	n, err := io.Copy(dst.file, src.finish())
	if err != nil {
		log.Fatalf("copying %s to %s: %v", src.name, dst.name, err)
	}
	dst.fileOff += n
}

// addName adds the file with the given name to the index.
// It returns the assigned file ID number.
func (ix *IndexWriter) addName(name Path) int {
	if writeVersion == 2 {
		if name.String() == "" {
			log.Fatalf("index of empty name")
		}
		if name.Compare(ix.nameLast) <= 0 {
			log.Fatalf("names not sorted: %q <= %q", name, ix.nameLast)
		}
	}

	id := ix.numName
	ix.numName++
	ix.names.Write(Path(name))
	return id
}

// flushPost writes ix.post to a new temporary file and
// clears the slice.
func (ix *IndexWriter) flushPost() {
	if ix.Verbose {
		log.Printf("flush %d entries to %v", len(ix.post), ix.postFile.name)
	}
	sortPost(ix.post)

	start := ix.postFile.Offset()
	var w postDataWriter
	w.init(ix.postFile, nil)
	trigram := invalidTrigram
	for _, p := range ix.post {
		if t := p.trigram(); t != trigram {
			if trigram != invalidTrigram {
				w.endTrigram()
			}
			w.trigram(t)
			trigram = t
		}
		w.fileid(p.fileid())
	}
	if trigram != invalidTrigram {
		w.endTrigram()
	}
	ix.post = ix.post[:0]
	end := ix.postFile.Offset()

	if ix.Verbose {
		log.Printf("flushed %d bytes to disk; total %d", end-start, end)
	}
	ix.postEnds = append(ix.postEnds, end)
}

// mergePost reads the flushed index entries and merges them
// into posting lists, writing the resulting lists to out.
func (ix *IndexWriter) mergePost(out *Buffer) {
	var h postHeap

	if len(ix.postEnds) > 0 {
		log.Printf("merge mem + %d MB disk", ix.postEnds[len(ix.postEnds)-1]>>20)
		h.addFile(ix.postFile, ix.postEnds)
	}
	sortPost(ix.post)
	h.addMem(ix.post)

	var w postDataWriter
	w.init(out, ix.postIndex)

	e := h.next()
	for {
		t := e.trigram()
		w.trigram(t)
		for ; e.trigram() == t && t != invalidTrigram; e = h.next() {
			w.fileid(e.fileid())
		}
		w.endTrigram()
		if t == invalidTrigram {
			break
		}
	}
	w.flush()
	ix.numTrigram = w.numTrigram
}

// A postChunk represents a chunk of post entries flushed to disk or
// still in memory.
type postChunk struct {
	e    postEntry                // first entry
	next func() (postEntry, bool) // reader for entries after first
}

const postBuf = 4096

// A postHeap is a heap (priority queue) of postChunks.
type postHeap struct {
	ch []*postChunk
}

func (h *postHeap) addFile(w *Buffer, ends []int) {
	w.Flush()
	data := mmapFile(w.file).d
	start := 0
	for _, end := range ends {
		var r allPostReader
		r.init(&Index{version: writeVersion, name: w.name}, data[start:end])
		h.add(r.next)
		start = end
	}
}

func (h *postHeap) addMem(x []postEntry) {
	h.add(func() (postEntry, bool) {
		if len(x) == 0 {
			return postEntry(0), false
		}
		e := x[0]
		x = x[1:]
		return e, true
	})
}

// step reads the next entry from ch and saves it in ch.e.
// It returns false if ch is over.
func (h *postHeap) step(ch *postChunk) bool {
	old := ch.e
	e, ok := ch.next()
	if !ok {
		return false
	}
	ch.e = e
	if old >= ch.e {
		panic("bad sort")
	}
	return true
}

// add adds the chunk to the postHeap.
// All adds must be called before the first call to next.
func (h *postHeap) add(next func() (postEntry, bool)) {
	e, ok := next()
	if !ok {
		return
	}
	h.push(&postChunk{e, next})
}

// empty reports whether the postHeap is empty.
func (h *postHeap) empty() bool {
	return len(h.ch) == 0
}

// next returns the next entry from the postHeap.
// It returns a postEntry with trigram == 1<<24 - 1 if h is empty.
func (h *postHeap) next() postEntry {
	if len(h.ch) == 0 {
		return makePostEntry(1<<24-1, 0)
	}
	ch := h.ch[0]
	e := ch.e
	e1, ok := ch.next()
	if !ok {
		h.pop()
	} else {
		ch.e = e1
		h.siftDown(0)
	}
	return e
}

func (h *postHeap) pop() *postChunk {
	ch := h.ch[0]
	n := len(h.ch) - 1
	h.ch[0] = h.ch[n]
	h.ch = h.ch[:n]
	if n > 1 {
		h.siftDown(0)
	}
	return ch
}

func (h *postHeap) push(ch *postChunk) {
	n := len(h.ch)
	h.ch = append(h.ch, ch)
	if len(h.ch) >= 2 {
		h.siftUp(n)
	}
}

func (h *postHeap) siftDown(i int) {
	ch := h.ch
	for {
		j1 := 2*i + 1
		if j1 >= len(ch) {
			break
		}
		j := j1
		if j2 := j1 + 1; j2 < len(ch) && ch[j1].e >= ch[j2].e {
			j = j2
		}
		if ch[i].e < ch[j].e {
			break
		}
		ch[i], ch[j] = ch[j], ch[i]
		i = j
	}
}

func (h *postHeap) siftUp(j int) {
	ch := h.ch
	for {
		i := (j - 1) / 2
		if i == j || ch[i].e < ch[j].e {
			break
		}
		ch[i], ch[j] = ch[j], ch[i]
		j = i
	}
}

// A Buffer is a convenience wrapper: a closeable bufio.Writer.
type Buffer struct {
	name    string
	file    *os.File
	fileOff int64
	buf     []byte
	tmp     [8]byte
}

// bufCreate creates a new file with the given name and returns a
// corresponding Buffer.  If name is empty, bufCreate uses a
// temporary file.
func bufCreate(name string) *Buffer {
	var (
		f   *os.File
		err error
	)
	if name != "" {
		f, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	} else {
		f, err = os.CreateTemp("", "csearch")
	}
	if err != nil {
		log.Fatal(err)
	}
	return &Buffer{
		name: f.Name(),
		buf:  make([]byte, 0, 256<<10),
		file: f,
	}
}

func (b *Buffer) Write(x []byte) {
	n := cap(b.buf) - len(b.buf)
	if len(x) > n {
		b.Flush()
		if b.file != nil && len(x) >= cap(b.buf) {
			if _, err := b.file.Write(x); err != nil {
				log.Fatalf("writing %s: %v", b.name, err)
			}
			b.fileOff += int64(len(x))
			return
		}
	}
	b.buf = append(b.buf, x...)
}

func (b *Buffer) WriteByte(x byte) error {
	if len(b.buf) >= cap(b.buf) {
		b.Flush()
	}
	b.buf = append(b.buf, x)
	return nil
}

func (b *Buffer) WriteString(s string) {
	n := cap(b.buf) - len(b.buf)
	if len(s) > n {
		b.Flush()
		if len(s) >= cap(b.buf) {
			if _, err := b.file.WriteString(s); err != nil {
				log.Fatalf("writing %s: %v", b.name, err)
			}
			b.fileOff += int64(len(s))
			return
		}
	}
	b.buf = append(b.buf, s...)
}

// Offset returns the current write offset.
func (b *Buffer) Offset() int {
	off := b.fileOff + int64(len(b.buf))
	if int64(int(off)) != off {
		log.Fatalf("index is larger than 2GB on 32-bit system")
	}
	return int(off)
}

func (b *Buffer) Flush() {
	if len(b.buf) == 0 || b.file == nil {
		return
	}
	n, err := b.file.Write(b.buf)
	if err != nil {
		log.Fatalf("writing %s: %v", b.name, err)
	}
	if n != len(b.buf) {
		log.Fatalf("writing %s: unexpected short write", b.name)
	}
	b.fileOff += int64(len(b.buf))
	b.buf = b.buf[:0]
}

// finish flushes the file to disk and returns an open file ready for reading.
func (b *Buffer) finish() *os.File {
	b.Flush()
	f := b.file
	f.Seek(0, 0)
	return f
}

func (b *Buffer) WriteTrigram(t uint32) {
	if cap(b.buf)-len(b.buf) < 3 {
		b.Flush()
	}
	b.buf = append(b.buf, byte(t>>16), byte(t>>8), byte(t))
}

func (b *Buffer) WriteVarint(x int) {
	if x < 0 {
		log.Fatalf("writeUvarint of negative number")
	}
	if cap(b.buf)-len(b.buf) < binary.MaxVarintLen64 {
		b.Flush()
	}
	b.buf = binary.AppendUvarint(b.buf, uint64(x))
}

func (b *Buffer) WriteUint(x int) {
	if writeVersion == 1 {
		b.writeUint32(x)
	} else {
		b.writeUint64(x)
	}
}

func (b *Buffer) writeUint32(x int) {
	if x < 0 || int(uint32(x)) != x {
		log.Fatalf("index is larger than 2GB on 32-bit system")
	}
	if cap(b.buf)-len(b.buf) < 4 {
		b.Flush()
	}
	b.buf = append(b.buf, byte(x>>24), byte(x>>16), byte(x>>8), byte(x))
}

func (b *Buffer) writeUint64(x int) {
	if x < 0 {
		log.Fatalf("index is too large")
	}
	if cap(b.buf)-len(b.buf) < 4 {
		b.Flush()
	}
	b.buf = append(b.buf, byte(x>>56), byte(x>>48), byte(x>>40), byte(x>>32), byte(x>>24), byte(x>>16), byte(x>>8), byte(x))
}

func (b *Buffer) Align(n int) {
	if writeVersion == 1 {
		return
	}
	// not required for reader, but nice for debugging:
	// align to 16-byte boundary.
	for b.Offset()%n != 0 {
		b.WriteByte(0)
	}
}

// validUTF8 reports whether the byte pair can appear in a
// valid sequence of UTF-8-encoded code points.
func validUTF8(c1, c2 uint32) bool {
	switch {
	case c1 < 0x80:
		// 1-byte, must be followed by 1-byte or first of multi-byte
		return c2 < 0x80 || 0xc0 <= c2 && c2 < 0xf8
	case c1 < 0xc0:
		// continuation byte, can be followed by nearly anything
		return c2 < 0xf8
	case c1 < 0xf8:
		// first of multi-byte, must be followed by continuation byte
		return 0x80 <= c2 && c2 < 0xc0
	}
	return false
}

// sortPost sorts the postentry list.
// The list is already sorted by fileid (bottom 40 bits)
// so there are only 24 bits to sort.
// Run two rounds of 12-bit radix sort.
const sortK = 12

var sortTmp []postEntry
var sortN [1 << sortK]int

func sortPost(post []postEntry) {
	if len(post) > len(sortTmp) {
		sortTmp = make([]postEntry, len(post))
	}
	tmp := sortTmp[:len(post)]

	const k = sortK
	for i := range sortN {
		sortN[i] = 0
	}
	for _, p := range post {
		r := uintptr(p>>40) & (1<<k - 1)
		sortN[r]++
	}
	tot := 0
	for i, count := range sortN {
		sortN[i] = tot
		tot += count
	}
	for _, p := range post {
		r := uintptr(p>>40) & (1<<k - 1)
		o := sortN[r]
		sortN[r]++
		tmp[o] = p
	}
	tmp, post = post, tmp

	for i := range sortN {
		sortN[i] = 0
	}
	for _, p := range post {
		r := uintptr(p>>(40+k)) & (1<<k - 1)
		sortN[r]++
	}
	tot = 0
	for i, count := range sortN {
		sortN[i] = tot
		tot += count
	}
	for _, p := range post {
		r := uintptr(p>>(40+k)) & (1<<k - 1)
		o := sortN[r]
		sortN[r]++
		tmp[o] = p
	}
}
