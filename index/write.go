// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"io"
	"io/ioutil"
	"log"
	"os"
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

	trigram *sparse.Set // trigrams for the current file
	buf     [8]byte     // scratch buffer

	paths []string

	nameData   *bufWriter // temp file holding list of names
	nameLen    int        // number of bytes written to nameData
	nameIndex  *bufWriter // temp file holding name index
	numName    int        // number of names written
	totalBytes int64

	post      []postEntry // list of (trigram, file#) pairs
	postFile  *bufWriter  // flushed post entries
	postEnds  []int
	postIndex *bufWriter // temp file holding posting list index

	inbuf []byte     // input buffer
	main  *bufWriter // main index file
}

const npost = 64 << 20 / 8 // 64 MB worth of post entries

// Create returns a new IndexWriter that will write the index to file.
func Create(file string) *IndexWriter {
	return &IndexWriter{
		trigram:   sparse.NewSet(1 << 24),
		nameData:  bufCreate(""),
		nameIndex: bufCreate(""),
		postFile:  bufCreate(""),
		postIndex: bufCreate(""),
		main:      bufCreate(file),
		post:      make([]postEntry, 0, npost),
		inbuf:     make([]byte, 1<<20),
	}
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

// AddPaths adds the given paths to the index's list of paths.
func (ix *IndexWriter) AddPaths(paths []string) {
	ix.paths = append(ix.paths, paths...)
}

// AddFile adds the file with the given name (opened using os.Open)
// to the index.  It logs errors using package log.
func (ix *IndexWriter) AddFile(name string) {
	f, err := os.Open(name)
	if err != nil {
		log.Print(err)
		return
	}
	defer f.Close()
	ix.Add(name, f)
}

// Add adds the file f to the index under the given name.
// It logs errors using package log.
func (ix *IndexWriter) Add(name string, f io.Reader) {
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
					log.Printf("%s: %v\n", name, err)
					return
				}
				log.Printf("%s: 0-length read\n", name)
				return
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
		if !validUTF8((tv>>8)&0xFF, tv&0xFF) {
			if ix.LogSkip {
				log.Printf("%s: invalid UTF-8, ignoring\n", name)
			}
			return
		}
		if n > maxFileLen {
			if ix.LogSkip {
				log.Printf("%s: too long, ignoring\n", name)
			}
			return
		}
		if linelen++; linelen > maxLineLen {
			if ix.LogSkip {
				log.Printf("%s: very long lines, ignoring\n", name)
			}
			return
		}
		if c == '\n' {
			linelen = 0
		}
	}
	if ix.trigram.Len() > maxTextTrigrams {
		if ix.LogSkip {
			log.Printf("%s: too many trigrams, probably not text, ignoring\n", name)
		}
		return
	}
	ix.totalBytes += n

	if ix.Verbose {
		log.Printf("%d %d %s\n", n, ix.trigram.Len(), name)
	}

	fileid := ix.addName(name)
	for _, trigram := range ix.trigram.Dense() {
		if len(ix.post) >= cap(ix.post) {
			ix.flushPost()
		}
		ix.post = append(ix.post, makePostEntry(trigram, fileid))
	}
}

// Flush flushes the index entry to the target file.
func (ix *IndexWriter) Flush() {
	ix.addName("")

	var off [5]int
	ix.main.writeString(magic)
	off[0] = ix.main.offset()
	for _, p := range ix.paths {
		ix.main.writeString(p)
		ix.main.writeString("\x00")
	}
	ix.main.writeString("\x00")
	off[1] = ix.main.offset()
	copyFile(ix.main, ix.nameData)
	off[2] = ix.main.offset()
	ix.mergePost(ix.main)
	off[3] = ix.main.offset()
	copyFile(ix.main, ix.nameIndex)
	off[4] = ix.main.offset()
	copyFile(ix.main, ix.postIndex)

	// not required for reader, but nice for debugging:
	// align trailer to end at 16-byte boundary.
	if !writeOldIndex {
		pos := ix.main.offset()
		for pos%16 != 8 {
			ix.main.writeByte(0)
			pos++
		}
	}

	for _, v := range off {
		ix.main.writeUint(v)
	}
	if writeOldIndex {
		ix.main.writeString(trailerMagic32)
	} else {
		ix.main.writeString(trailerMagic64)
	}

	os.Remove(ix.nameData.name)
	os.Remove(ix.postFile.name)
	os.Remove(ix.nameIndex.name)
	os.Remove(ix.postIndex.name)

	log.Printf("%d data bytes, %d index bytes", ix.totalBytes, ix.main.offset())

	ix.main.flush()
}

func copyFile(dst, src *bufWriter) {
	dst.flush()
	n, err := io.Copy(dst.file, src.finish())
	if err != nil {
		log.Fatalf("copying %s to %s: %v", src.name, dst.name, err)
	}
	dst.fileOff += n
}

// addName adds the file with the given name to the index.
// It returns the assigned file ID number.
func (ix *IndexWriter) addName(name string) int {
	if strings.Contains(name, "\x00") {
		log.Fatalf("%q: file has NUL byte in name", name)
	}

	ix.nameIndex.writeUint(ix.nameData.offset())
	ix.nameData.writeString(name)
	ix.nameData.writeByte(0)
	id := ix.numName
	ix.numName++
	return id
}

// flushPost writes ix.post to a new temporary file and
// clears the slice.
func (ix *IndexWriter) flushPost() {
	if ix.Verbose {
		log.Printf("flush %d entries to %v", len(ix.post), ix.postFile.name)
	}
	sortPost(ix.post)

	start := ix.postFile.offset()
	var w postDataWriter
	w.init(ix.postFile, false)
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
	end := ix.postFile.offset()

	log.Printf("flushed %d bytes to disk; total %d", end-start, end)
	ix.postEnds = append(ix.postEnds, end)
}

// mergePost reads the flushed index entries and merges them
// into posting lists, writing the resulting lists to out.
func (ix *IndexWriter) mergePost(out *bufWriter) {
	var h postHeap

	log.Printf("merge %d files + mem", len(ix.postEnds))
	h.addFile(ix.postFile, ix.postEnds)
	sortPost(ix.post)
	h.addMem(ix.post)

	npost := 0
	e := h.next()
	offset0 := out.offset()
	var delta deltaWriter
	delta.init(out)
	for {
		npost++
		offset := out.offset() - offset0
		trigram := e.trigram()
		ix.buf[0] = byte(trigram >> 16)
		ix.buf[1] = byte(trigram >> 8)
		ix.buf[2] = byte(trigram)

		// posting list
		fileid := -1
		nfile := 0
		out.write(ix.buf[:3])
		for ; e.trigram() == trigram && trigram != invalidTrigram; e = h.next() {
			delta.write(e.fileid() - fileid)
			fileid = e.fileid()
			nfile++
		}
		delta.write(0)
		delta.flush()

		// index entry
		ix.postIndex.write(ix.buf[:3])
		ix.postIndex.writeUint(nfile)
		ix.postIndex.writeUint(offset)

		if trigram == 1<<24-1 {
			break
		}
	}
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

func (h *postHeap) addFile(w *bufWriter, ends []int) {
	w.flush()
	data := mmapFile(w.file).d
	start := 0
	for _, end := range ends {
		var r allPostReader
		r.init(&Index{is64: !writeOldIndex, name: w.name}, data[start:end])
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

// A bufWriter is a convenience wrapper: a closeable bufio.Writer.
type bufWriter struct {
	name    string
	file    *os.File
	fileOff int64
	buf     []byte
	tmp     [8]byte
}

// bufCreate creates a new file with the given name and returns a
// corresponding bufWriter.  If name is empty, bufCreate uses a
// temporary file.
func bufCreate(name string) *bufWriter {
	var (
		f   *os.File
		err error
	)
	if name != "" {
		f, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	} else {
		f, err = ioutil.TempFile("", "csearch")
	}
	if err != nil {
		log.Fatal(err)
	}
	return &bufWriter{
		name: f.Name(),
		buf:  make([]byte, 0, 256<<10),
		file: f,
	}
}

func (b *bufWriter) write(x []byte) {
	n := cap(b.buf) - len(b.buf)
	if len(x) > n {
		b.flush()
		if len(x) >= cap(b.buf) {
			if _, err := b.file.Write(x); err != nil {
				log.Fatalf("writing %s: %v", b.name, err)
			}
			b.fileOff += int64(len(x))
			return
		}
	}
	b.buf = append(b.buf, x...)
}

func (b *bufWriter) writeByte(x byte) {
	if len(b.buf) >= cap(b.buf) {
		b.flush()
	}
	b.buf = append(b.buf, x)
}

func (b *bufWriter) writeString(s string) {
	n := cap(b.buf) - len(b.buf)
	if len(s) > n {
		b.flush()
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

// offset returns the current write offset.
func (b *bufWriter) offset() int {
	off := b.fileOff + int64(len(b.buf))
	if int64(int(off)) != off {
		log.Fatalf("index is larger than 2GB on 32-bit system")
	}
	return int(off)
}

func (b *bufWriter) flush() {
	if len(b.buf) == 0 {
		return
	}
	_, err := b.file.Write(b.buf)
	if err != nil {
		log.Fatalf("writing %s: %v", b.name, err)
	}
	b.fileOff += int64(len(b.buf))
	b.buf = b.buf[:0]
}

// finish flushes the file to disk and returns an open file ready for reading.
func (b *bufWriter) finish() *os.File {
	b.flush()
	f := b.file
	f.Seek(0, 0)
	return f
}

func (b *bufWriter) writeTrigram(t uint32) {
	if cap(b.buf)-len(b.buf) < 3 {
		b.flush()
	}
	b.buf = append(b.buf, byte(t>>16), byte(t>>8), byte(t))
}

func (b *bufWriter) writeUint(x int) {
	if writeOldIndex {
		b.writeUint32(x)
	} else {
		b.writeUint64(x)
	}
}

func (b *bufWriter) writeUint32(x int) {
	if x < 0 || int(uint32(x)) != x {
		log.Fatalf("index is larger than 2GB on 32-bit system")
	}
	if cap(b.buf)-len(b.buf) < 4 {
		b.flush()
	}
	b.buf = append(b.buf, byte(x>>24), byte(x>>16), byte(x>>8), byte(x))
}

func (b *bufWriter) writeUint64(x int) {
	if x < 0 {
		log.Fatalf("index is too large")
	}
	if cap(b.buf)-len(b.buf) < 4 {
		b.flush()
	}
	b.buf = append(b.buf, byte(x>>56), byte(x>>48), byte(x>>40), byte(x>>32), byte(x>>24), byte(x>>16), byte(x>>8), byte(x))
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
