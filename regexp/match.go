// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package regexp

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"html"
	"io"
	"os"
	"regexp/syntax"
	"sort"
	"strconv"
	"strings"

	"github.com/google/codesearch/sparse"
)

// A matcher holds the state for running regular expression search.
type matcher struct {
	prog      *syntax.Prog       // compiled program
	dstate    map[string]*dstate // dstate cache
	start     *dstate            // start state
	startLine *dstate            // start state for beginning of line
	z1, z2    nstate             // two temporary nstates
}

// An nstate corresponds to an NFA state.
type nstate struct {
	q       sparse.Set // queue of program instructions
	partial rune       // partially decoded rune (TODO)
	flag    flags      // flags (TODO)
}

// The flags record state about a position between bytes in the text.
type flags uint32

const (
	flagBOL  flags = 1 << iota // beginning of line
	flagEOL                    // end of line
	flagBOT                    // beginning of text
	flagEOT                    // end of text
	flagWord                   // last byte was word byte
)

// A dstate corresponds to a DFA state.
type dstate struct {
	next     [256]*dstate // next state, per byte
	enc      string       // encoded nstate
	matchNL  bool         // match when next byte is \n
	matchEOT bool         // match in this state at end of text
}

func (z *nstate) String() string {
	return fmt.Sprintf("%v/%#x+%#x", z.q.Dense(), z.flag, z.partial)
}

// enc encodes z as a string.
func (z *nstate) enc() string {
	var buf []byte
	var v [10]byte
	last := ^uint32(0)
	n := binary.PutUvarint(v[:], uint64(z.partial))
	buf = append(buf, v[:n]...)
	n = binary.PutUvarint(v[:], uint64(z.flag))
	buf = append(buf, v[:n]...)
	dense := z.q.Dense()
	ids := make([]int, 0, len(dense))
	for _, id := range z.q.Dense() {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, id := range ids {
		n := binary.PutUvarint(v[:], uint64(uint32(id)-last))
		buf = append(buf, v[:n]...)
		last = uint32(id)
	}
	return string(buf)
}

// dec decodes the encoding s into z.
func (z *nstate) dec(s string) {
	b := []byte(s)
	i, n := binary.Uvarint(b)
	if n <= 0 {
		bug()
	}
	b = b[n:]
	z.partial = rune(i)
	i, n = binary.Uvarint(b)
	if n <= 0 {
		bug()
	}
	b = b[n:]
	z.flag = flags(i)
	z.q.Reset()
	last := ^uint32(0)
	for len(b) > 0 {
		i, n = binary.Uvarint(b)
		if n <= 0 {
			bug()
		}
		b = b[n:]
		last += uint32(i)
		z.q.Add(last)
	}
}

// dmatch is the state we're in when we've seen a match and are just
// waiting for the end of the line.
var dmatch = dstate{
	matchNL:  true,
	matchEOT: true,
}

func init() {
	var z nstate
	dmatch.enc = z.enc()
	for i := range dmatch.next {
		if i != '\n' {
			dmatch.next[i] = &dmatch
		}
	}
}

// init initializes the matcher.
func (m *matcher) init(prog *syntax.Prog) error {
	m.prog = prog
	m.dstate = make(map[string]*dstate)

	m.z1.q.Init(uint32(len(prog.Inst)))
	m.z2.q.Init(uint32(len(prog.Inst)))

	m.addq(&m.z1.q, uint32(prog.Start), syntax.EmptyBeginLine|syntax.EmptyBeginText)
	m.z1.flag = flagBOL | flagBOT
	m.start = m.cache(&m.z1)

	m.z1.q.Reset()
	m.addq(&m.z1.q, uint32(prog.Start), syntax.EmptyBeginLine)
	m.z1.flag = flagBOL
	m.startLine = m.cache(&m.z1)

	return nil
}

// stepEmpty steps runq to nextq expanding according to flag.
func (m *matcher) stepEmpty(runq, nextq *sparse.Set, flag syntax.EmptyOp) {
	nextq.Reset()
	for _, id := range runq.Dense() {
		m.addq(nextq, id, flag)
	}
}

// stepByte steps runq to nextq consuming c and then expanding according to flag.
// It returns true if a match ends immediately before c.
// c is either an input byte or endText.
func (m *matcher) stepByte(runq, nextq *sparse.Set, c int, flag syntax.EmptyOp) (match bool) {
	nextq.Reset()
	m.addq(nextq, uint32(m.prog.Start), flag)
	for _, id := range runq.Dense() {
		i := &m.prog.Inst[id]
		switch i.Op {
		default:
			continue
		case syntax.InstMatch:
			match = true
			continue
		case instByteRange:
			if c == endText {
				break
			}
			lo := int((i.Arg >> 8) & 0xFF)
			hi := int(i.Arg & 0xFF)
			ch := c
			if i.Arg&argFold != 0 && 'a' <= ch && ch <= 'z' {
				ch += 'A' - 'a'
			}
			if lo <= ch && ch <= hi {
				m.addq(nextq, i.Out, flag)
			}
		}
	}
	return
}

// addq adds id to the queue, expanding according to flag.
func (m *matcher) addq(q *sparse.Set, id uint32, flag syntax.EmptyOp) {
	if q.Has(id) {
		return
	}
	q.Add(id)
	i := &m.prog.Inst[id]
	switch i.Op {
	case syntax.InstCapture, syntax.InstNop:
		m.addq(q, i.Out, flag)
	case syntax.InstAlt, syntax.InstAltMatch:
		m.addq(q, i.Out, flag)
		m.addq(q, i.Arg, flag)
	case syntax.InstEmptyWidth:
		if syntax.EmptyOp(i.Arg)&^flag == 0 {
			m.addq(q, i.Out, flag)
		}
	}
}

const endText = -1

// computeNext computes the next DFA state if we're in d reading c (an input byte or endText).
func (m *matcher) computeNext(d *dstate, c int) *dstate {
	this, next := &m.z1, &m.z2
	this.dec(d.enc)

	// compute flags in effect before c
	flag := syntax.EmptyOp(0)
	if this.flag&flagBOL != 0 {
		flag |= syntax.EmptyBeginLine
	}
	if this.flag&flagBOT != 0 {
		flag |= syntax.EmptyBeginText
	}
	if this.flag&flagWord != 0 {
		if !isWordByte(c) {
			flag |= syntax.EmptyWordBoundary
		} else {
			flag |= syntax.EmptyNoWordBoundary
		}
	} else {
		if isWordByte(c) {
			flag |= syntax.EmptyWordBoundary
		} else {
			flag |= syntax.EmptyNoWordBoundary
		}
	}
	if c == '\n' {
		flag |= syntax.EmptyEndLine
	}
	if c == endText {
		flag |= syntax.EmptyEndLine | syntax.EmptyEndText
	}

	// re-expand queue using new flags.
	// TODO: only do this when it matters
	// (something is gating on word boundaries).
	m.stepEmpty(&this.q, &next.q, flag)
	this, next = next, this

	// now compute flags after c.
	flag = 0
	next.flag = 0
	if c == '\n' {
		flag |= syntax.EmptyBeginLine
		next.flag |= flagBOL
	}
	if isWordByte(c) {
		next.flag |= flagWord
	}

	// re-add start, process rune + expand according to flags.
	if m.stepByte(&this.q, &next.q, c, flag) {
		return &dmatch
	}
	return m.cache(next)
}

func (m *matcher) cache(z *nstate) *dstate {
	enc := z.enc()
	d := m.dstate[enc]
	if d != nil {
		return d
	}

	d = &dstate{enc: enc}
	m.dstate[enc] = d
	d.matchNL = m.computeNext(d, '\n') == &dmatch
	d.matchEOT = m.computeNext(d, endText) == &dmatch
	return d
}

func (m *matcher) match(b []byte, beginText, endText bool) (end int) {
	//	fmt.Printf("%v\n", m.prog)

	d := m.startLine
	if beginText {
		d = m.start
	}
	//	m.z1.dec(d.enc)
	//	fmt.Printf("%v (%v)\n", &m.z1, d==&dmatch)
	for i, c := range b {
		d1 := d.next[c]
		if d1 == nil {
			if c == '\n' {
				if d.matchNL {
					return i
				}
				d1 = m.startLine
			} else {
				d1 = m.computeNext(d, int(c))
			}
			d.next[c] = d1
		}
		d = d1
		//		m.z1.dec(d.enc)
		//		fmt.Printf("%#U: %v (%v, %v, %v)\n", c, &m.z1, d==&dmatch, d.matchNL, d.matchEOT)
	}
	if d.matchNL || endText && d.matchEOT {
		return len(b)
	}
	return -1
}

func (m *matcher) matchString(b string, beginText, endText bool) (end int) {
	d := m.startLine
	if beginText {
		d = m.start
	}
	for i := 0; i < len(b); i++ {
		c := b[i]
		d1 := d.next[c]
		if d1 == nil {
			if c == '\n' {
				if d.matchNL {
					return i
				}
				d1 = m.startLine
			} else {
				d1 = m.computeNext(d, int(c))
			}
			d.next[c] = d1
		}
		d = d1
	}
	if d.matchNL || endText && d.matchEOT {
		return len(b)
	}
	return -1
}

// isWordByte reports whether the byte c is a word character: ASCII only.
// This is used to implement \b and \B.  This is not right for Unicode, but:
//   - it's hard to get right in a byte-at-a-time matching world
//     (the DFA has only one-byte lookahead)
//   - this crude approximation is the same one PCRE uses
func isWordByte(c int) bool {
	return 'A' <= c && c <= 'Z' ||
		'a' <= c && c <= 'z' ||
		'0' <= c && c <= '9' ||
		c == '_'
}

// TODO:
type Grep struct {
	Regexp *Regexp   // regexp to search for
	Stdout io.Writer // output target
	Stderr io.Writer // error target

	L bool // L flag - print file names only
	C bool // C flag - print count of matches
	N bool // N flag - print line numbers
	H bool // H flag - do not print file names
	V bool // V flag - print non-matching lines (only for cgrep, not csearch)

	HTML    bool // emit HTML output for csweb
	Match   bool // were any matches found?
	Matches int  // how many matches were found?
	Limit   int  // stop after this many matches
	Limited bool // stopped because of limit

	PreContext  int // number of lines to print after
	PostContext int // number of lines to print before

	buf []byte
}

func (g *Grep) AddFlags() {
	flag.BoolVar(&g.L, "l", false, "list matching files only")
	flag.BoolVar(&g.C, "c", false, "print match counts only")
	flag.BoolVar(&g.N, "n", false, "show line numbers")
	flag.BoolVar(&g.H, "h", false, "omit file names")
	flag.IntVar(&g.PreContext, "B", 0, "show `n` lines before match")
	flag.IntVar(&g.PostContext, "A", 0, "show `n` lines after match")
	flag.Func("C", "show `n` lines before and after match", func(s string) error {
		n, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		g.PreContext = n
		g.PostContext = n
		return nil
	})
}

func (g *Grep) AddVFlag() {
	flag.BoolVar(&g.V, "v", false, "show non-matching lines")
}

func (g *Grep) esc(s string) string {
	if g.HTML {
		return html.EscapeString(s)
	}
	return s
}

func (g *Grep) File(name string) {
	f, err := os.Open(name)
	if err != nil {
		fmt.Fprintf(g.Stderr, "%s\n", g.esc(err.Error()))
		return
	}
	defer f.Close()
	g.Reader(f, name)
}

var nl = []byte{'\n'}

func countNL(b []byte) int {
	n := 0
	for {
		i := bytes.IndexByte(b, '\n')
		if i < 0 {
			break
		}
		n++
		b = b[i+1:]
	}
	return n
}

func (g *Grep) Reader(r io.Reader, name string) {
	if g.buf == nil {
		g.buf = make([]byte, 1<<20)
	}
	var (
		buf        = g.buf[:0]
		needLineno = g.N || g.HTML
		lineno     = 1
		count      = 0
		prefix     = ""
		beginText  = true
		endText    = false
	)
	if !g.H {
		prefix = name + ":"
	}
	chunkStart := 0
	for {
		n, err := io.ReadFull(r, buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		end := len(buf)
		if err == nil {
			// Stop scan before trailing fragment of a line;
			// also stop before g.PostContext whole lines,
			// so we know we'll have the context we need to print.
			d := lineSuffixLen(buf, g.PostContext+1)
			if d < len(buf) {
				end = len(buf) - d
			}
		} else {
			endText = true
		}
		for chunkStart < end {
			m1 := g.Regexp.Match(buf[chunkStart:end], beginText, endText) + chunkStart
			beginText = false
			if m1 < chunkStart {
				break
			}
			g.Match = true
			if g.Limit > 0 && g.Matches >= g.Limit {
				g.Limited = true
				return
			}
			g.Matches++
			if g.L {
				if g.HTML {
					fmt.Fprintf(g.Stdout, "<a href=\"show/%s\">%s</a>\n", g.esc(name), g.esc(name))
				} else {
					fmt.Fprintf(g.Stdout, "%s\n", name)
				}
				return
			}
			lineStart := bytes.LastIndex(buf[chunkStart:m1], nl) + 1 + chunkStart
			lineEnd := m1 + 1
			if lineEnd > end {
				lineEnd = end
			}
			if needLineno {
				lineno += countNL(buf[chunkStart:lineStart])
			}
			line := buf[lineStart:lineEnd]
			nl := ""
			if len(line) == 0 || line[len(line)-1] != '\n' {
				nl = "\n"
			}
			switch {
			case g.C:
				count++
			case g.PreContext+g.PostContext > 0:
				fmt.Fprintf(g.Stdout, "%s%d:\n", prefix, lineno)
				before, match, after := lineContext(g.PreContext, g.PostContext, buf, lineStart, lineEnd)
				for _, line := range before {
					fmt.Fprintf(g.Stdout, "\t\t%s\n", line)
				}
				fmt.Fprintf(g.Stdout, "\t>>\t%s\n", match)
				for _, line := range after {
					fmt.Fprintf(g.Stdout, "\t\t%s\n", line)
				}
			case g.HTML:
				fmt.Fprintf(g.Stdout, "<a href=\"/show/%s?q=%s#L%d\">%s:%d</a>:%s%s", g.esc(strings.ReplaceAll(name, "#", ">")), g.esc(g.Regexp.String()), lineno, g.esc(name), lineno, g.esc(string(line)), nl)
			case g.N:
				fmt.Fprintf(g.Stdout, "%s%d:%s%s", prefix, lineno, line, nl)
			default:
				fmt.Fprintf(g.Stdout, "%s%s%s", prefix, line, nl)
			}
			if needLineno {
				lineno++
			}
			chunkStart = lineEnd
		}
		if needLineno && err == nil {
			lineno += countNL(buf[chunkStart:end])
		}
		// Slide pre-context and unprocessed bytes down to start of buffer.
		d := lineSuffixLen(buf[:end], g.PreContext)
		if d == end {
			// Not enough room; give up on context.
			d = 0
		}
		n = copy(buf, buf[end-d:])
		buf = buf[:n]
		chunkStart = d
		if endText && err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				fmt.Fprintf(g.Stderr, "%s: %v\n", g.esc(name), err)
			}
			break
		}
	}
	if g.C && count > 0 {
		if g.HTML {
			fmt.Fprintf(g.Stdout, "<a href=\"show/%s?q=%s\">%s</a>: %d\n", g.esc(name), g.esc(g.Regexp.String()), g.esc(name), count)
		} else {
			fmt.Fprintf(g.Stdout, "%s: %d\n", name, count)
		}
	}
}

func lineSuffixLen(buf []byte, n int) int {
	end := len(buf)
	for i := 0; i < n; i++ {
		j := bytes.LastIndex(buf[:end], nl)
		if j < 0 {
			break
		}
		end = j
	}
	if j := bytes.LastIndex(buf[:end], nl); j >= 0 {
		return len(buf) - (j + 1)
	}
	return len(buf)
}

func linePrefixLen(buf []byte, lines int) int {
	start := 0
	for i := 0; i < lines; i++ {
		j := bytes.IndexByte(buf[start:], '\n')
		if j < 0 {
			return len(buf)
		}
		start += j + 1
	}
	return start
}

func lineContext(numBefore, numAfter int, buf []byte, lineStart, lineEnd int) (before [][]byte, line []byte, after [][]byte) {
	beforeChunk := buf[lineStart-lineSuffixLen(buf[:lineStart], numBefore) : lineStart]
	afterChunk := buf[lineEnd : lineEnd+linePrefixLen(buf[lineEnd:], numAfter)]

	line = chomp(buf[lineStart:lineEnd])
	before = bytes.SplitAfter(beforeChunk, nl)
	if len(before[len(before)-1]) == 0 {
		before = before[:len(before)-1]
	}
	for i := range before {
		before[i] = chomp(before[i])
	}
	after = bytes.Split(afterChunk, nl)
	if len(after[len(after)-1]) == 0 {
		after = after[:len(after)-1]
	}
	for i := range after {
		after[i] = chomp(after[i])
	}

	var prefix []byte
	prefix = updatePrefix(prefix, line)
	for _, l := range before {
		prefix = updatePrefix(prefix, l)
	}
	for _, l := range after {
		prefix = updatePrefix(prefix, l)
	}

	line = cutPrefix(line, prefix)
	for i, l := range before {
		before[i] = cutPrefix(l, prefix)
	}
	for i, l := range after {
		after[i] = cutPrefix(l, prefix)
	}
	return
}

func updatePrefix(prefix, line []byte) []byte {
	if prefix == nil {
		i := 0
		for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
			i++
		}
		return line[:i]
	}

	i := 0
	for i < len(line) && i < len(prefix) && line[i] == prefix[i] {
		i++
	}
	if i >= len(line) {
		return prefix
	}
	return prefix[:i]
}

func cutPrefix(line, prefix []byte) []byte {
	if len(prefix) > len(line) {
		return nil
	}
	return line[len(prefix):]
}

func chomp(s []byte) []byte {
	i := len(s)
	for i > 0 && (s[i-1] == ' ' || s[i-1] == '\t' || s[i-1] == '\r' || s[i-1] == '\n') {
		i--
	}
	return s[:i]
}
