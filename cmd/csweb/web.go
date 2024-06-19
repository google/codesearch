// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"archive/zip"
	"bytes"
	"embed"
	"flag"
	"fmt"
	"html"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/codesearch/index"
	"github.com/google/codesearch/regexp"
)

var (
	verboseFlag = flag.Bool("verbose", false, "print extra information")
)

func main() {
	http.HandleFunc("/", home)
	http.Handle("/_static/", http.FileServer(http.FS(static)))
	http.HandleFunc("/show/", show)
	log.Fatal(http.ListenAndServe("localhost:2473", nil))
}

//go:embed _static
var static embed.FS

func home(w http.ResponseWriter, r *http.Request) {
	qarg := r.FormValue("q")
	w.Write([]byte(strings.ReplaceAll(homePage, "QUERY", html.EscapeString(qarg))))
	if qarg == "" {
		return
	}

	g := regexp.Grep{
		HTML:   true,
		Limit:  100000,
		Stdout: w,
		Stderr: w,
	}

	pat := "(?m)" + qarg
	re, err := regexp.Compile(pat)
	if err != nil {
		fmt.Fprintf(w, "Bad query: %v\n", err)
		return
	}
	g.Regexp = re
	var fre *regexp.Regexp
	farg := r.FormValue("f")
	if farg != "" {
		fre, err = regexp.Compile(farg)
		if err != nil {
			fmt.Fprintf(w, "Bad -f flag: %v\n", err)
			return
		}
	}
	q := index.RegexpQuery(re.Syntax)
	if *verboseFlag {
		log.Printf("query: %s\n", q)
	}

	start := time.Now()
	ix := index.Open(index.File())
	ix.Verbose = *verboseFlag
	post := ix.PostingQuery(q)
	if *verboseFlag {
		fmt.Fprintf(w, "post query identified %d possible files\n", len(post))
	}

	if fre != nil {
		fnames := make([]int, 0, len(post))

		for _, fileid := range post {
			name := ix.Name(fileid)
			if fre.MatchString(name.String(), true, true) < 0 {
				continue
			}
			fnames = append(fnames, fileid)
		}

		if *verboseFlag {
			fmt.Fprintf(w, "filename regexp matched %d files\n", len(fnames))
		}
		post = fnames
	}

	var (
		zipFile   string
		zipReader *zip.ReadCloser
		zipMap    map[string]*zip.File
	)

	for _, fileid := range post {
		if g.Limited {
			break
		}
		name := ix.Name(fileid).String()
		file, err := os.Open(name)
		if err != nil {
			if i := strings.Index(name, ".zip\x01"); i >= 0 {
				zfile, zname := name[:i+4], name[i+5:]
				if zfile != zipFile {
					if zipReader != nil {
						zipReader.Close()
						zipMap = nil
					}
					zipFile = zfile
					zipReader, err = zip.OpenReader(zfile)
					if err != nil {
						zipReader = nil
					}
					if zipReader != nil {
						zipMap = make(map[string]*zip.File)
						for _, file := range zipReader.File {
							zipMap[file.Name] = file
						}
					}
				}
				file := zipMap[zname]
				if file != nil {
					r, err := file.Open()
					if err != nil {
						continue
					}
					g.Reader(r, name)
					r.Close()
					continue
				}
			}
			continue
		}
		g.Reader(file, name)
		file.Close()
	}

	fmt.Fprintf(w, "\n%d matches in %.3fs\n", g.Matches, time.Since(start).Seconds())
	if g.Limited {
		fmt.Fprintf(w, "more matches not shown due to match limit\n")
	}
}

var homePage = `<!DOCTYPE html>
<html>
<head>
<link rel="stylesheet" type="text/css" href="_static/viewer.css" />
<body>
Code Search
<p>
<form action="/">
<input type="text" name="q" value="QUERY">
<input type="submit">
</form>
<p>
<hr>
<pre>
`

func show(w http.ResponseWriter, r *http.Request) {
	file := strings.TrimPrefix(r.URL.Path, "/show")
	if strings.HasPrefix(file, "/") && filepath.IsAbs(file[1:]) {
		// Turn /c:/foo into c:/foo on Windows.
		file = file[1:]
	}
	// TODO maybe trim file by ix.roots
	// TODO zips
	info, err := os.Stat(file)
	if err != nil {
		// TODO
		http.Error(w, err.Error(), 500)
		return
	}
	if info.IsDir() {
		dirs, err := os.ReadDir(file)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		w.Write(serveDir(file, dirs))
		return
	}

	data, err := os.ReadFile(file)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Write(serveFile(file, data))
}

func printHeader(buf *bytes.Buffer, file string) {
	e := html.EscapeString
	buf.WriteString("<!DOCTYPE html>\n<head>\n")
	buf.WriteString("<link rel=\"stylesheet\" href=\"/_static/viewer.css\">\n")
	buf.WriteString("<script src=\"/_static/viewer.js\"></script>\n")
	fmt.Fprintf(buf, `<title>%s - code search</title>`, e(file))
	buf.WriteString("\n</head><body onload=\"highlight()\"><pre>\n")
	f := ""
	for _, elem := range strings.Split(file, "/") {
		f += "/" + elem
		fmt.Fprintf(buf, `/<a href="/show%s">%s</a>`, e(f), e(elem))
	}
	fmt.Fprintf(buf, `</b> <small>(<a href="/">about</a>)</small>`)
	fmt.Fprintf(buf, "\n\n")
}

func serveDir(file string, dir []fs.DirEntry) []byte {
	var buf bytes.Buffer
	e := html.EscapeString
	printHeader(&buf, file)
	for _, d := range dir {
		// Note: file is the full path including mod@vers.
		file := path.Join(file, d.Name())
		fmt.Fprintf(&buf, "<a href=\"/show%s\">%s</a>\n", e(file), e(path.Base(file)))
	}
	return buf.Bytes()
}

var nl = []byte("\n")

func serveFile(file string, data []byte) []byte {
	if !isText(data) {
		return data
	}

	var buf bytes.Buffer
	e := html.EscapeString
	printHeader(&buf, file)
	n := 1 + bytes.Count(data, nl)
	wid := len(fmt.Sprintf("%d", n))
	wid = (wid+2+7)&^7 - 2
	n = 1
	for len(data) > 0 {
		var line []byte
		line, data, _ = bytes.Cut(data, nl)
		fmt.Fprintf(&buf, "<span id=\"L%d\">%*d  %s\n</span>", n, wid, n, e(string(line)))
		n++
	}
	return buf.Bytes()
}

// isText reports whether a significant prefix of s looks like correct UTF-8;
// that is, if it is likely that s is human-readable text.
func isText(s []byte) bool {
	const max = 1024 // at least utf8.UTFMax
	if len(s) > max {
		s = s[0:max]
	}
	for i, c := range string(s) {
		if i+utf8.UTFMax > len(s) {
			// last char may be incomplete - ignore
			break
		}
		if c == 0xFFFD || c < ' ' && c != '\n' && c != '\t' && c != '\f' {
			// decoding error or control character - not a text file
			return false
		}
	}
	return true
}
