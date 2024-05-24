// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"os"
	"slices"
	"testing"
)

var mergePaths1 = []string{
	"/a",
	"/b",
	"/c",
}

var mergePaths2 = []string{
	"/b",
	"/cc",
}

var mergeFiles1 = map[string]string{
	"/a/x":  "hello world",
	"/a/y":  "goodbye world",
	"/b/xx": "now is the time",
	"/b/xy": "for all good men",
	"/c/ab": "give me all the potatoes",
	"/c/de": "or give me death now",
}

var mergeFiles2 = map[string]string{
	"/b/www": "world wide indeed",
	"/b/xx":  "no, not now",
	"/b/yy":  "first potatoes, now liberty?",
	"/cc":    "come to the aid of his potatoes",
}

func TestMerge(t *testing.T) {
	f1, _ := os.CreateTemp("", "index-test")
	f2, _ := os.CreateTemp("", "index-test")
	f3, _ := os.CreateTemp("", "index-test")
	defer os.Remove(f1.Name())
	defer os.Remove(f2.Name())
	defer os.Remove(f3.Name())

	out1 := f1.Name()
	out2 := f2.Name()
	out3 := f3.Name()

	writeVersion = 2
	buildIndex(out1, mergePaths1, mergeFiles1)
	writeVersion = 1
	buildIndex(out2, mergePaths2, mergeFiles2)

	Merge(out3, out1, out2)

	ix1 := Open(out1)
	ix2 := Open(out2)
	ix3 := Open(out3)

	checkFiles(t, ix1, "/a/x", "/a/y", "/b/xx", "/b/xy", "/c/ab", "/c/de")
	checkFiles(t, ix2, "/b/www", "/b/xx", "/b/yy", "/cc")
	checkFiles(t, ix3, "/a/x", "/a/y", "/b/www", "/b/xx", "/b/yy", "/c/ab", "/c/de", "/cc")

	checkPosting(t, ix1, "wor", 0, 1)
	checkPosting(t, ix1, "now", 2, 5)
	checkPosting(t, ix1, "all", 3, 4)

	checkPosting(t, ix2, "now", 1, 2)

	checkPosting(t, ix3, "all", 5)
	checkPosting(t, ix3, "wor", 0, 1, 2)
	checkPosting(t, ix3, "now", 3, 4, 6)
	checkPosting(t, ix3, "pot", 4, 5, 7)
}

func checkFiles(t *testing.T, ix *Index, l ...string) {
	t.Helper()
	for i, s := range l {
		if n := ix.Name(i).String(); n != s {
			t.Fatalf("Name(%d) = %s, want %s", i, n, s)
		}
	}
}

func checkPosting(t *testing.T, ix *Index, trig string, l ...int) {
	t.Helper()
	l1 := ix.PostingList(tri(trig))
	if !slices.Equal(l1, l) {
		t.Errorf("PostingList(%q) = %v, want %v", trig, l1, l)
	}
}
