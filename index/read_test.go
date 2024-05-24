// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"os"
	"slices"
	"testing"
)

var postFiles = map[string]string{
	"file0": "",
	"file1": "Google Code Search",
	"file2": "Google Code Project Hosting",
	"file3": "Google Web Search",
}

func tri(x string) uint32 {
	return uint32(x[0])<<16 | uint32(x[1])<<8 | uint32(x[2])
}

func TestTrivialPosting(t *testing.T) {
	f, _ := os.CreateTemp("", "index-test")
	defer os.Remove(f.Name())
	out := f.Name()
	buildIndex(out, nil, postFiles)
	data, _ := os.ReadFile(out)
	os.WriteFile("/tmp/out", data, 0666)
	ix := Open(out)
	if l := ix.PostingList(tri(" Co")); !slices.Equal(l, []int{1, 2}) {
		t.Errorf("PostingList( Co) = %v, want [1 3]", l)
	}
	if l := ix.PostingList(tri("Sea")); !slices.Equal(l, []int{1, 3}) {
		t.Errorf("PostingList(Sea) = %v, want [1 3]", l)
	}
	if l := ix.PostingList(tri("Goo")); !slices.Equal(l, []int{1, 2, 3}) {
		t.Errorf("PostingList(Goo) = %v, want [1 2 3]", l)
	}
	if l := ix.PostingAnd(ix.PostingList(tri("Sea")), tri("Goo")); !slices.Equal(l, []int{1, 3}) {
		t.Errorf("PostingList(Sea&Goo) = %v, want [1 3]", l)
	}
	if l := ix.PostingAnd(ix.PostingList(tri("Goo")), tri("Sea")); !slices.Equal(l, []int{1, 3}) {
		t.Errorf("PostingList(Goo&Sea) = %v, want [1 3]", l)
	}
	if l := ix.PostingOr(ix.PostingList(tri("Sea")), tri("Goo")); !slices.Equal(l, []int{1, 2, 3}) {
		t.Errorf("PostingList(Sea|Goo) = %v, want [1 2 3]", l)
	}
	if l := ix.PostingOr(ix.PostingList(tri("Goo")), tri("Sea")); !slices.Equal(l, []int{1, 2, 3}) {
		t.Errorf("PostingList(Goo|Sea) = %v, want [1 2 3]", l)
	}
}
