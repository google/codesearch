// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"io/ioutil"
	"os"
	"testing"
)

// Test correct mmap resource clean up.
func TestMmap(t *testing.T) {
	f1, err := ioutil.TempFile("", "index-test")
	if err != nil {
		t.Errorf("%s: %s", f1.Name(), err)
	}
	f1.WriteString("123456789")
	data := mmapFile(f1)
	err = unmmapFile(&data)
	if err != nil {
		t.Errorf("%s: data %v %s", data.d, f1.Name(), err)
	}
	err = f1.Close()
	if err != nil {
		t.Errorf("%s: %s", f1.Name(), err)
	}
	err = os.Remove(f1.Name())
	if err != nil {
		t.Errorf("%s: %s", f1.Name(), err)
	}
}
