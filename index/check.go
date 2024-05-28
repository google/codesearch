// Copyright 2024 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package index

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
)

func (ix *Index) Check() error {
	if ix.version == 1 {
		return nil
	}

	// TODO move to Index
	old := panicOnCorrupt
	panicOnCorrupt = true
	defer func() {
		panicOnCorrupt = old
	}()

	// Read all names.
	for _ = range ix.NamesAt(0, ix.numName).All() {
	}

	// Read all posting lists blocks.
	pblocks := ix.slice(ix.postIndex, ix.numPostBlock*postBlockSize)
	pdata := ix.slice(ix.postData, ix.nameIndex-ix.postData)
	pblocks0 := pblocks
	n := 0
	for len(pblocks) > 0 {
		b := pblocks[:postBlockSize]
		pblocks = pblocks[postBlockSize:]
		offset := 0
		b0 := b
		_ = b0
		for len(b) > 3 && (b[0] != 0 || b[1] != 0 || b[2] != 0) {
			t := b[:3]
			trigram := uint32(b[0])<<16 | uint32(b[1])<<8 | uint32(b[2])
			_ = trigram
			count, l1 := binary.Uvarint(b[3:])
			if l1 <= 0 {
				ix.corrupt()
			}
			o, l2 := binary.Uvarint(b[3+l1:])
			if l2 <= 0 {
				ix.corrupt()
			}
			offset += int(o)
			b = b[3+l1+l2:]

			// Read posting list for this trigram.
			plist := pdata[offset:]
			if len(plist) < 3 || string(plist[:3]) != string(t) {
				fmt.Fprintf(os.Stderr, "BLOCK %d at %d %#x %d %d\n%s\nPLIST\n%s", n, cap(b0)-cap(t), trigram, count, offset, hex.Dump(pblocks0[:len(pblocks0)-len(pblocks)]), hex.Dump(plist[:min(256, len(plist))]))
				ix.corrupt()
			}
			var dr deltaReader
			dr.init(ix, plist[3:])
			for range count {
				d := dr.next()
				if d == 0 {
					ix.corrupt()
				}
			}
			if dr.next() != 0 {
				ix.corrupt()
			}
		}
		n++
	}
	return nil
}
