// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/pprof"
	"slices"

	"github.com/google/codesearch/index"
)

var usageMessage = `usage: cindex [-list] [-reset] [-zip] [path...]

Cindex prepares the trigram index for use by csearch.  The index is the
file named by $CSEARCHINDEX, or else $HOME/.csearchindex.

The simplest invocation is

	cindex path...

which adds the file or directory tree named by each path to the index.
For example:

	cindex $HOME/src /usr/include

or, equivalently:

	cindex $HOME/src
	cindex /usr/include

If cindex is invoked with no paths, it reindexes the paths that have
already been added, in case the files have changed.  Thus, 'cindex' by
itself is a useful command to run in a nightly cron job.

The -list flag causes cindex to list the paths it has indexed and exit.

The -zip flag causes cindex to index content inside ZIP files.
This feature is experimental and will almost certainly change
in the future, possibly in incompatible ways.

By default cindex adds the named paths to the index but preserves
information about other paths that might already be indexed
(the ones printed by cindex -list).  The -reset flag causes cindex to
delete the existing index before indexing the new paths.
With no path arguments, cindex -reset removes the index.
`

func usage() {
	fmt.Fprintf(os.Stderr, usageMessage)
	os.Exit(2)
}

var (
	listFlag    = flag.Bool("list", false, "list indexed paths and exit")
	resetFlag   = flag.Bool("reset", false, "discard existing index")
	verboseFlag = flag.Bool("verbose", false, "print extra information")
	cpuProfile  = flag.String("cpuprofile", "", "write cpu profile to this file")
	checkFlag   = flag.Bool("check", false, "check index is well-formatted")
	zipFlag     = flag.Bool("zip", false, "index content in zip files")
	statsFlag   = flag.Bool("stats", false, "print index size statistics")
)

func main() {
	log.SetPrefix("cindex: ")
	flag.Usage = usage
	flag.Parse()

	if *listFlag {
		ix := index.Open(index.File())
		if *checkFlag {
			if err := ix.Check(); err != nil {
				log.Fatal(err)
			}
		}
		for p := range ix.Roots().All() {
			fmt.Printf("%s\n", p)
		}
		return
	}

	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if *resetFlag && flag.NArg() == 0 {
		os.Remove(index.File())
		return
	}
	var roots []index.Path
	if flag.NArg() == 0 {
		ix := index.Open(index.File())
		roots = slices.Collect(ix.Roots().All())
	} else {
		// Translate arguments to absolute paths so that
		// we can generate the file list in sorted order.
		for _, arg := range flag.Args() {
			a, err := filepath.Abs(arg)
			if err != nil {
				log.Printf("%s: %s", arg, err)
				continue
			}
			roots = append(roots, index.MakePath(a))
		}
		slices.SortFunc(roots, index.Path.Compare)
	}

	master := index.File()
	if _, err := os.Stat(master); err != nil {
		// Does not exist.
		*resetFlag = true
	}
	file := master
	if !*resetFlag {
		file += "~"
		if *checkFlag {
			ix := index.Open(master)
			if err := ix.Check(); err != nil {
				log.Fatal(err)
			}
		}
	}

	ix := index.Create(file)
	ix.Verbose = *verboseFlag
	ix.LogSkip = true
	ix.Zip = *zipFlag
	ix.AddRoots(roots)
	for _, root := range roots {
		log.Printf("index %s", root)
		filepath.Walk(root.String(), func(path string, info os.FileInfo, err error) error {
			if _, elem := filepath.Split(path); elem != "" {
				// Skip various temporary or "hidden" files or directories.
				if elem[0] == '.' || elem[0] == '#' || elem[0] == '~' || elem[len(elem)-1] == '~' {
					if info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}
			if err != nil {
				log.Printf("%s: %s", path, err)
				return nil
			}
			if info != nil && info.Mode()&os.ModeType == 0 {
				if err := ix.AddFile(path); err != nil {
					log.Printf("%s: %s", path, err)
					return nil
				}
			}
			return nil
		})
	}
	log.Printf("flush index")
	ix.Flush()

	if !*resetFlag {
		log.Printf("merge %s %s", master, file)
		index.Merge(file+"~", master, file)
		if *checkFlag {
			ix := index.Open(file + "~")
			if err := ix.Check(); err != nil {
				log.Fatal(err)
			}
		}
		os.Remove(file)
		os.Rename(file+"~", master)
	} else {
		if *checkFlag {
			ix := index.Open(file)
			if err := ix.Check(); err != nil {
				log.Fatal(err)
			}
		}
	}

	log.Printf("done")

	if *statsFlag {
		ix := index.Open(master)
		ix.PrintStats()
	}
	return
}
