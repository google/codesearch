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
	"sort"
	"strings"

	"../../index"  // FIXME: before merge
)

var usageMessage = `usage: cindex [-list] [-reset] [-exclude path]... [path...]

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

By default cindex adds the named paths to the index but preserves 
information about other paths that might already be indexed
(the ones printed by cindex -list).  The -reset flag causes cindex to
delete the existing index before indexing the new paths.
With no path arguments, cindex -reset removes the index.

Specify one or more -exclude parameters to do not index some files:

	cindex -exclude src/out/Debug -exclude src/out/Release src

Note that excludes preced includes.
`

func usage() {
	fmt.Fprintf(os.Stderr, usageMessage)
	os.Exit(2)
}

type pathFlag []string

var (
	listFlag    = flag.Bool("list", false, "list indexed / excluded paths and exit")
	resetFlag   = flag.Bool("reset", false, "discard existing index")
	verboseFlag = flag.Bool("verbose", false, "print extra information")
	cpuProfile  = flag.String("cpuprofile", "", "write cpu profile to this file")
	excludes    pathFlag
)

func main() {
	flag.Usage = usage
	flag.Var(&excludes, "exclude", "exclude paths")
	flag.Parse()
	args := flag.Args()

	if *listFlag {
		ix := index.Open(index.File())
		for _, arg := range ix.Paths() {
			fmt.Printf("%s\n", arg)
		}
		for _, arg := range ix.ExcludePaths() {
			fmt.Printf("- %s\n", arg)
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

	if *resetFlag && len(args) == 0 {
		os.Remove(index.File())
		return
	}
	if len(args) == 0 {
		ix := index.Open(index.File())
		for _, arg := range ix.Paths() {
			args = append(args, arg)
		}
		for _, arg := range ix.ExcludePaths() {
			fmt.Printf("exclude %s\n", arg)
			excludes = append(excludes, arg)
		}
	}

	// Translate paths to absolute paths so that we can
	// generate the file list in sorted order.
	for i, arg := range args {
		a, err := filepath.Abs(arg)
		if err != nil {
			log.Printf("%s: %s", arg, err)
			args[i] = ""
			continue
		}
		args[i] = a
	}
	sort.Strings(args)

	for len(args) > 0 && args[0] == "" {
		args = args[1:]
	}

	// Do the same with excludes.
	for i, arg := range excludes {
		a, err := filepath.Abs(arg)
		if err != nil {
			log.Printf("%s: %s", arg, err)
			excludes[i] = ""
			continue
		}
		excludes[i] = a
	}
	sort.Strings(excludes)

	for len(excludes) > 0 && excludes[0] == "" {
		excludes = excludes[1:]
	}

	master := index.File()
	if _, err := os.Stat(master); err != nil {
		// Does not exist.
		*resetFlag = true
	}
	file := master
	if !*resetFlag {
		file += "~"
	}

	ix := index.Create(file)
	ix.Verbose = *verboseFlag
	ix.AddPaths(args)
	ix.AddExcludePaths(excludes)
	for _, arg := range args {
		log.Printf("index %s", arg)
		filepath.Walk(arg, func(path string, info os.FileInfo, err error) error {
			if _, elem := filepath.Split(path); elem != "" {
				// Skip various temporary or "hidden" files or directories.
				if elem[0] == '.' || elem[0] == '#' || elem[0] == '~' || elem[len(elem)-1] == '~' {
					if info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}
			// Check if we should skip entire directory.
			if info.IsDir() {
				for _, ex := range excludes {
					if strings.HasPrefix(path, ex) {
						return filepath.SkipDir
					}
				}
			}
			if err != nil {
				log.Printf("%s: %s", path, err)
				return nil
			}
			if info != nil && info.Mode()&os.ModeType == 0 {
				include := true
				for _, ex := range excludes {
					if strings.HasPrefix(path, ex) {
						include = false
						break
					}
				}
				if include {
					ix.AddFile(path)
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
		os.Remove(file)
		os.Rename(file+"~", master)
	}
	log.Printf("done")
	return
}

func (i *pathFlag) String() string {
    return strings.Join(*i, ", ")
}

func (i *pathFlag) Set(value string) error {
    *i = append(*i, value)
    return nil
}
