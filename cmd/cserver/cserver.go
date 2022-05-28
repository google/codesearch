// Copyright 2011 The Go Authors.  All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package main

import (
	"bufio"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	stdregexp "regexp"
	"strings"
	"strconv"

	"github.com/hakonhall/codesearch/index"
	"github.com/hakonhall/codesearch/regexp"
)

var defaultMaxHits = 100

var usageMessage = `usage: cserver [-c] [-f fileregexp] [-h] [-i] [-l] [-n] regexp

Csearch behaves like grep over all indexed files, searching for regexp,
an RE2 (nearly PCRE) regular expression.

The -c, -h, -i, -l, and -n flags are as in grep, although note that as per Go's
flag parsing convention, they cannot be combined: the option pair -i -n 
cannot be abbreviated to -in.

The -f flag restricts the search to files whose names match the RE2 regular
expression fileregexp.

Csearch relies on the existence of an up-to-date index created ahead of time.
To build or rebuild the index that csearch uses, run:

	cindex path...

where path... is a list of directories or individual files to be included in the index.
If no index exists, this command creates one.  If an index already exists, cindex
overwrites it.  Run cindex -help for more.

Csearch uses the index stored in $CSEARCHINDEX or, if that variable is unset or
empty, $HOME/.csearchindex.
`

func usage() {
	fmt.Fprintf(os.Stderr, usageMessage)
	os.Exit(2)
}

var (
	pFlag       = flag.String("p", "/home/hakon/private/src/", "remove this prefix from paths")
)

func EscapeChar(char rune) string {
	switch {
	case char == '<':
		return "&lt;"
	case char == '>':
		return "&gt;"
	case char == '&':
		return "&amp;"
	}
	return string(char)
}

func EscapeString(text string) string {
	escaped_text := ""
	for _, char := range text {
		escaped_text = escaped_text + EscapeChar(char)
	}
	return escaped_text
}

func EscapeCharForAttributeValue(char rune) string {
	switch {
	case char == '"':
		return "&quot;"
	case char == '\'':
		return "&apos;"
	}
	return string(char)
}

func EscapeForAttributeValue(text string) string {
	escaped_text := ""
	for _, char := range text {
		escaped_text = escaped_text + EscapeCharForAttributeValue(char)
	}
	return escaped_text
}

func RemovePathPrefix(path string) string {
	return strings.TrimPrefix(path, *pFlag)
}

func uri_encode(name string, value string) string {
	uri_values := url.Values{}
	uri_values.Add(name, value)
	return uri_values.Encode()
}

func query_append(query string, unescaped_param_name string,
	unescaped_param_value string,
	unescaped_param_default_value string) string {
	new_query := query
	if unescaped_param_value != unescaped_param_default_value {
		if len(new_query) > 0 {
			new_query = new_query + "&"
		}
		new_query = new_query + uri_encode(unescaped_param_name,
			unescaped_param_value)
	}
	return new_query
}

func query_append_int(query string, unescaped_param_name string,
	param_value int, param_default_value int) string {
	return query_append(query, unescaped_param_name,
		fmt.Sprintf("%d", param_value),
		fmt.Sprintf("%d", param_default_value))
}

func pretty_print_query(query string, file string, exclude_file string,
                        hit int, line int, max_hits int, ignore_case bool) string {
	uri_query := pretty_print_query3(query, file, exclude_file,
		max_hits, ignore_case)
	uri_query = query_append_int(uri_query, "h", hit, -1)
	uri_query = query_append_int(uri_query, "l", line, -1)
	return uri_query
}

func pretty_print_query2(file string, exclude_file string, max_hits int,
	ignore_case bool) string {
	return pretty_print_query3("", file, exclude_file, max_hits,
		ignore_case)
}

func pretty_print_query3(search string, file string ,
	exclude_file string, max_hits int, ignore_case bool) string {
	uri_query := ""
	uri_query = query_append(uri_query, "q", search, "")
	uri_query = query_append(uri_query, "f", file, "")
	uri_query = query_append(uri_query, "xf", exclude_file, "")
	if ignore_case {
		uri_query = query_append(uri_query, "i", "on", "")
	}
	uri_query = query_append_int(uri_query, "n", max_hits, defaultMaxHits)
	return uri_query
}

func PrintHitHeader(writer http.ResponseWriter) {
	fmt.Fprintf(writer, `
<table class="hits">
`)
}

func PrintHitFooter(writer http.ResponseWriter,
	select_hit int, num_hits_shown int, truncated bool,
	files_matched_shown, files_matched int, direction string) string {
	var truncated_string string
	if truncated {
		truncated_string = "true"
	} else {
		truncated_string = "false"
	}

	if (select_hit < 0 || select_hit > num_hits_shown) {
		select_hit = 0
	}

	fmt.Fprintf(writer, `
</table>
<script type="text/javascript">
  var num_hits = %d;
  var truncated_hits = %s;
  var selected_hit = %d;
  var direction = "%s";
  var SELECTED_FILE = -1;
  var NUM_FILES = %d;
</script>

<hr class="end-of-results"/>
`, num_hits_shown, truncated_string, select_hit, direction,
   files_matched_shown)

	if truncated {
		return fmt.Sprintf("%d matches in %d of %d files (result list truncated)",
			num_hits_shown, files_matched_shown, files_matched)
	} else {
		return fmt.Sprintf("%d matches in %d files",
			num_hits_shown, files_matched_shown)
	}
}

func PrintFileHitHeader(writer http.ResponseWriter,
	filename string,
	file_hit_id int,
	search string,
	file_filter string,
	exclude_file_filter string,
	next_hit_id int,
	max_hits int,
	ignore_case bool) {
	query := pretty_print_query3(search, file_filter, exclude_file_filter,
		max_hits, ignore_case)
	file_url := fmt.Sprintf("/file/%s?%s", filename, query)

	fmt.Fprintf(writer, `
<tr class="file-hit">
  <td class="file-hit">
    <table class="file-hit">
      <tr class="file-hit">
        <th id="file-hit-header-%d" class="file-hit-header">
          <a id="file-hit-%d" href="%s">%s</a>
        </th>
  	<script type="text/javascript">
  	  HIT_FROM_FILE.push(%d)
  	</script>
      </tr>
      <tr id="file-hit-body-%d" class="file-hit-body">
        <td class="file-hit-body">
<table class="hit">
`, file_hit_id, file_hit_id,
   EscapeForAttributeValue(file_url),
   EscapeString(filename),
   next_hit_id,
   file_hit_id)
}

func PrintFileHitFooter(writer http.ResponseWriter) {
	fmt.Fprintf(writer, `
</table>
        </td>
      </tr>
    </table>
  </td>
</tr>
`)
}

func PrintHit(writer http.ResponseWriter, query string, re *stdregexp.Regexp,
	file string, exclude_file string, path string, hit regexp.LineHit,
	line_index int, max_hits int, file_id int, ignore_case bool) {
	short_path := RemovePathPrefix(path)
	uri_query := pretty_print_query(
		query, file, exclude_file, line_index, hit.Lineno, max_hits,
			ignore_case)
	href := fmt.Sprintf("/file/%s?%s#l%d", short_path, uri_query,
		hit.Lineno - 10);

	html_path := fmt.Sprintf(`<a id="file-link-%d" href="%s">%d.</a>`,
		line_index, href, hit.Lineno)

	html_line, _ := escape_and_mark_line(hit.Line, line_index, re, href)
	
	line_hit_class := "line-hit"
	if (line_index + 1) % 2 == 0 {
		line_hit_class += " even-line"
	} else {
		line_hit_class += " odd-line"
	}

        slices := strings.Split(short_path, "/")
        org_repo := strings.Join(slices[:2], "/")
        relative_path := strings.Join(slices[2:], "/")

	fmt.Fprintf(writer, `
<tr class="hit %s">
  <td id="location-%d" class="location">%s</td>
  <td id="line-hit-%d" class="%s"><pre class="hit prettyprint">%s</pre></td>
  <script type="text/javascript">
    ORG_REPOS.push('%s');
    RELATIVE_PATHS.push('%s');
    LINENOS.push(%d);
    FILE_FROM_HIT.push(%d)
  </script>
</tr>
`, line_hit_class, line_index, html_path, line_index, line_hit_class,
   html_line, org_repo, relative_path, hit.Lineno, file_id)
}

func Search(writer http.ResponseWriter, request *http.Request, query string,
	file_filter string, select_hit int, direction string,
	exclude_file_filter string, max_hits int, ignore_case bool) string {
	pattern := "(?m)" + query
	if ignore_case {
		pattern = "(?i)" + pattern;
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		log.Print(err)
		return "Bad line regular expression"
	}

	var fre *regexp.Regexp
	if file_filter != "" {
		file_pattern := file_filter
		if ignore_case {
			file_pattern = "(?i)" + file_pattern
		}

		fre, err = regexp.Compile(file_pattern)
		if err != nil {
			log.Print(err)
			return "Bad file regular expression"
		}
		if fre == nil {
			log.Fatal("fre cannot be nil if err isn't!")
		}
	}

	var xfre *regexp.Regexp
	if exclude_file_filter != "" {
		exclude_pattern := exclude_file_filter
		if ignore_case {
			exclude_pattern = "(?i)" + exclude_pattern
		}

		xfre, err = regexp.Compile(exclude_pattern)
		if err != nil {
			log.Print(err)
			return "Bad exclude file regular expression"
		}
		if xfre == nil {
			log.Fatal("xfre cannot be nil if err isn't!")
		}
	}
	q := index.RegexpQuery(re.Syntax)

	ix := index.Open(index.File())
	ix.Verbose = false
	var post []uint32 = ix.PostingQuery(q)

	if fre != nil {
		// Retain only those files matching the file pattern.
		fnames := make([]uint32, 0, len(post))

		for _, fileid := range post {
			full_path := ix.Name(fileid)
			name := RemovePathPrefix(full_path)
			if fre.MatchString(name, true, true) < 0 {
				continue
			}
			fnames = append(fnames, fileid)
		}

		post = fnames
	}

	if xfre != nil {
		// Remove those files matching the exclude file pattern.
		fnames := make([]uint32, 0, len(post))

		for _, fileid := range post {
			full_path := ix.Name(fileid)
			name := RemovePathPrefix(full_path)
			if xfre.MatchString(name, true, true) >= 0 {
				continue
			}
			fnames = append(fnames, fileid)
		}

		post = fnames
	}

	if len(post) > 0 {
		// pattern includes e.g. (?i), which is correct even for plain
		// "regexp" package.
		stdre, err := stdregexp.Compile(pattern)
		if err != nil {
			log.Print(err)
			// Hopefully stdre is nil and everything works
		}

		truncated_hits := false
		num_hits := 0
		files_matched := 0

		PrintHitHeader(writer)
		for _, fileid := range post {
			if num_hits >= max_hits {
				truncated_hits = true
				break
			}

			name := ix.Name(fileid)
			grep := regexp.Grep{
				Regexp: re,
				Stderr: os.Stderr,
			}

			grep.File2(name)

			if len(grep.MatchedLines) > 0 {
				short_name := RemovePathPrefix(name)
				PrintFileHitHeader(writer, short_name,
					files_matched,
					query,
					file_filter,
					exclude_file_filter,
					num_hits,
					max_hits,
					ignore_case)

				for _, hit := range grep.MatchedLines {
					//if num_hits >= max_hits {
					//	truncated_hits = true
					//	break
					//}
					PrintHit(writer, query, stdre,
						file_filter,
						exclude_file_filter, name, hit,
						num_hits, max_hits,
						files_matched,
						ignore_case)
					num_hits += 1
				}

				PrintFileHitFooter(writer)

				files_matched += 1
			}
		}

		if num_hits > 0 {
			return PrintHitFooter(writer, select_hit, num_hits,
				truncated_hits,
				files_matched, len(post), direction)
		}
	}

	return ""
}

func SearchFile(writer http.ResponseWriter, request *http.Request,
	file_filter string, exclude_file_filter string, max_hits int,
	ignore_case bool) string {
	file_pattern := file_filter
	if ignore_case {
		file_pattern = "(?i)" + file_pattern;
	}
	file_re, err := stdregexp.Compile(file_pattern)
	if err != nil {
		log.Print(err)
		return "Bad regular expression"
	}

	file_index_name := index.File() + ".files"
	file, err := os.Open(file_index_name)
	if err != nil {
		log.Print(err)
		return "Failed to open file index"
	}
	defer file.Close()

	query := pretty_print_query2(file_filter, exclude_file_filter,
		max_hits, ignore_case)

	fmt.Fprintf(writer, `
<table class="hit">
`)

	selected_id := 0
	hits := 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if hits >= max_hits {
			break
		}

		full_path := scanner.Text()
		path := RemovePathPrefix(full_path)

		file_url := fmt.Sprintf("/file/%s?%s", path, query)
		href := EscapeForAttributeValue(file_url)

		formatted_line, matches := escape_and_mark_line(
			path, hits, file_re, "")
		if !matches {
			continue
		}

		selected_class := ""
		if hits == selected_id {
			selected_class = " selected-file"
		}

		slices := strings.Split(path, "/")
		org_repo := strings.Join(slices[:2], "/")
		relative_path := strings.Join(slices[2:], "/")

		fmt.Fprintf(writer, `
<tr class="file-hit">
  <td class="file-hit">
    <table class="file-hit">
      <tr class="file-hit">
        <th id="file-hit-header-%d" class="file-hit-header%s">
          <a id="file-hit-%d" href="%s">%s</a>
        </th>
      </tr>
    </table>
    <script type="text/javascript">
      ORG_REPOS.push('%s');
      RELATIVE_PATHS.push('%s');
    </script>
  </td>
</tr>
`, hits, selected_class, hits,
   href,
   formatted_line,
   EscapeForAttributeValue(org_repo),
   EscapeForAttributeValue(relative_path))

		hits += 1
	}

	if err := scanner.Err(); err != nil {
		log.Print(err)
		return "Failed to read file"
	}

	fmt.Fprintf(writer, `
</table>
<script type="text/javascript">
  var selected_hit = %d;
  var num_hits = %d;
</script>
`, selected_id, hits)

        if (hits > 0) {
		fmt.Fprintf(writer, `
<hr class="end-of-results"/>
`)
	}

	message := fmt.Sprintf("%d files", hits)
	if hits >= max_hits {
		message += " (result list truncated)"
	}
	return message
}

func PrintTop(writer http.ResponseWriter, error string, query string,
	saved_h string, file_filter string, exclude_file_filter string,
	focus bool, javascript_filename string, checked bool) {
	if error != "" {
		error = fmt.Sprintf("<p style='color: red'>%s</p>", error)
	}

	checked_string := ""
	if checked {
		checked_string = " checked"
	}

	saved_h_input := ""
	if (saved_h != "") {
		saved_h_input = fmt.Sprintf(
			`<input id="saved_h" type="hidden" value="%s"/>`,
			EscapeForAttributeValue(saved_h))
	}

	// There's a timing issue with getting run_prettify.js running
	// before/after main(). prettyPrint() is called at setTimeout(x, 0),
	// which ought to be after the page has been loaded. Which is about the
	// same time as the body onload is called. We would like the onload to
	// just call it, but how??
	fmt.Fprintf(writer, `
<html>
  <head>
    <script type="text/javascript">
      var ORG_REPOS = [];
      var RELATIVE_PATHS = [];
      var LINENOS = [];
      var FILE_FROM_HIT = [];
      var HIT_FROM_FILE = [];
    </script>
    <link rel="stylesheet" type="text/css" href="/static/style.css"/>
    <script type="text/javascript" src="/static/lib.js"></script>
    <script src="https://cdn.rawgit.com/google/code-prettify/master/loader/run_prettify.js"></script>
    <!-- script type="text/javascript" src="/static/prettify/run_prettify.js"></script -->
    <script type="text/javascript" src="/static/%s"></script>
  </head>
  <body onload="main()">
    %s
    <form id="search" class="search" align="center" action="/search">
      Lines matching <input type="text" id="q" name="q" value="%s" size="30"/><span class="input-focus-key">q</span>
      in files matching <input type="text" id="f" name="f" value="%s" size="20"/><span class="input-focus-key">f</span>
      and not <input type="text" id="xf" name="xf" value="%s" size="20"/><span class="input-focus-key">x</span>
      case insensitive <input type="checkbox" id="i" name="i"%s/><span class="input-focus-key">i</span>
      <input type="hidden" id="h" name="h" value=""/>
      <input type="hidden" id="d" name="d" value=""/>
      <input id="Submit" type="submit" value="Search"><span class="input-focus-key">s</span>
      %s
    </form>

    <hr class="pre-help"/>

    <div id="help" class="help" style="display: none">
      <table align="center">
    	<tr>
    	  <td><span class="key">b</span><span class="key-description">: Git blame file</span></td>
    	  <td><span class="key">g</span><span class="key-description">: Open on GitHub</span></td>
    	  <td><span class="key">h</span><span class="key-description">: GitHub history</span></td>
    	  <td><span class="key">j</span><span class="key-description">: Next matching line</span></td>
    	  <td><span class="key">n</span><span class="key-description">: Next matching file</span></td>
    	  <td><span class="key">o</span><span class="key-description">: Open file</span></td>
    	  <td><span class="key">u</span><span class="key-description">: Close file</span></td>
    	  <td><span class="key">+</span><span class="key-description">: Expand matches in file</span></td>
	</tr>
	<tr>
    	  <td><span class="key">B</span><span class="key-description">: b in new window</span></td>
    	  <td><span class="key">G</span><span class="key-description">: g in new window</span></td>
    	  <td><span class="key">H</span><span class="key-description">: h in new window</span></td>
    	  <td><span class="key">k</span><span class="key-description">: Previous matching line</span></td>
    	  <td><span class="key">p</span><span class="key-description">: Previous matching file</span></td>
    	  <td><span class="key">O</span><span class="key-description">: o in new window</span></td>
    	  <td><span class="key">r</span><span class="key-description">: Reset search</span></td>
    	  <td><span class="key">-</span><span class="key-description">: Collapse matches in file</span></td>
    	</tr>
      </table>

      <hr class="post-help"/>

    </div>

`, javascript_filename, error,
   EscapeForAttributeValue(query),
   EscapeForAttributeValue(file_filter),
   EscapeForAttributeValue(exclude_file_filter),
   checked_string,
   saved_h_input)
}

func PrintBottom(writer http.ResponseWriter, message string) {
	timestamp := ""
	data, err := ioutil.ReadFile("/home/hakon/codesearch/index.ts")
	if (err != nil) {
		log.Print(err)
		timestamp = ""
	} else {
		timestamp = string(data)
	}

	fmt.Fprintf(writer, `
    <table class="footer">
      <tr class="footer">
        <td class="left-footer"><span class="key">?</span><span class="key-description"> toggles help</span></td>
        <td class="center-footer">%s</td>
        <td class="right-footer"><a href="/file/repos">repositories</a> indexed at %s</td>
      </tr>
    </table>
  </body>
</html>
`, EscapeString(message), timestamp)
}

func search_handler(w http.ResponseWriter, r *http.Request) {
	err := r.ParseForm()
	if err != nil {
		log.Fatal(err)
		return
	}

	query := r.Form.Get("q")
	file_filter := r.Form.Get("f")
	exclude_files := r.Form.Get("xf")
	direction := r.Form.Get("d")
	select_hit_string := r.Form.Get("h")
	select_hit, err := strconv.Atoi(select_hit_string)
	if err != nil {
		select_hit = -1
	}
	ignore_case := r.Form.Get("i") != ""
	max_hits_string := r.Form.Get("n")
	max_hits, err := strconv.Atoi(max_hits_string)
	if err != nil {
		max_hits = defaultMaxHits
	} else if max_hits > 1000 {
		max_hits = 1000
	}
	
	saved_h := ""
	message := ""

	if query == "" && file_filter != "" {
		PrintTop(w, "", query, saved_h, file_filter, exclude_files,
			true, "filesearch.js", ignore_case)
		message = SearchFile(w, r, file_filter, exclude_files,
			max_hits, ignore_case)
	} else {
		PrintTop(w, "", query, saved_h, file_filter, exclude_files,
			true, "search.js", ignore_case)
		if query != "" {
			message = Search(w, r, query, file_filter, select_hit,
				direction, exclude_files, max_hits,
				ignore_case)
		}
	}
	PrintBottom(w, message)
}

type MatchedLines struct {
	Linenos []int
	Index int
}

func PrintFileHeader(writer http.ResponseWriter, path string) {
	if path == "repos" {
		fmt.Fprintf(writer, `<pre id="repos" class="repos">
`)
	} else {
	        slices := strings.Split(path, "/")
	        org_repo := strings.Join(slices[:2], "/")
	        relative_path := strings.Join(slices[2:], "/")
		fmt.Fprintf(writer, `
<script type="text/javascript">
  var ORG_REPO = '%s';
  var RELATIVE_PATH = '%s';
</script>
<span class="path">%s</span>
<pre id="file-pre" class="prettyprint linenums">`,
        org_repo, relative_path, path)
	}
}

func PrintFileFooter(writer http.ResponseWriter, max_lineno int,
	matched_lines MatchedLines) {

	// matched_linenos is an array of lineno IDs, for lines that matches the
	// regexp. This can be used to jump to the previous/next match.
	matched_linenos_js := ""
	if len(matched_lines.Linenos) > 0 {
		matched_linenos_js = "var matched_linenos = [" +
			strconv.Itoa(matched_lines.Linenos[0])
		for _, i := range matched_lines.Linenos[1:] {
			matched_linenos_js = matched_linenos_js + ", " +
				strconv.Itoa(i)
		}
		matched_linenos_js = matched_linenos_js + `];
  var matched_linenos_index = ` + strconv.Itoa(matched_lines.Index) + ";"
	}

	fmt.Fprintf(writer, `
</pre>
<script type="text/javascript">
  var max_lineno = %d;
  %s
</script>

<hr class="end-of-results"/>
`, max_lineno, matched_linenos_js)
}

func escape_and_mark_line(line string,
			  id int,
			  re *stdregexp.Regexp,
			  href string) (string, bool) {
	if re == nil {
		return EscapeString(line), false
	}

	// TODO: Make the All version.
	matches := re.FindStringSubmatchIndex(line)
	if matches == nil {
		return EscapeString(line), false
	}

	begin_of_match := matches[0]
	end_of_match := matches[1]
	pre_text := EscapeString(line[0:begin_of_match])
	matched_text := EscapeString(line[begin_of_match:end_of_match])
	post_text := EscapeString(line[end_of_match:])

	if href == "" {
		return fmt.Sprintf(
			`%s<span id="match-%d" class="matched-text">%s</span>%s`,
			pre_text, id, matched_text, post_text), true
	} else {
		return fmt.Sprintf(
			`%s<a id="match-%d" class="matched-text" href="%s">%s</a>%s`,
			pre_text, id, href, matched_text, post_text), true
	}
}

func PrintFileLine(writer http.ResponseWriter, line string, lineno int,
	re *stdregexp.Regexp) bool {
	formatted_line, matches := escape_and_mark_line(line, lineno, re, "javascript: false")
	fmt.Fprintf(writer, "%s\n", formatted_line)
	return matches
}

func ShowFile(writer http.ResponseWriter, request *http.Request,
	path string, lineno int, query string, ignore_case bool) {
	pattern := query
	if ignore_case {
		pattern = "(?i)" + pattern
	}
	re, err := stdregexp.Compile(pattern)
	if err != nil {
		log.Print(err)
		return
	}

	file, err := os.Open(*pFlag + path)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	matched_lines := MatchedLines{make([]int, 0, 10), -1}

	PrintFileHeader(writer, path)
	i := 1
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if PrintFileLine(writer, scanner.Text(), i, re) {
			if i == lineno {
				matched_lines.Index = len(matched_lines.Linenos)
			}
			matched_lines.Linenos =
				append(matched_lines.Linenos, i)
		}
		i = i + 1
	}
	PrintFileFooter(writer, i, matched_lines)

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}

func file_handler(w http.ResponseWriter, request *http.Request) {
	err := request.ParseForm()
	if err != nil {
		log.Fatal(err)
		return
	}

	query := request.Form.Get("q")
	file_filter := request.Form.Get("f")
	exclude_file_filter := request.Form.Get("xf")
	saved_h := request.Form.Get("h")
	ignore_case := request.Form.Get("i") != ""
	lineno_string := request.Form.Get("l")
	lineno, err := strconv.Atoi(lineno_string)
	if err != nil {
		lineno = -1
	}
	path := strings.TrimPrefix(request.URL.Path, "/file/")

	error := ""
	if strings.Contains(path, "..") {
		error = "Path cannot contain \"..\""
	}

	PrintTop(w, error, query, saved_h, file_filter, exclude_file_filter,
		false, "file.js", ignore_case)
	if error == "" {
		ShowFile(w, request, path, lineno, query, ignore_case)
	}
	PrintBottom(w, "")
}

func main() {
	flag.Usage = usage
	flag.Parse()

	http.HandleFunc("/", search_handler)
	http.Handle("/static/", http.FileServer(http.Dir("src/code.google.com/p/codesearch/cmd/cserver/static")))
	http.HandleFunc("/file/", file_handler)
	http.ListenAndServe(":4443", nil)
	fmt.Println("ListenAndServe returned, exiting process!");
}
