// Javascript loaded for cserver searches (/search and / paths).

// The id of the td element containing the matching line. index starts at 0 for
// the first hit.
function line_hit_td_id(index) { return "line-hit-" + index; }

// The id of the location td element containing the matching line. index starts
// at 0 for the first hit.
function location_hit_td_id(index) { return "location-" + index; }

// The a element id that points to a file and line number. index starts at 0.
function hit_link_id(index) { return "file-link-" + index; }

// The a element id enclosing the match in the line.
//function a_match_id(index) { return "match-" + index; }
function a_match_id(index) { return "file-link-" + index; }

// The class of the td element containing a SELECTED matching line.
function line_hit_td_selected_class() { return "line-selected"; }

function file_hit_header_id(index) { return "file-hit-header-" + index; }
function file_hit_id(index) { return "file-hit-" + index; }
// The tr element containing the list of hits for a file
function file_hit_body_id(index) { return "file-hit-body-" + index; }

function file_hit_body_from_hit(index) {
    return get(file_hit_body_id(FILE_FROM_HIT[index]))
}

function file_hit_body_from_file(index) {
    return get(file_hit_body_id(index))
}

// Warning: does not modify selected_hit
function unselect_hit() {
    if (selected_hit < 0 || selected_hit >= num_hits) return;

    remove_class(hit_link_id(selected_hit), line_hit_td_selected_class());
    remove_class(line_hit_td_id(selected_hit), line_hit_td_selected_class());
    remove_class(location_hit_td_id(selected_hit), "location-selected");
}

// Warning: dos not modify selected_hit
function select_hit() {
    if (selected_hit < 0 || selected_hit >= num_hits) return;

    add_class(hit_link_id(selected_hit), line_hit_td_selected_class());
    add_class(line_hit_td_id(selected_hit), line_hit_td_selected_class());
    add_class(location_hit_td_id(selected_hit), "location-selected");
    get(a_match_id(selected_hit)).focus();
}

function unselect_file() {
    if (SELECTED_FILE < 0 || SELECTED_FILE >= NUM_FILES) return;

    remove_class(file_hit_header_id(SELECTED_FILE), "selected-file");
}

function select_file() {
    if (SELECTED_FILE < 0 || SELECTED_FILE >= NUM_FILES) return;

    add_class(file_hit_header_id(SELECTED_FILE), "selected-file");
    get(file_hit_id(SELECTED_FILE)).focus();
}

function hit_displayed(hit_id) {
    return file_hit_body_from_hit(hit_id).style.display != "none";
}

function search_hit(base_hit, sign) {
    var hit = base_hit;
    while (true) {
	hit += sign;
	if (hit < 0 || hit >= num_hits) return -1;
	if (hit_displayed(hit)) break;
    }

    return hit;
}

function refresh_selected() {
    if (selected_hit >= 0) {
	unselect_hit();
	select_hit();
    } else if (SELECTED_FILE >= 0) {
	unselect_file();
	select_file();
    }
}

function bump_selected_hit(sign) {
    if (num_hits <= 0) return;

    if (selected_hit >= 0) {
	var new_selected_hit = search_hit(selected_hit, sign);
	if (new_selected_hit < 0) return;

	unselect_hit();
	selected_hit = new_selected_hit;
	select_hit();
    } else if (SELECTED_FILE >= 0) {
	var new_selected_hit;
	if (sign > 0) {
	    new_selected_hit = search_hit(HIT_FROM_FILE[SELECTED_FILE] - 1, sign);
	} else {
	    new_selected_hit = search_hit(HIT_FROM_FILE[SELECTED_FILE], sign);
	}
	    
	if (new_selected_hit < 0) return;
	selected_hit = new_selected_hit;

	unselect_file();
	SELECTED_FILE = -1;

	select_hit();
    } else {
	alert('bump_selected_hit: Neither hit nor file has been selected');
    }
}

function bump_selected_file(sign) {
    if (NUM_FILES <= 0) return;

    if (selected_hit >= 0) {
	if (sign < 0) {
	    SELECTED_FILE = FILE_FROM_HIT[selected_hit] + sign + 1;
	} else {
	    SELECTED_FILE = FILE_FROM_HIT[selected_hit] + sign;
	}

	if (SELECTED_FILE < 0) {
	    SELECTED_FILE = 0;
	} else if (SELECTED_FILE >= NUM_FILES) {
	    SELECTED_FILE = NUM_FILES - 1;
	}

	unselect_hit();
	selected_hit = -1;

	select_file();
    } else if (SELECTED_FILE >= 0) {
	var new_selected_file = SELECTED_FILE + sign;
	if (new_selected_file < 0 || new_selected_file >= NUM_FILES) return;

	unselect_file();
	SELECTED_FILE = new_selected_file;
	select_file();
    } else {
	alert('bump_selected_file: Neither hit nor file has been selected');
    }
}

function GetPath() {
    if (selected_hit >= 0) {
	var org_repos = ORG_REPOS[selected_hit];
	var relative_path = RELATIVE_PATHS[selected_hit];
	return org_repos + "/" + relative_path;
    } else if (SELECTED_FILE >= 0) {
	var td = get("file-hit-" + SELECTED_FILE);
	return td.innerHTML;
    } else {
	alert("Internal error: GetPath called with not selection");
    }
}

function GetDirectory() {
    var path = GetPath();
    var slash_index = path.lastIndexOf("/");
    var directory = path.substring(0, slash_index);
    return directory;
}

function GetFilename() {
    var location = GetPath();
    var slash_index = location.lastIndexOf("/");
    var filename = location.substring(slash_index + 1);
    return filename;
}

function key_git(view, new_window) {
    var path = GetPath(SELECTED_FILE);

    var separator_index = path.indexOf('/', 0);
    if (separator_index < 0) {
	alert("Internal error: path without /: " + path);
	return;
    }

    separator_index = path.indexOf('/', separator_index + 1);
    if (separator_index < 0) {
	alert("Internal error: path without two /: " + path);
	return;
    }

    var org_repo = path.substring(0, separator_index);
    var relative_path = path.substring(separator_index + 1);

    
    var lineno = -1;
    if (selected_hit >= 0) {
	lineno = LINENOS[selected_hit];
    }

    goto_git(org_repo, relative_path, lineno, view, new_window);
}

function key_plus() {
    var selected_file;
    if (selected_hit >= 0) {
	selected_file = FILE_FROM_HIT[selected_hit];
    } else {
	selected_file = SELECTED_FILE;
    }

    var style = file_hit_body_from_file(selected_file).style;
    if (style.display == "none") {
	style.display = "table-row";
    } else {
	for (var i = 0; i < NUM_FILES; ++i) {
	    file_hit_body_from_file(i).style.display = "table-row";
	}
    }
}

function key_minus() {
    if (selected_hit >= 0) {
	key_p();
    }

    var style = file_hit_body_from_file(SELECTED_FILE).style;
    if (style.display == "none") {
	for (var i = 0; i < NUM_FILES; ++i) {
	    file_hit_body_from_file(i).style.display = "none";
	}
    } else {
	style.display = "none";
    }
}

// Common functionality for key_o() and key_O()
function key_o_element() {
    if (num_hits <= 0) return null;

    if (selected_hit >= 0) {
	return get(hit_link_id(selected_hit));
    } else if (SELECTED_FILE >= 0) {
	// TODO: Make 'b' work after an 'o'/'O' on a file.
	return get(file_hit_id(SELECTED_FILE));
    } else {
	alert('key_o: neither a hit nor file has been selected');
    }
}

function key_escape() {
    if (any_hits()) {
	var hit_link = get(hit_link_id(selected_hit));
	if (hit_link) {
	    hit_link.focus();
	    return;
	}
    }

    get("Submit").focus();
}

function key_b() {
    key_git('blame', false);
}

function key_B() {
    key_git('blame', true);
}

function key_g() {
    key_git('blob', false);
}

function key_G() {
    key_git('blob', true);
}

function key_h() {
    key_git('commits', false);
}

function key_H() {
    key_git('commits', true);
}

function key_j() {
    bump_selected_hit(1);
}

function search_java_class() {
    var q = get("q");

    var prefix = "(class|interface) ";
    var selection = "ClassName";
    var suffix = "\\b";
    q.value = prefix + selection + suffix;

    q.focus();
    q.selectionStart = prefix.length;
    q.selectionEnd = q.selectionStart + selection.length;
}

function search_java_files() {
    toggle_set_value_of("f", "\\.java$");
}

function J_prefix_key_function(keyCode) {
    var c = String.fromCharCode(keyCode);
    switch (c) {
    case 'c':
	search_java_class();
	return false;
    case 'f':
	search_java_files();
	return false;
    case 'q':
	search_java_files();
	focus("q");
	return false;
    }

    return true;
}

function key_J() {
    PREFIX_KEY_FUNCTION = J_prefix_key_function;
}

function key_k() {
    bump_selected_hit(-1);
}

function key_n() {
    bump_selected_file(1);
}

function key_o() {
    open_link(key_o_element(), false);
}

function key_O() {
    open_link(key_o_element(), true);
}

function key_p() {
    bump_selected_file(-1);
}

function equal_then_then(keyCode) {
    if (selected_hit < 0 && SELECTED_FILE < 0) {
	alert("Internal error: equal_then_then: There is no selection");
	return true;
    }

    var first_char = PENDING_ASSIGNMENT_TEXT;
    var second_char = String.fromCharCode(keyCode);

    // WARNING: This case-list must match the one in equal_then().
    switch (first_char) {
    case 'd':
	var directory = escape_for_regex(GetDirectory());
	switch (second_char) {
	case 's':
	    // 'd' 's' ==> directory to query field (input field brought to
	    // focus with 's' which has an ID of "q").
	    toggle_set_value_of("q", "\\b" + directory + "\\b");
	    return false;
	case 'f':
	    // Match all files that are in or below the directory.
	    toggle_set_value_of("f", "^" + directory + "/");
	    return false;
	case 'F':
	    toggle_set_value_of("xf", "^" + directory + "/");
	    return false;
	}
	return true;
    case 'f':
	var filename = escape_for_regex(GetFilename());
	switch (second_char) {
	case 's':
	    toggle_set_value_of("q", "\\b" + filename + "\\b");
	    return false;
	case 'f':
	    toggle_set_value_of("f", "/" + filename + "$");
	    return false;
	case 'F':
	    toggle_set_value_of("xf", "/" + filename + "$");
	    return false;
	}
	return true;
    case 'p':
	var path = escape_for_regex(GetPath());
	switch (second_char) {
	case 's':
	    toggle_set_value_of("q", "\\b" + path + "\\b");
	    return false;
	case 'f':
	    // Match all files that are in or below the path.
	    toggle_set_value_of("f", "^" + path + "$");
	    return false;
	case 'F':
	    toggle_set_value_of("xf", "^" + path + "$");
	    return false;
	}
	return true;
    }

    return true;
}

function equal_then(keyCode) {
    if (selected_hit < 0 && SELECTED_FILE < 0) {
	alert("Internal error: equal_then: There is no selection");
	return true;
    }

    var c = String.fromCharCode(keyCode);
    // WARNING: This case-list must match the one in equal_then_then.
    switch (c) {
    case 'd':
    case 'f':
    case 'p':
	PENDING_ASSIGNMENT_TEXT = c;
	PREFIX_KEY_FUNCTION = equal_then_then;
	return false;
    }

    return true;
}

function key_equal_sign() {
    PREFIX_KEY_FUNCTION = equal_then;
}

document.onkeydown = function(event) {
    event = event || window.event;
    switch (event.keyCode) {
    case 0x1b:
	key_escape();
	break;
    default:
	return true;
    }

    return false;
};

document.onkeypress = function(event) {
    var propagate_event = generic_onkeypress(event);
    if (propagate_event != null) {
	return propagate_event;
    }

    event = event || window.event;

    if (PREFIX_KEY_FUNCTION == null) {
	switch (event.keyCode) {
	case 0x2B: key_plus(); return false;
	case 0x2D: key_minus(); return false;
	case 0x3D: key_equal_sign(); return false;
	case 0x42: key_B(); return false;
	case 0x47: key_G(); return false;
	case 0x48: key_H(); return false;
	case 0x4A: key_J(); return false;
	case 0x4F: key_O(); return false;
	case 0x62: key_b(); return false;
	case 0x64: key_d(); return false;
	case 0x67: key_g(); return false;
	case 0x68: key_h(); return false;
	case 0x6A: key_j(); return false;
	case 0x6B: key_k(); return false;
	case 0x6E: key_n(); return false;
	case 0x6F: key_o(); return false;
	case 0x70: key_p(); return false;
	default:
	    // alert('Not bound: ' + event.keyCode);
	}
    } else {
	var func = PREFIX_KEY_FUNCTION;
	PREFIX_KEY_FUNCTION = null;
	return func(event.keyCode);
    }

    return true;
};

function main() {
    if (some_results()) {
	if (direction == "d") {
	    key_n();
	    key_j();
	    key_o();
	} else if (direction == "u") {
	    key_p();
	    key_k();
	    key_o();
	}

	refresh_selected();
    } else {
	key_question();
	key_q();
    }
}

