// Utilities for cserver.

var PREFIX_KEY_FUNCTION = null;

var PENDING_ASSIGNMENT_TEXT = null;

function escape_for_regex(text) {
    var result = "";
    for (var i = 0; i < text.length; ++i) {
	var c = text[i];
	switch (c) {
	case '\\':
	case '^':
	case '$':
	case '(':
	case ')':
	case '{':
	case '}':
	case '[':
	case ']':
	case '.':
	case '?':
	case '*':
	case '+':
	    result += "\\" + c;
	    break;
	default:
	    result += c;
	}
    }

    return result;
}

function get(id) {
    return document.getElementById(id);
}

function add_class(id, Class) {
    var element = get(id);
    if (element) {
	if (element.className) {
	    element.className += " " + Class;
	} else {
	    element.className = Class;
	}
    }
}

function remove_class(id, Class) {
    var element = get(id);
    if (element) {
	element.className = (element.className + " ").replace(Class + " ", "");
	element.className = element.className.slice(0, -1);
    }
}

function q_input() {
    return get("q");
}

function f_input() {
    return get("f");
}

function xf_input() {
    return get("xf");
}

function selection() {
    return selected_hit >= 0 || SELECTED_FILE >= 0;
}

// Returns true iff the current focus is on an input field.
function input_focus() {
    var active_element = document.activeElement;
    if (!active_element) {
	return false;
    }

    return active_element == q_input() ||
	active_element == f_input() ||
	active_element == xf_input();
}

function focus(id) {
    var element = get(id);
    element.onfocus = function() {
	this.selectionStart = this.selectionEnd = this.value.length;
    };
    element.focus();
}

function open_link(a_element, new_window) {
    if (a_element == null) return;
    if (new_window) {
	var win = window.open(a_element.href, '_blank');
	// win.focus();
    } else {
	a_element.click();
    }
}

function open_url(url, new_window) {
    if (url == null) return;
    if (new_window) {
	var win = window.open(url, '_blank');
	// win.focus();
    } else {
	window.location = url;
    }
}

function goto_git(org_repo, relative_path, lineno, view, new_window) {
    var fragment = '';
    if (lineno != null && lineno >= 1) {
        fragment = '#L' + lineno;
    }
    if (org_repo.lastIndexOf('vespa-engine/', 0) == 0) {
        var server = 'github.com';
    } else {
        var server = 'git.vzbuilders.com';
    }
    if (!view) {
      view = 'blob';
    }
    var git_url = 'https://' + server + '/' + org_repo +
        '/' + view + '/master/' + relative_path + fragment;
    open_url(git_url, new_window);
}

function some_results() {
    if (typeof num_hits != "undefined" && num_hits > 0) {
	return true;
    }

    if (typeof NUM_FILES != "undefined" && NUM_FILES > 0) {
	return true;
    }

    return false;
}

// Deprecated
function any_hits() {
    if (typeof num_hits == "undefined") {
	return false;
    }

    return num_hits > 0;
}

function toggle_set_value_of(input_id, value) {
    var element = get(input_id);
    if (element.value == value) {
	element.value = "";
    } else {
	element.value = value;
    }
}

function key_f() {
    focus("f");
}

function key_i() {
    var i_element = get("i")
    i_element.checked = !i_element.checked;
}

function key_question() {
    var style = get("help").style;
    if (style.display == "none") {
	style.display = "block";
    } else {
	style.display = "none";
    }
}

function key_q() {
    focus("q");
}

function key_r() {
    window.location = "/";
}

function key_s() {
    get("Submit").click();
}

function key_t() {
    var test_file_regex = "/tests?/|_tests?\\b|Tests?[^a-z]|/systemtests/|\\.html$";
    var xf = get("xf");
    if (xf.value == test_file_regex) {
	xf.value = "";
    } else {
	xf.value = test_file_regex;
    }
}

function key_x() {
    focus("xf");
}

function generic_onkeypress(event) {
    if (input_focus()) {
	PREFIX_KEY_FUNCTION = null;
	return true;
    }

    event = event || window.event;

    if (PREFIX_KEY_FUNCTION == null) {
	switch (event.keyCode) {
	case 0x3F: key_question(); return false;
	case 0x66: key_f(); return false;
	case 0x69: key_i(); return false;
	case 0x71: key_q(); return false;
	case 0x72: key_r(); return false;
	case 0x73: key_s(); return false;
	case 0x74: key_t(); return false;
	case 0x78: key_x(); return false;
	default:
	    // alert('generic_onkeypress: Not bound: ' + event.keyCode);
	}
    }

    // null means the site-specific onkeypress event handler should try to
    // process the event.
    return null;
};
