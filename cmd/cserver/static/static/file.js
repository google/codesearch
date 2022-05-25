// Utilities for requests with /file/ path prefix.

// The id of the td element holding a file line.
function line_td_id(lineno) { return "line-" + lineno; }

// The class of the td element holding a file line.
function line_td_class() { return "file-line"; }

// The class of the td element holding a FOCUSED file line.
function line_td_focus_class() { return "file-line-hit"; }

function key_git(view, new_window) {
    var lineno = null;
    if (matched_linenos_index >= 0) {
        lineno = matched_linenos[matched_linenos_index];
    }
    goto_git(ORG_REPO, RELATIVE_PATH, lineno, view, new_window);
}

function key_escape() {
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

var key_j_hit_once = false;
var key_k_hit_once = false;

function key_j() {
    key_k_hit_once = false;

    if (matched_linenos_index < 0) {
	return;
    }

    if (matched_linenos_index + 1 >= matched_linenos.length) {
	// Jump to next file on second time we hit key_j.
	if (key_j_hit_once) {
	    key_u("d");
	} else {
	    key_j_hit_once = true;
	}
	return;
    }

    var previous_lineno = matched_linenos[matched_linenos_index];
    remove_class("l" + previous_lineno, "hit");
    ++matched_linenos_index;
    var lineno = matched_linenos[matched_linenos_index];
    add_class("l" + lineno, "hit");
    var a_element = get("match-" + lineno);
    if (a_element) {
	a_element.focus();
    }
}

function key_k() {
    key_j_hit_once = false;

    if (matched_linenos_index < 0) {
	return;
    }

    if (matched_linenos_index == 0) {
	// Jump to previous file on second time we hit key_j.
	if (key_k_hit_once) {
	    key_u("u");
	} else {
	    key_k_hit_once = true;
	}
	return;
    }

    var previous_lineno = matched_linenos[matched_linenos_index];
    remove_class("l" + previous_lineno, "hit");
    --matched_linenos_index;
    var lineno = matched_linenos[matched_linenos_index];
    add_class("l" + lineno, "hit");
    var a_element = get("match-" + lineno);
    if (a_element) {
	a_element.focus();
    }
}

function key_u(direction) {
    var saved_h = get("saved_h");
    if (saved_h) {
	get("h").value = saved_h.value;
    }
    get("d").value = direction;

    get("search").submit();
}

document.onkeypress = function (event) {
    if (input_focus()) {
	return true;
    }

    event = event || window.event;
    // use event.keyCode
    switch (event.keyCode) {
    case 0x3F: key_question(); break;
    case 0x42: key_B(); break;
    case 0x47: key_G(); break;
    case 0x48: key_H(); break;
    case 0x62: key_b(); break;
    case 0x66: key_f(); break;
    case 0x67: key_g(); break;
    case 0x68: key_h(); break;
    case 0x69: key_i(); break;
    case 0x6a: key_j(); break;
    case 0x6b: key_k(); break;
    case 0x71: key_q(); break;
    case 0x72: key_r(); break;
    case 0x73: key_s(); break;
    case 0x75: key_u(""); break;
    case 0x78: key_x(); break;
    default:
	return true;
    }

    return false;
};

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

function main() {
    var func = function () {
	var pre = get("file-pre");
	var lis = pre.children[0].children;
	var i = 0;
	for (; i < lis.length; ++i) {
	    var li = lis[i];
	    var lineno = i + 1;
	    li.id = "l" + lineno;
	}
	if (window.location.hash) {
	    window.location = window.location;
	}
	if (matched_linenos_index != -1) {
	    var lineno = matched_linenos[matched_linenos_index];
	    lis[lineno - 1].className += " hit";
	    var a_element = get("match-" + lineno);
	    if (a_element) {
		a_element.focus();
	    }
	}
    };

    setTimeout(func, 0);
}
