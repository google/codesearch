// Javascript loaded for cserver searches (/search and / paths).

function file_hit_header_id(index) { return "file-hit-header-" + index; }
function file_hit_id(index) { return "file-hit-" + index; }

function key_git(view, new_window) {
    if (!any_hits()) return;

    var org_repo = ORG_REPOS[selected_hit];
    var relative_path = RELATIVE_PATHS[selected_hit];
    goto_git(org_repo, relative_path, null, view, new_window);
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

function key_n() {
    if (!any_hits()) return;

    if (selected_hit + 1 >= num_hits) {
	return;
    }

    remove_class(file_hit_header_id(selected_hit), "selected-file");
    ++selected_hit;
    add_class(file_hit_header_id(selected_hit), "selected-file");
    get(file_hit_id(selected_hit)).focus();
}

function key_p() {
    if (!any_hits()) return;

    if (selected_hit <= 0) {
	return;
    }

    remove_class(file_hit_header_id(selected_hit), "selected-file");
    --selected_hit;
    add_class(file_hit_header_id(selected_hit), "selected-file");
    get(file_hit_id(selected_hit)).focus();
}

function key_o() {
    if (!any_hits()) return;

    var a = get(file_hit_id(selected_hit));
    if (!a) {
	return;
    }

    a.click();
}

function key_O() {
    if (!any_hits()) return;

    var a = get(file_hit_id(selected_hit));
    if (!a) {
	return;
    }

    var win = window.open(a.href, '_blank');
    win.focus();
}

function key_escape() {
    if (any_hits()) {
	var hit_link = get(file_hit_id(selected_hit));
	if (hit_link) {
	    hit_link.focus();
	    return;
	}
    }

    get("Submit").focus();
}

document.onkeypress = function(event) {
    if (input_focus()) {
	return true;
    }

    event = event || window.event;
    switch (event.keyCode) {
    case 0x3F: key_question(); break;
    case 0x42: key_B(); break;
    case 0x47: key_G(); break;
    case 0x48: key_H(); break;
    case 0x4F: key_O(); break;
    case 0x62: key_b(); break;
    case 0x66: key_f(); break;
    case 0x67: key_g(); break;
    case 0x68: key_h(); break;
    case 0x69: key_i(); break;
    case 0x6e: key_n(); break;
    case 0x6f: key_o(); break;
    case 0x70: key_p(); break;
    case 0x71: key_q(); break;
    case 0x72: key_r(); break;
    case 0x73: key_s(); break;
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

function GetFile(index) {
    var td = get("file-link-" + index);
    var location = td.innerHTML;
    var colon_index = location.lastIndexOf(":");
    var file_name = location.substring(0, colon_index);
    return file_name;
}

function main() {
    if (typeof num_hits == "undefined" || num_hits == 0) {
	focus("q");
    }

    if (!any_hits()) {
	    get("help").style.display = "block";
    }
}
