function highlight() {
	if(window.location.hash) {
		var span = document.getElementById(window.location.hash.substr(1));
		if(span) {
			span.classList.add("sel");
		}
	}
}
