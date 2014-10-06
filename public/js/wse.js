var log = function(log) {
	console.log(log);
}

var clickHandler = function(did) {
	$.get("click_loging", 
		{
			"did": did,
			"query": query,
			"ranker": ranker,
			"action": "click"
		});
	bootbox.alert("You just clicked on document with did: " + did + ". Action has been recorded.");
}

$(function() {
	var splitResult = result.split("&&&");
	var results = [];

	$("#queryLabel").text(query);
	$("#rankerLabel").text(ranker);
	$("#countLabel").text(count);
	
	// Parse the result.
	for (var i = 0; i < (splitResult.length - 1) / 3; i++) {
		var rslt = {
			did: splitResult[ i * 3 ],
			score: splitResult[ i * 3 + 1],
			title: splitResult[ i * 3 + 2],

		};
		results.push(rslt);
	}

	var resultAreaDom = $("#resultArea");

	var templateHtml = _.template($("#result-template").html());

	// Build HTML from template.
	$.each(results, function(idx, item) {
		$.get("click_loging", 
		{
			"did": item.did,
			"query": query,
			"ranker": ranker,
			"action": "render"
		});
		resultAreaDom.append(templateHtml(item));
	});
});