var admin = new Admin({
  baseUrl:    turntableBaseUrl,
  runPath:    "stage",
  addPath:    "add",
  removePath: "remove"
});

admin.clickQuery = function(d) {
  $("#name").val(d.opts.name);
  $("#query").val(d.query);
  $("#period").val(d.period);
};

admin.testQuery = function(opts) {
  runQuery(opts.db, opts.query);
};

admin.addTree("#query-tree");

function runQuery(db, query) {
  $("#inspect-db").html(db);
  $("#inspect-query").html(query);
  var $button = $("#test");
  $button.attr("disabled", true);
  admin.runQuery({query: query, db: db}, function(d) {
    $("#inspect-results").text(d);
  }, function() {
    $button.attr("disabled", false);
  });
};

$(document).ready(function() {
  admin.addOpts("db", "#db");

  $("#add").click(function(e) {
    var $button = $(this);
    $button.attr("disabled", true);
    admin.addQuery({
      query: $("#query").val(),
      db: $("#db").val(),
      period: $("#period").val(),
      name: $("#name").val()
    }, function(d) {
      $button.attr("disabled", false);
    });
  });

  $("#test").click(function() {
    runQuery($("#db").val(), $("#query").val());
  });

  $("#name").focus(function(e) { $(this).attr("placeholder", "query:name") });
  $("#name").blur(function(e)  { $(this).attr("placeholder", "name")       });

  $("#period").focus(function(e) { $(this).attr("placeholder", "{:minute [0 15 30 45]}") });
  $("#period").blur(function(e)  { $(this).attr("placeholder", "run at")                 });
});
