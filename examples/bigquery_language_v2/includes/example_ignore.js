// Calls to publish in includes files should NOT end up in the final graph.
publish("example_ignore")
  .query("select 1 as test");
