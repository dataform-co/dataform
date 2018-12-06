// Calls to materialize in includes files should NOT end up in the final graph.
materialize("example_ignore")
  .query("select 1 as test");
