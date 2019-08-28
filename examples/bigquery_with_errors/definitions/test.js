assert("sample_data_assertion")
  .query(ctx => `select * from ${ctx.ref("sample_data")} where sample > 3`)
  .dependencies(["example_incremental", "example_js_blocks", "example_table", "example_view"]);
throw new Error("Error in JS");
