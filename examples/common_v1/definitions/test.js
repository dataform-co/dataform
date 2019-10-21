assert("sample_data_assertion")
  .query(ctx => `select * from ${ctx.ref("sample_data")} where sample > 3`)
  .dependencies([ "example_backticks",
                  "example_deferred",
                  "example_incremental",
                  "example_inline",
                  "example_js_blocks",
                  "example_table",
                  "example_using_inline",
                  "example_view"]);
