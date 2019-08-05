assert("sample_data_assertion_we_are_here_cesc")
  .query(ctx => `select * from ${ctx.ref("sample_data")} where sample > 3`)
  .dependencies([ "example_assertion",
                  "example_assertion_with_tags",
                  //"example_backticks",
                  //"example_deferred",
                  "example_incremental",
                  "example_inline",
                  "example_js_blocks",
                  "example_operation_with_output",
                  "example_operations",
                  "example_operations_with_tags",
                  "example_table",
                  "example_table_with_tags",
                  //"example_test_case",
                  //"example_using_inline",
                  "example_view"]);
                  