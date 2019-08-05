assert("sample_data_assertion")
  .query(ctx => `select * from ${ctx.ref("sample_data")} where sample > 3`)
  .dependencies([ "example_assertion_with_tags",
                  "example_assertion",
                  "example_backticks",
                  "example_deferred.js",
                  "example_incremental",
                  "example_inline",
                  "example_js_blocks",
                  "example_operation_with_output",
                  "example_operations",
                  "example_operations_with_tags",
                  "example_table",
                  "example_table_with_tags",
                  "example_test_case",
                  "example_using_inline",
                  "example_view"]);


