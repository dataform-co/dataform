assert("sample_data_assertion")
  .query(ctx => `select * from ${ctx.ref("sample_data")} where sample > 3`)
  .dependencies("example_incremental",
                "example_inline",
                "example_table",
                "example_table_dependency",
                "example_using_inline",
                "example_view");
