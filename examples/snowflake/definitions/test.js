assert("sample_data_assertion")
  .query(ctx => `select * from ${ctx.ref("sample_data")} where sample_column > 3`)
  .dependencies("example_*");
