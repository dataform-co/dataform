assert("sample_data_assertion")
  .query(ctx => [`select * from ${ctx.ref("sample_data")} where sample > 3`])
  .dependencies("example_*");
throw Error('Error in JS');
