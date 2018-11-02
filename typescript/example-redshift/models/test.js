assert("sample_data_assertion", ctx => [
  `select * from ${ctx.ref("sample_data")} where sample > 3`
])
