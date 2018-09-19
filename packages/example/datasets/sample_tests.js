assert("sample_copy_assertion", ctx => [
  `select * from ${ctx.ref("sample_copy")} where sample > 3`
])
