const { materialize } = require("dft");

materialize("test1").query(`select 1 as test`);

materialize("test2")
  .query(
    ctx => `
    select *
    from ${ctx.ref("test1")}`
  );

materialize("test3")
  .preHook(`select 1 as test`)
  .query(`select 2 as test`)
  .postHook(`select 3 as test`);
