const { materialize } = require("dft");

var {macro} = require("./includes/testmacro.js");

materialize("test1").query(`select 1 as test`);

materialize("test2")
  .query(
    ctx => `
    ${ctx.preHook("select 1 as prehook")}
    select ${macro("hello")}
    from ${ctx.ref("test1")}`
  );

materialize("test3")
  .preHook(`select 1 as test`)
  .query(`select 2 as test`)
  .postHook(`select 3 as test`);
