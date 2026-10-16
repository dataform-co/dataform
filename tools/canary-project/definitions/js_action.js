publish("js_action", {
  type: "view",
  description: "Canary JS action",
}).query((ctx) => `SELECT id, label FROM ${ctx.ref("table")}`);
