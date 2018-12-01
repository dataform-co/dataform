--js type("incremental");
--js where(ctx => `ts > (select max(ts) from ${self()}) or (select max(ts) from ${self()}) is null`)
--js const crossdb = require("@dataform/crossdb");
select ${crossdb.currentTimestampUTC()} as ${describe("ts")}
