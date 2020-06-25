--js type("incremental");
--js where(ctx => `ts > (select max(ts) from ${self()}) or (select max(ts) from ${self()}) is null`)
--js const sql = require("@dataform/sql");
select ${sql().timestampCurrentUTC()} as ts
