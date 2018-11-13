${type("incremental")}
${where(ctx => `ts > (select max(ts) from ${self()}) or (select max(ts) from ${self()}) is null`)}
select current_timestamp() as ts
