insert into "df_integration_test"."example_incremental"
(val1)
select val1
from (
  select * from (
select 1 as val1, 2 as val2
   ) as subquery
      where true) as insertions