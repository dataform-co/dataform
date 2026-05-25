merge `project-id.dataset-id.incremental_on_schema_change` T
using (select 1 as id, 'a' as field1, 'new' as field2
) S
on T.id = S.id
  
when matched then
  update set `id` = S.id,`field1` = S.field1
when not matched then
  insert (`id`,`field1`) values (`id`,`field1`)