merge `project-id.dataset-id.incremental_on_schema_change` DATAFORM_DEST
using (select 1 as id, 'a' as field1, 'new' as field2
) DATAFORM_SOURCE
on DATAFORM_DEST.id = DATAFORM_SOURCE.id 

when matched then
  update set `id` = DATAFORM_SOURCE.id,`field1` = DATAFORM_SOURCE.field1
when not matched then
  insert (`id`,`field1`) values (`id`,`field1`)