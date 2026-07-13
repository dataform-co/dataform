CREATE OR REPLACE TEMP TABLE `staging_table_temp_test_uuid` AS (
  select 1 as id, 'a' as field1, 'new' as field2
);

BEGIN
  DECLARE partitions_for_replacement DEFAULT (
    ARRAY(
      SELECT DISTINCT date_col
      FROM `staging_table_temp_test_uuid`
      WHERE date_col IS NOT NULL
    )
  );

  MERGE `project-id.dataset-id.incremental_on_schema_change` DATAFORM_DEST
  USING `staging_table_temp_test_uuid` DATAFORM_SOURCE
  ON FALSE
  WHEN NOT MATCHED BY SOURCE AND date_col IN UNNEST(partitions_for_replacement) 
  
  THEN
    DELETE
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (`id`,`field1`) VALUES (`id`,`field1`);
END;

DROP TABLE IF EXISTS `staging_table_temp_test_uuid`;
