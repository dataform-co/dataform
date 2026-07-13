CREATE OR REPLACE PROCEDURE `project-id.dataset-id.df_osc_test_uuid`()
OPTIONS(strict_mode=false)
BEGIN

-- Create empty table to extract schema of new query.
CREATE OR REPLACE TABLE `project-id.dataset-id.incremental_on_schema_change_df_temp_test_uuid_empty` AS (
  SELECT * FROM (select 1 as id, 'a' as field1, 'new' as field2) AS insertions LIMIT 0
);


-- Compare schemas
DECLARE dataform_columns ARRAY<STRING>;
DECLARE temp_table_columns ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_added ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_removed ARRAY<STRING>;

SET dataform_columns = (
  SELECT IFNULL(ARRAY_AGG(DISTINCT column_name), [])
  FROM `project-id.dataset-id.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'incremental_on_schema_change'
);

SET temp_table_columns = (
  SELECT IFNULL(ARRAY_AGG(STRUCT(column_name, data_type)), [])
  FROM `project-id.dataset-id.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'incremental_on_schema_change_df_temp_test_uuid_empty'
);

SET columns_added = (
  SELECT IFNULL(ARRAY_AGG(column_info), [])
  FROM UNNEST(temp_table_columns) AS column_info
  WHERE column_info.column_name NOT IN UNNEST(dataform_columns)
);
SET columns_removed = (
  SELECT IFNULL(ARRAY_AGG(column_name), [])
  FROM UNNEST(dataform_columns) AS column_name
  WHERE column_name NOT IN (SELECT col.column_name FROM UNNEST(temp_table_columns) AS col)
);


-- Apply schema change strategy (EXTEND).
IF ARRAY_LENGTH(columns_removed) > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    "Column removals are not allowed when on_schema_change = 'EXTEND'. Removed columns: %T",
    columns_removed
  );
END IF;

IF ARRAY_LENGTH(columns_added) > 0 THEN
  EXECUTE IMMEDIATE (
    "ALTER TABLE `project-id.dataset-id.incremental_on_schema_change` " ||
    (
      SELECT STRING_AGG(FORMAT("ADD COLUMN IF NOT EXISTS %s %s", column_info.column_name, column_info.data_type), ", ")
      FROM UNNEST(columns_added) AS column_info
    )
  );
END IF;



-- Cleanup temporary tables.
DROP TABLE IF EXISTS `project-id.dataset-id.incremental_on_schema_change_df_temp_test_uuid_empty`;
    
END
;
BEGIN
  CALL `project-id.dataset-id.df_osc_test_uuid`();
EXCEPTION WHEN ERROR THEN
  DROP TABLE IF EXISTS `project-id.dataset-id.incremental_on_schema_change_df_temp_test_uuid_empty`;
  DROP PROCEDURE IF EXISTS `project-id.dataset-id.df_osc_test_uuid`;
  RAISE;
END;
DROP PROCEDURE IF EXISTS `project-id.dataset-id.df_osc_test_uuid`
;
CREATE OR REPLACE TEMP TABLE `staging_table_temp_test_uuid` AS (
  select 1 as id, 'a' as field1, 'new' as field2
);

BEGIN
  DECLARE partitions_for_replacement DEFAULT (
    ARRAY(
      SELECT DISTINCT DATE(ts)
      FROM `staging_table_temp_test_uuid`
      WHERE DATE(ts) IS NOT NULL
    )
  );

  MERGE `project-id.dataset-id.incremental_on_schema_change` DATAFORM_DEST
  USING `staging_table_temp_test_uuid` DATAFORM_SOURCE
  ON FALSE
  WHEN NOT MATCHED BY SOURCE AND DATE(ts) IN UNNEST(partitions_for_replacement) 
  
  THEN
    DELETE
  WHEN NOT MATCHED BY TARGET THEN
    INSERT (`id`,`field1`) VALUES (`id`,`field1`);
END;

DROP TABLE IF EXISTS `staging_table_temp_test_uuid`
