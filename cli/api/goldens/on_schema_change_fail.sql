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


-- Apply schema change strategy (FAIL).
IF ARRAY_LENGTH(columns_added) > 0 OR ARRAY_LENGTH(columns_removed) > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    "Schema mismatch defined by on_schema_change = 'FAIL'. Added columns: %T, removed columns: %T",
    columns_added,
    columns_removed
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
insert into `project-id.dataset-id.incremental_on_schema_change`	
(`id`,`field1`)	
select `id`,`field1`	
from (select 1 as id, 'a' as field1, 'new' as field2) as insertions