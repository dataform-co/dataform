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


-- Apply schema change strategy (SYNCHRONIZE).
DECLARE invalid_removed_columns ARRAY<STRING>;
SET invalid_removed_columns = (
  SELECT IFNULL(ARRAY_AGG(col), []) FROM UNNEST(columns_removed) AS col WHERE col IN UNNEST(["id"])
);

IF ARRAY_LENGTH(invalid_removed_columns) > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    "Cannot drop columns %T as they are part of the unique key for table `project-id.dataset-id.incremental_on_schema_change`",
    invalid_removed_columns
  );
END IF;

IF ARRAY_LENGTH(columns_removed) > 0 THEN
  EXECUTE IMMEDIATE (
    "ALTER TABLE `project-id.dataset-id.incremental_on_schema_change` " ||
    (
      SELECT STRING_AGG(FORMAT("DROP COLUMN IF EXISTS %s", col), ", ")
      FROM UNNEST(columns_removed) AS col
    )
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
merge `project-id.dataset-id.incremental_on_schema_change` T
using (select 1 as id, 'a' as field1, 'new' as field2
) S
on T.id = S.id
  
when matched then
  update set `id` = S.id,`field1` = S.field1
when not matched then
  insert (`id`,`field1`) values (`id`,`field1`)