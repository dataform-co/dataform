BEGIN
SELECT 'inc_pre_op';

BEGIN

-- Declare variables for schema comparison and strategy execution.
DECLARE dataform_columns ARRAY<STRING>;
DECLARE dataform_columns_list STRING;
DECLARE dataform_columns_merge STRING;
DECLARE dataform_temp_table_columns ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE dataform_columns_added ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE dataform_columns_removed ARRAY<STRING>;


-- Create empty table to extract schema of new query.
CREATE OR REPLACE TABLE `project-id.default_dataset.customer_orders_df_temp_test_uuid_empty`
OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS (
  SELECT * FROM (SELECT 1 AS order_id, 2 AS customer_id, 'new' AS status
) AS insertions LIMIT 0
);


-- Compare schemas
SET dataform_columns = ARRAY<STRING>['order_id', 'customer_id'];

SET dataform_temp_table_columns = (
  SELECT IFNULL(ARRAY_AGG(STRUCT(column_name, data_type)), [])
  FROM `project-id.default_dataset.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'customer_orders_df_temp_test_uuid_empty'
);

SET dataform_columns_added = (
  SELECT IFNULL(ARRAY_AGG(column_info), [])
  FROM UNNEST(dataform_temp_table_columns) AS column_info
  WHERE column_info.column_name NOT IN UNNEST(dataform_columns)
);
SET dataform_columns_removed = (
  SELECT IFNULL(ARRAY_AGG(column_name), [])
  FROM UNNEST(dataform_columns) AS column_name
  WHERE column_name NOT IN (SELECT col.column_name FROM UNNEST(dataform_temp_table_columns) AS col)
);


-- Cleanup empty temp table
DROP TABLE IF EXISTS `project-id.default_dataset.customer_orders_df_temp_test_uuid_empty`;
    


-- Apply schema change strategy (EXTEND).
IF ARRAY_LENGTH(dataform_columns_removed) > 0 THEN
  RAISE USING MESSAGE = FORMAT(
    "Column removals are not allowed when on_schema_change = 'EXTEND'. Removed columns: %T",
    dataform_columns_removed
  );
END IF;

IF ARRAY_LENGTH(dataform_columns_added) > 0 THEN
  EXECUTE IMMEDIATE (
    "ALTER TABLE `project-id.ecommerce_catalog.sales_dw.customer_orders` " ||
    (
      SELECT STRING_AGG(FORMAT("ADD COLUMN IF NOT EXISTS %s %s", column_info.column_name, column_info.data_type), ", ")
      FROM UNNEST(dataform_columns_added) AS column_info
    )
  );
END IF;



-- Prepare dynamic column lists for DML.
SET dataform_columns_list = (
  SELECT STRING_AGG(FORMAT("`%s`", column_info.column_name), ", ")
  FROM UNNEST(dataform_temp_table_columns) AS column_info
);
SET dataform_columns_merge = (
  SELECT STRING_AGG(FORMAT("`%s` = DATAFORM_SOURCE.`%s`", column_info.column_name, column_info.column_name), ", ")
  FROM UNNEST(dataform_temp_table_columns) AS column_info
);

CREATE OR REPLACE TEMP TABLE `staging_table_temp_test_uuid` AS (
  SELECT 1 AS order_id, 2 AS customer_id, 'new' AS status
);

EXECUTE IMMEDIATE ("""
merge `project-id.ecommerce_catalog.sales_dw.customer_orders` DATAFORM_DEST
using `staging_table_temp_test_uuid` DATAFORM_SOURCE
on DATAFORM_DEST.order_id = DATAFORM_SOURCE.order_id 

when matched then
  update set """ || dataform_columns_merge || """
when not matched then
  insert (""" || dataform_columns_list || """) values (""" || dataform_columns_list || """)""");

-- Cleanup staging table
DROP TABLE IF EXISTS `staging_table_temp_test_uuid`;
END;

SELECT 'inc_post_op';
EXCEPTION WHEN ERROR THEN
  DROP TABLE IF EXISTS `project-id.default_dataset.customer_orders_df_temp_test_uuid_empty`;
  RAISE;
END;