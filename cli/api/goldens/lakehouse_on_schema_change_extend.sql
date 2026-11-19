SELECT 'inc_pre_op';
BEGIN

-- Declare variables for schema comparison and strategy execution.
DECLARE dataform_columns ARRAY<STRING>;
DECLARE temp_table_columns ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_added ARRAY<STRUCT<column_name STRING, data_type STRING>>;
DECLARE columns_removed ARRAY<STRING>;


-- Create empty table to extract schema of new query.
CREATE OR REPLACE TABLE `project-id.default_dataset.customer_orders_df_temp_test_uuid_empty`
OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)) AS (
  SELECT * FROM (SELECT 1 AS order_id, 2 AS customer_id, 'new' AS status) AS insertions LIMIT 0
);


-- Compare schemas
SET dataform_columns = ARRAY<STRING>['order_id', 'customer_id'];

SET temp_table_columns = (
  SELECT IFNULL(ARRAY_AGG(STRUCT(column_name, data_type)), [])
  FROM `project-id.default_dataset.INFORMATION_SCHEMA.COLUMNS`
  WHERE table_name = 'customer_orders_df_temp_test_uuid_empty'
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
    "ALTER TABLE `project-id.ecommerce_catalog.sales_dw.customer_orders` " ||
    (
      SELECT STRING_AGG(FORMAT("ADD COLUMN IF NOT EXISTS %s %s", column_info.column_name, column_info.data_type), ", ")
      FROM UNNEST(columns_added) AS column_info
    )
  );
END IF;



-- Cleanup temporary tables.
DROP TABLE IF EXISTS `project-id.default_dataset.customer_orders_df_temp_test_uuid_empty`;
    
EXCEPTION WHEN ERROR THEN
  DROP TABLE IF EXISTS `project-id.default_dataset.customer_orders_df_temp_test_uuid_empty`;
  RAISE;
END;
merge `project-id.ecommerce_catalog.sales_dw.customer_orders` DATAFORM_DEST
using (SELECT 1 AS order_id, 2 AS customer_id, 'new' AS status
) DATAFORM_SOURCE
on DATAFORM_DEST.order_id = DATAFORM_SOURCE.order_id 

when matched then
  update set `order_id` = DATAFORM_SOURCE.order_id,`customer_id` = DATAFORM_SOURCE.customer_id
when not matched then
  insert (`order_id`,`customer_id`) values (`order_id`,`customer_id`);
SELECT 'inc_post_op'