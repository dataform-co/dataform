import { fail } from "assert";
import { expect } from "chai";
import * as path from "path";

import { Builder, compile } from "df/api";
import { JSONObjectStringifier, StringifiedSet } from "df/common/strings/stringifier";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { cleanSql } from "df/tests/utils";

suite("examples", () => {
  suite("common_v2 bigquery", async () => {
    for (const useMain of [true, false]) {
      for (const databaseSuffix of ["", "foo"]) {
        for (const schemaSuffix of ["", "bar"]) {
          const databaseWithSuffix = (database: string) =>
            databaseSuffix ? `${database}_${databaseSuffix}` : database;
          const schemaWithSuffix = (schema: string) =>
            schemaSuffix ? `${schema}_${schemaSuffix}` : schema;

          test(`compiles with database suffix "${databaseSuffix}", schema suffix "${schemaSuffix}"`, async () => {
            const graph = await compile({
              projectDir: path.resolve("examples/common_v2"),
              projectConfigOverride: { schemaSuffix, databaseSuffix, warehouse: "bigquery" },
              useMain
            });
            expect(
              new StringifiedSet(
                new JSONObjectStringifier(),
                graph.graphErrors.compilationErrors.map(({ fileName, message }) => ({
                  fileName,
                  message
                }))
              )
            ).deep.equals(
              new StringifiedSet(new JSONObjectStringifier(), [
                {
                  fileName: "includes/example_ignore.js",
                  message: "publish is not defined"
                },
                {
                  fileName: "definitions/has_compile_errors/assertion_with_bigquery.sqlx",
                  message:
                    'Unexpected property "bigquery" in assertion config. Supported properties are: ["database","dependencies","description","disabled","hermetic","name","schema","tags","type"]'
                },
                {
                  fileName: "definitions/has_compile_errors/assertion_with_materialized.sqlx",
                  message:
                    'Unexpected property "materialized" in assertion config. Supported properties are: ["database","dependencies","description","disabled","hermetic","name","schema","tags","type"]'
                },
                {
                  fileName: "definitions/has_compile_errors/assertion_with_output.sqlx",
                  message:
                    'Unexpected property "hasOutput" in assertion config. Supported properties are: ["database","dependencies","description","disabled","hermetic","name","schema","tags","type"]'
                },
                {
                  fileName: "definitions/has_compile_errors/assertion_with_postops.sqlx",
                  message: "Actions may only include post_operations if they create a dataset."
                },
                {
                  fileName: "definitions/has_compile_errors/assertion_with_preops.sqlx",
                  message: "Actions may only include pre_operations if they create a dataset."
                },
                {
                  fileName: "definitions/has_compile_errors/assertion_with_redshift.sqlx",
                  message:
                    'Unexpected property "redshift" in assertion config. Supported properties are: ["database","dependencies","description","disabled","hermetic","name","schema","tags","type"]'
                },
                {
                  fileName: "definitions/has_compile_errors/protected_assertion.sqlx",
                  message:
                    "Actions may only specify 'protected: true' if they are of type 'incremental'."
                },
                {
                  fileName: "definitions/has_compile_errors/protected_assertion.sqlx",
                  message:
                    'Unexpected property "protected" in assertion config. Supported properties are: ["database","dependencies","description","disabled","hermetic","name","schema","tags","type"]'
                },
                {
                  fileName: "definitions/has_compile_errors/view_with_incremental.sqlx",
                  message:
                    "Actions may only include incremental_where if they are of type 'incremental'."
                },
                {
                  fileName: "definitions/has_compile_errors/view_with_multiple_statements.sqlx",
                  message:
                    "Actions may only contain more than one SQL statement if they are of type 'operations'."
                },
                {
                  fileName: "definitions/has_compile_errors/view_with_semi_colon_at_end.sqlx",
                  message: "Semi-colons are not allowed at the end of SQL statements."
                },
                {
                  fileName: "definitions/has_compile_errors/table_with_materialized.sqlx",
                  message:
                    "The 'materialized' option is only valid for Snowflake and BigQuery views"
                },
                {
                  fileName: "definitions/has_compile_errors/view_without_hermetic.sqlx",
                  message:
                    "Zero-dependency actions which create datasets are required to explicitly declare 'hermetic: (true|false)' when run caching is turned on."
                }
              ])
            );

            // Check JS blocks get processed.
            const exampleJsBlocks = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_js_blocks"
                )
            );
            expect(exampleJsBlocks.type).equals("table");
            expect(exampleJsBlocks.enumType).equals(dataform.TableType.TABLE);
            expect(exampleJsBlocks.query.trim()).equals("select 1 as foo");

            // Check we can import and use an external package.
            const exampleIncremental = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_incremental"
                )
            );
            expect(exampleIncremental.protected).eql(true);
            expect(exampleIncremental.query.trim()).equals("select current_timestamp() as ts");
            expect(exampleIncremental.where.trim()).equals(
              `ts > (select max(ts) from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "example_incremental"
              )}\`) or (select max(ts) from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "example_incremental"
              )}\`) is null`
            );

            const exampleIsIncremental = graph.tables.filter(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_is_incremental"
                )
            )[0];
            expect(cleanSql(exampleIsIncremental.query.trim())).equals(
              "select * from (select current_timestamp() as ts)"
            );
            expect(cleanSql(exampleIsIncremental.incrementalQuery)).equals(
              cleanSql(
                `select * from (select current_timestamp() as ts)
           where ts > (select max(ts) from \`${dotJoined(
             databaseWithSuffix("tada-analytics"),
             schemaWithSuffix("df_integration_test"),
             "example_is_incremental"
           )}\`) or (select max(ts) from \`${dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_is_incremental"
                )}\`) is null`
              )
            );

            expect(exampleIsIncremental.incrementalPreOps).to.eql(["\n    select 1\n"]);
            expect(exampleIsIncremental.incrementalPostOps).to.eql(["\n    select 15\n"]);

            // Check tables defined in includes are not included.
            const exampleIgnore = graph.tables.find(
              (t: dataform.ITable) => targetAsReadableString(t.target) === "example_ignore"
            );
            expect(exampleIgnore).equal(undefined);
            const exampleIgnore2 = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_ignore"
                )
            );
            expect(exampleIgnore2).equal(undefined);

            // Check SQL files with raw back-ticks get escaped.
            const exampleBackticks = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_backticks"
                )
            );
            expect(cleanSql(exampleBackticks.query)).equals(
              "select * from `tada-analytics.df_integration_test.sample_data`"
            );
            expect(exampleBackticks.preOps).to.eql([
              '\n    GRANT SELECT ON `tada-analytics.df_integration_test.sample_data` TO GROUP "allusers@dataform.co"\n'
            ]);
            expect(exampleBackticks.postOps).to.eql([]);

            // Check deferred calls to table resolve to the correct definitions file.
            const exampleDeferred = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_deferred"
                )
            );
            expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");

            // Check inline tables
            const exampleInline = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_inline"
                )
            );
            expect(exampleInline.type).equals("inline");
            expect(exampleInline.enumType).equals(dataform.TableType.INLINE);
            expect(exampleInline.query.trim()).equals(
              `select * from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "sample_data"
              )}\``
            );
            expect(exampleInline.dependencyTargets).eql([
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "sample_data"
              })
            ]);

            // Check view
            const exampleView = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_view"
                )
            );
            expect(exampleView.type).equals("view");
            expect(exampleView.enumType).equals(dataform.TableType.VIEW);
            expect(exampleView.query.trim()).equals(
              `select * from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "sample_data"
              )}\`\n` +
                `inner join select * from \`${dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("override_schema"),
                  "override_schema_example"
                )}\`\n` +
                `inner join select * from \`${dotJoined(
                  databaseWithSuffix("override_database"),
                  schemaWithSuffix("df_integration_test"),
                  "override_database_example"
                )}\``
            );
            expect(exampleView.target).deep.equals(
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "example_view"
              })
            );
            expect(exampleView.canonicalTarget).deep.equals(
              dataform.Target.create({
                database: "tada-analytics",
                schema: "df_integration_test",
                name: "example_view"
              })
            );
            expect(exampleView.dependencyTargets).eql([
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "sample_data"
              }),
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("override_schema"),
                name: "override_schema_example"
              }),
              dataform.Target.create({
                database: databaseWithSuffix("override_database"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "override_database_example"
              })
            ]);
            expect(exampleView.tags).to.eql([]);

            // Check materialized view
            const exampleMaterializedView = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_materialized_view"
                )
            );
            expect(exampleMaterializedView.type).equals("view");
            expect(exampleMaterializedView.enumType).equals(dataform.TableType.VIEW);
            expect(exampleMaterializedView.materialized).equals(true);
            expect(exampleMaterializedView.query.trim()).equals(
              `select * from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "sample_data"
              )}\`\n` + `group by 1`
            );
            expect(exampleMaterializedView.target).deep.equals(
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "example_materialized_view"
              })
            );
            expect(exampleMaterializedView.canonicalTarget).deep.equals(
              dataform.Target.create({
                database: "tada-analytics",
                schema: "df_integration_test",
                name: "example_materialized_view"
              })
            );
            expect(exampleMaterializedView.dependencyTargets).eql([
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "sample_data"
              })
            ]);
            expect(exampleMaterializedView.tags).to.eql([]);

            // Check table
            const exampleTable = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_table"
                )
            );
            expect(exampleTable.type).equals("table");
            expect(exampleTable.enumType).equals(dataform.TableType.TABLE);
            expect(exampleTable.query.trim()).equals(
              `select * from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "sample_data"
              )}\`\n\n-- here \${"is"} a \`comment\n\n/* \${"another"} \` backtick \` containing \`\`\`comment */`
            );
            expect(exampleTable.dependencyTargets).eql([
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "sample_data"
              })
            ]);
            expect(exampleTable.preOps).to.eql([]);
            expect(exampleTable.postOps).to.eql([
              `\n    GRANT SELECT ON \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "example_table"
              )}\` TO GROUP "allusers@dataform.co"\n`,
              `\n    GRANT SELECT ON \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "example_table"
              )}\` TO GROUP "otherusers@dataform.co"\n`
            ]);
            expect(exampleTable.tags).to.eql([]);

            // Check Table with tags
            const exampleTableWithTags = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_table_with_tags"
                )
            );
            expect(exampleTableWithTags.disabled).eql(true);
            expect(exampleTableWithTags.tags).to.eql(["tag1", "tag2", "tag3"]);

            // Check table-with-tags's unique key assertion
            const exampleTableWithTagsUniqueKeyAssertion = graph.assertions.filter(
              t =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test_assertions"),
                  "df_integration_test_example_table_with_tags_assertions_uniqueKey_0"
                )
            )[0];
            expect(exampleTableWithTagsUniqueKeyAssertion.disabled).eql(true);
            expect(cleanSql(exampleTableWithTagsUniqueKeyAssertion.query)).equals(
              `select * from (select sample, count(1) as index_row_count from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "example_table_with_tags"
              )}\` group by sample) as data where index_row_count > 1`
            );
            expect(exampleTableWithTagsUniqueKeyAssertion.dependencyTargets).eql([
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "example_table_with_tags"
              })
            ]);
            expect(exampleTableWithTagsUniqueKeyAssertion.tags).eql(["tag1", "tag2", "tag3"]);

            // Check table-with-tags's row conditions assertion
            const exampleTableWithTagsRowConditionsAssertion = graph.assertions.filter(
              t =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test_assertions"),
                  "df_integration_test_example_table_with_tags_assertions_rowConditions"
                )
            )[0];
            expect(exampleTableWithTagsRowConditionsAssertion.disabled).eql(true);
            expect(cleanSql(exampleTableWithTagsRowConditionsAssertion.query)).equals(
              `select 'sample is not null' as failing_row_condition, * from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "example_table_with_tags"
              )}\` where not (sample is not null)`
            );
            expect(exampleTableWithTagsRowConditionsAssertion.dependencyTargets).eql([
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "example_table_with_tags"
              })
            ]);

            // Check sample data
            const exampleSampleData = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "sample_data"
                )
            );
            expect(exampleSampleData.type).equals("view");
            expect(exampleSampleData.enumType).equals(dataform.TableType.VIEW);
            expect(exampleSampleData.query.trim()).equals(
              "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
            );
            expect(exampleSampleData.preOps).eql([]);
            expect(exampleSampleData.dependencyTargets).eql([]);
            expect(exampleSampleData.actionDescriptor).to.eql(
              dataform.ActionDescriptor.create({
                description: "This is some sample data.",
                columns: [
                  dataform.ColumnDescriptor.create({
                    description: "Sample integers.",
                    path: ["sample"]
                  })
                ]
              })
            );

            // Check database override defined in "config {}".
            const exampleUsingOverriddenDatabase = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("override_database"),
                  schemaWithSuffix("df_integration_test"),
                  "override_database_example"
                )
            );

            expect(exampleUsingOverriddenDatabase.target.database).equals(
              databaseWithSuffix("override_database")
            );
            expect(exampleUsingOverriddenDatabase.type).equals("view");
            expect(exampleUsingOverriddenDatabase.enumType).equals(dataform.TableType.VIEW);
            expect(exampleUsingOverriddenDatabase.query.trim()).equals(
              "select 1 as test_database_override"
            );

            // Check schema overrides defined in "config {}"
            const exampleUsingOverriddenSchema = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("override_schema"),
                  "override_schema_example"
                )
            );

            expect(exampleUsingOverriddenSchema.target.schema).equals(
              schemaWithSuffix("override_schema")
            );
            expect(exampleUsingOverriddenSchema.type).equals("view");
            expect(exampleUsingOverriddenSchema.enumType).equals(dataform.TableType.VIEW);
            expect(exampleUsingOverriddenSchema.query.trim()).equals(
              "select 1 as test_schema_override"
            );

            // Check schema overrides defined in "config {}" -- case with schema unchanged
            const exampleUsingOverriddenSchemaUnchanged = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "override_schema_example_unchanged"
                )
            );

            expect(exampleUsingOverriddenSchemaUnchanged.target.schema).equals(
              schemaWithSuffix("df_integration_test")
            );
            expect(exampleUsingOverriddenSchemaUnchanged.type).equals("view");
            expect(exampleUsingOverriddenSchemaUnchanged.enumType).equals(dataform.TableType.VIEW);
            expect(exampleUsingOverriddenSchemaUnchanged.query.trim()).equals(
              "select 1 as test_schema_override"
            );

            // Check assertion
            const exampleAssertion = graph.assertions.find(
              (a: dataform.IAssertion) =>
                targetAsReadableString(a.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("hi_there"),
                  "example_assertion"
                )
            );
            expect(exampleAssertion.target.schema).equals(schemaWithSuffix("hi_there"));
            expect(exampleAssertion.query.trim()).equals(
              `select * from \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "sample_data"
              )}\` where sample = 100`
            );
            expect(exampleAssertion.dependencyTargets).eql([
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "sample_data"
              })
            ]);
            expect(exampleAssertion.tags).to.eql([]);
            expect(exampleAssertion.actionDescriptor).to.eql(
              dataform.ActionDescriptor.create({
                description: "An example assertion looking for incorrect 'sample' values."
              })
            );

            // Check Assertion with tags
            const exampleAssertionWithTags = graph.assertions.find(
              (a: dataform.IAssertion) =>
                targetAsReadableString(a.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test_assertions"),
                  "example_assertion_with_tags"
                )
            );
            expect(exampleAssertionWithTags.target.schema).equals(
              schemaWithSuffix("df_integration_test_assertions")
            );
            expect(exampleAssertionWithTags.tags).to.eql(["tag1", "tag2"]);

            // Check example operations file
            const exampleOperations = graph.operations.find(
              (o: dataform.IOperation) =>
                targetAsReadableString(o.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_operations"
                )
            );
            expect(exampleOperations.hasOutput).equals(false);
            expect(exampleOperations.queries).to.eql([
              "\n\nCREATE OR REPLACE VIEW someschema.someview AS (SELECT 1 AS test)\n",
              `\nDROP VIEW IF EXISTS \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("override_schema"),
                "override_schema_example"
              )}\`\n`,
              `\nDROP VIEW IF EXISTS \`${dotJoined(
                databaseWithSuffix("override_database"),
                schemaWithSuffix("df_integration_test"),
                "override_database_example"
              )}\`\n`
            ]);
            expect(exampleOperations.dependencyTargets).eql([
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("override_schema"),
                name: "override_schema_example"
              }),
              dataform.Target.create({
                database: databaseWithSuffix("override_database"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "override_database_example"
              }),
              dataform.Target.create({
                database: databaseWithSuffix("tada-analytics"),
                schema: schemaWithSuffix("df_integration_test"),
                name: "sample_data"
              })
            ]);
            expect(exampleOperations.tags).to.eql([]);

            // Check example operation with output.
            const exampleOperationWithOutput = graph.operations.find(
              (o: dataform.IOperation) =>
                targetAsReadableString(o.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_operation_with_output"
                )
            );
            expect(exampleOperationWithOutput.target.schema).equals(
              schemaWithSuffix("df_integration_test")
            );
            expect(exampleOperationWithOutput.target.name).equals("example_operation_with_output");
            expect(exampleOperationWithOutput.queries).to.eql([
              `\nCREATE OR REPLACE VIEW \`${dotJoined(
                databaseWithSuffix("tada-analytics"),
                schemaWithSuffix("df_integration_test"),
                "example_operation_with_output"
              )}\` AS (SELECT * FROM \`some_database_name.some_external_schema_name.very_important_external_table\`)`
            ]);
            expect(exampleOperationWithOutput.dependencyTargets).eql([
              dataform.Target.create({
                database: "some_database_name",
                schema: "some_external_schema_name",
                name: "very_important_external_table"
              })
            ]);
            expect(exampleOperationWithOutput.actionDescriptor).to.eql(
              dataform.ActionDescriptor.create({
                description: "An example operations file which outputs a dataset.",
                columns: [
                  dataform.ColumnDescriptor.create({
                    description: "Just 1!",
                    path: ["TEST"]
                  })
                ]
              })
            );

            // Check Operation with tags
            const exampleOperationsWithTags = graph.operations.find(
              (o: dataform.IOperation) =>
                targetAsReadableString(o.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_operations_with_tags"
                )
            );
            expect(exampleOperationsWithTags.tags).to.eql(["tag1"]);

            // Check declaration.
            const exampleDeclaration = graph.declarations.find(
              d =>
                targetAsReadableString(d.target) ===
                "some_database_name.some_external_schema_name.very_important_external_table"
            );
            expect(exampleDeclaration.target).eql(
              dataform.Target.create({
                database: "some_database_name",
                schema: "some_external_schema_name",
                name: "very_important_external_table"
              })
            );
            expect(exampleDeclaration.actionDescriptor.description).to.equal(
              "This table is not generated by Dataform!"
            );

            // Check testcases.
            const testCase = graph.tests.find(t => t.name === "example_test_case");
            expect(testCase.testQuery.trim()).equals(
              "select * from (\n    select 'hi' as faked union all\n    select 'ben' as faked union all\n    select 'sup?' as faked\n)\n\n-- here ${\"is\"} a `comment\n\n/* ${\"another\"} ` backtick ` containing ```comment */"
            );
            expect(testCase.expectedOutputQuery.trim()).equals(
              "select 'hi' as faked union all\nselect 'ben' as faked union all\nselect 'sup?' as faked"
            );

            const testCaseFQ = graph.tests.find(t => t.name === "example_test_case_fq_ref");
            expect(testCaseFQ.testQuery.trim()).equals(
              "select * from (\n    select 'hi' as faked union all\n    select 'ben' as faked union all\n    select 'sup?' as faked\n)\n\n-- here ${\"is\"} a `comment\n\n/* ${\"another\"} ` backtick ` containing ```comment */"
            );
            expect(testCaseFQ.expectedOutputQuery.trim()).equals(
              "select 'hi' as faked union all\nselect 'ben' as faked union all\nselect 'sup?' as faked"
            );

            // Check double backslashes don't get converted to singular.
            const exampleDoubleBackslash = graph.tables.find(
              (t: dataform.ITable) =>
                targetAsReadableString(t.target) ===
                dotJoined(
                  databaseWithSuffix("tada-analytics"),
                  schemaWithSuffix("df_integration_test"),
                  "example_double_backslash"
                )
            );
            expect(cleanSql(exampleDoubleBackslash.query)).equals(
              "select * from regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)') select * from regexp_extract('01a_data_engine', r'^(\\d{2}\\w)')"
            );
            expect(cleanSql(exampleDoubleBackslash.preOps[0])).equals(
              "select * from regexp_extract('\\\\\\\\', '\\\\')"
            );
          });
        }
      }
    }
  });

  test("backwards_compatibility", async () => {
    const graph = await compile({ projectDir: "examples/backwards_compatibility" });

    const tableNames = graph.tables.map((t: dataform.ITable) => t.target.name);

    // Make sure it compiles.
    expect(tableNames).includes("example");
    const example = graph.tables.filter((t: dataform.ITable) => t.target.name === "example")[0];
    expect(example.type).equals("table");
    expect(example.query.trim()).equals("select 1 as foo_bar");

    // Make sure we can dry run.
    new Builder(graph, {}, { tables: [] }).build();
  });

  test("times out after timeout period during compilation", async () => {
    try {
      await compile({ projectDir: "examples/never_finishes_compiling" });
      fail("Compilation timeout Error expected.");
    } catch (e) {
      expect(e.message).to.equal("Compilation timed out");
    }
  });

  test("invalid dataform json throws error", async () => {
    try {
      await compile({
        projectDir: path.resolve("examples/invalid_dataform_json")
      });
      fail("Should have failed.");
    } catch (e) {
      // OK
    }
  });

  test("version is correctly set", async () => {
    const graph = await compile({
      projectDir: "examples/common_v2",
      projectConfigOverride: { warehouse: "bigquery" }
    });
    const { version: expectedVersion } = require("df/core/version");
    expect(graph.dataformCoreVersion).equals(expectedVersion);
  });
});

function dotJoined(...strings: string[]) {
  return strings.join(".");
}
