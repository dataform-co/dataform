import { fail } from "assert";
import { expect } from "chai";
import * as path from "path";

import { Builder, compile } from "df/api";
import { computeAllTransitiveInputs } from "df/api/commands/build";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { cleanSql } from "df/tests/utils";

suite("examples", () => {
  suite("common_v2 bigquery", async () => {
    for (const schemaSuffix of ["", "suffix"]) {
      const schemaWithSuffix = (schema: string) =>
        schemaSuffix ? `${schema}_${schemaSuffix}` : schema;

      test(`compiles with suffix "${schemaSuffix}"`, async () => {
        const graph = await compile({
          projectDir: path.resolve("examples/common_v2"),
          projectConfigOverride: { schemaSuffix, warehouse: "bigquery" }
        });
        expect(
          graph.graphErrors.compilationErrors.map(({ fileName, message }) => ({
            fileName,
            message
          }))
        ).deep.equals([
          {
            fileName: "definitions/has_compile_errors/assertion_with_bigquery.sqlx",
            message: "Actions may only specify 'bigquery: { ... }' if they create a dataset."
          },
          {
            fileName: "definitions/has_compile_errors/assertion_with_bigquery.sqlx",
            message:
              'Unexpected property "bigquery" in assertion config. Supported properties are: ["database","dependencies","description","disabled","hermetic","name","schema","tags","type"]'
          },
          {
            fileName: "definitions/has_compile_errors/assertion_with_output.sqlx",
            message:
              "Actions may only specify 'hasOutput: true' if they are of type 'operations' or create a dataset."
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
            message: "Actions may only specify 'redshift: { ... }' if they create a dataset."
          },
          {
            fileName: "definitions/has_compile_errors/assertion_with_redshift.sqlx",
            message:
              'Unexpected property "redshift" in assertion config. Supported properties are: ["database","dependencies","description","disabled","hermetic","name","schema","tags","type"]'
          },
          {
            fileName: "definitions/has_compile_errors/disabled_assertion.sqlx",
            message: "Actions may only specify 'disabled: true' if they create a dataset."
          },
          {
            fileName: "definitions/has_compile_errors/protected_assertion.sqlx",
            message: "Actions may only specify 'protected: true' if they are of type 'incremental'."
          },
          {
            fileName: "definitions/has_compile_errors/protected_assertion.sqlx",
            message:
              'Unexpected property "protected" in assertion config. Supported properties are: ["database","dependencies","description","disabled","hermetic","name","schema","tags","type"]'
          },
          {
            fileName: "definitions/has_compile_errors/view_with_incremental.sqlx",
            message: "Actions may only include incremental_where if they are of type 'incremental'."
          },
          {
            fileName: "definitions/has_compile_errors/view_with_multiple_statements.sqlx",
            message:
              "Actions may only contain more than one SQL statement if they are of type 'operations'."
          },
          {
            fileName: "definitions/has_compile_errors/view_without_hermetic.sqlx",
            message:
              "Zero-dependency actions which create datasets are required to explicitly declare 'hermetic: (true|false)' when run caching is turned on."
          }
        ]);

        // Check JS blocks get processed.
        const exampleJsBlocks = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_js_blocks"
        );
        expect(exampleJsBlocks.type).equals("table");
        expect(exampleJsBlocks.query.trim()).equals("select 1 as foo");

        // Check we can import and use an external package.
        const exampleIncremental = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_incremental"
        );
        expect(exampleIncremental.protected).eql(true);
        expect(exampleIncremental.query.trim()).equals("select current_timestamp() as ts");
        expect(exampleIncremental.where.trim()).equals(
          `ts > (select max(ts) from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_incremental\`) or (select max(ts) from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_incremental\`) is null`
        );

        const exampleIsIncremental = graph.tables.filter(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_is_incremental"
        )[0];
        expect(cleanSql(exampleIsIncremental.query.trim())).equals(
          "select * from (select current_timestamp() as ts)"
        );
        expect(cleanSql(exampleIsIncremental.incrementalQuery)).equals(
          cleanSql(
            `select * from (select current_timestamp() as ts)
           where ts > (select max(ts) from \`tada-analytics.${schemaWithSuffix(
             "df_integration_test"
           )}.example_is_incremental\`) or (select max(ts) from \`tada-analytics.${schemaWithSuffix(
              "df_integration_test"
            )}.example_is_incremental\`) is null`
          )
        );

        expect(exampleIsIncremental.incrementalPreOps).to.eql(["\n    select 1\n"]);
        expect(exampleIsIncremental.incrementalPostOps).to.eql(["\n    select 15\n"]);

        // Check tables defined in includes are not included.
        const exampleIgnore = graph.tables.find(
          (t: dataform.ITable) => t.name === "example_ignore"
        );
        expect(exampleIgnore).equal(undefined);
        const exampleIgnore2 = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_ignore"
        );
        expect(exampleIgnore2).equal(undefined);

        // Check SQL files with raw back-ticks get escaped.
        const exampleBackticks = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_backticks"
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
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_deferred"
        );
        expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");

        // Check inline tables
        const exampleInline = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_inline"
        );
        expect(exampleInline.type).equals("inline");
        expect(exampleInline.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix("df_integration_test")}.sample_data\``
        );
        expect(exampleInline.dependencies).eql([
          "tada-analytics." + schemaWithSuffix("df_integration_test") + ".sample_data"
        ]);
        expect(exampleInline.dependencyTargets).eql([
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("df_integration_test"),
            name: "sample_data"
          })
        ]);

        const exampleUsingInline = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_using_inline"
        );
        expect(exampleUsingInline.type).equals("table");
        expect(exampleUsingInline.query.trim()).equals(
          `select * from (\n\nselect * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\`\n)\nwhere true`
        );
        expect(exampleUsingInline.dependencies).eql([
          "tada-analytics." + schemaWithSuffix("df_integration_test") + ".sample_data"
        ]);
        expect(exampleUsingInline.dependencyTargets).eql([
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("df_integration_test"),
            name: "sample_data"
          })
        ]);

        // Check view
        const exampleView = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_view"
        );
        expect(exampleView.type).equals("view");
        expect(exampleView.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\`\n` +
            `inner join select * from \`tada-analytics.${schemaWithSuffix(
              "override_schema"
            )}.override_schema_example\`\n` +
            `inner join select * from \`override_database.${schemaWithSuffix(
              "df_integration_test"
            )}.override_database_example\``
        );
        expect(exampleView.target).deep.equals(
          dataform.Target.create({
            name: "example_view",
            schema: schemaWithSuffix("df_integration_test"),
            database: "tada-analytics"
          })
        );
        expect(exampleView.canonicalTarget).deep.equals(
          dataform.Target.create({
            name: "example_view",
            schema: "df_integration_test",
            database: "tada-analytics"
          })
        );
        expect(exampleView.dependencies).deep.equals([
          "tada-analytics." + schemaWithSuffix("df_integration_test") + ".sample_data",
          "tada-analytics." + schemaWithSuffix("override_schema") + ".override_schema_example",
          "override_database." +
            schemaWithSuffix("df_integration_test") +
            ".override_database_example"
        ]);
        expect(exampleView.dependencyTargets).eql([
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("df_integration_test"),
            name: "sample_data"
          }),
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("override_schema"),
            name: "override_schema_example"
          }),
          dataform.Target.create({
            database: "override_database",
            schema: schemaWithSuffix("df_integration_test"),
            name: "override_database_example"
          })
        ]);
        expect(exampleView.tags).to.eql([]);

        // Check table
        const exampleTable = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_table"
        );
        expect(exampleTable.type).equals("table");
        expect(exampleTable.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\`\n\n-- here \${"is"} a \`comment\n\n/* \${"another"} \` backtick \` containing \`\`\`comment */`
        );
        expect(exampleTable.dependencies).deep.equals([
          "tada-analytics." + schemaWithSuffix("df_integration_test") + ".sample_data"
        ]);
        expect(exampleTable.dependencyTargets).eql([
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("df_integration_test"),
            name: "sample_data"
          })
        ]);
        expect(exampleTable.preOps).to.eql([]);
        expect(exampleTable.postOps).to.eql([
          `\n    GRANT SELECT ON \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_table\` TO GROUP "allusers@dataform.co"\n`,
          `\n    GRANT SELECT ON \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_table\` TO GROUP "otherusers@dataform.co"\n`
        ]);
        expect(exampleTable.tags).to.eql([]);

        // Check Table with tags
        const exampleTableWithTags = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_table_with_tags"
        );
        expect(exampleTableWithTags.tags).to.eql(["tag1", "tag2", "tag3"]);

        // Check table-with-tags's unique key assertion
        const exampleTableWithTagsUniqueKeyAssertion = graph.assertions.filter(
          t =>
            t.name ===
            "tada-analytics." +
              schemaWithSuffix("df_integration_test_assertions") +
              ".example_table_with_tags_assertions_uniqueKey"
        )[0];
        expect(cleanSql(exampleTableWithTagsUniqueKeyAssertion.query)).equals(
          "select * from (select sample, count(1) as index_row_count from `tada-analytics." +
            schemaWithSuffix("df_integration_test") +
            ".example_table_with_tags` group by sample) as data where index_row_count > 1"
        );
        expect(exampleTableWithTagsUniqueKeyAssertion.dependencies).deep.equals([
          "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_table_with_tags"
        ]);
        expect(exampleTableWithTagsUniqueKeyAssertion.dependencyTargets).eql([
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("df_integration_test"),
            name: "example_table_with_tags"
          })
        ]);
        expect(exampleTableWithTagsUniqueKeyAssertion.tags).eql(["tag1", "tag2", "tag3"]);

        // Check table-with-tags's row conditions assertion
        const exampleTableWithTagsRowConditionsAssertion = graph.assertions.filter(
          t =>
            t.name ===
            "tada-analytics." +
              schemaWithSuffix("df_integration_test_assertions") +
              ".example_table_with_tags_assertions_rowConditions"
        )[0];
        expect(cleanSql(exampleTableWithTagsRowConditionsAssertion.query)).equals(
          "select 'sample is not null' as failing_row_condition, * from `tada-analytics." +
            schemaWithSuffix("df_integration_test") +
            ".example_table_with_tags` where not (sample is not null)"
        );
        expect(exampleTableWithTagsRowConditionsAssertion.dependencies).deep.equals([
          "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_table_with_tags"
        ]);
        expect(exampleTableWithTagsRowConditionsAssertion.dependencyTargets).eql([
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("df_integration_test"),
            name: "example_table_with_tags"
          })
        ]);
        expect(exampleTableWithTagsRowConditionsAssertion.tags).eql(["tag1", "tag2", "tag3"]);

        // Check sample data
        const exampleSampleData = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === "tada-analytics." + schemaWithSuffix("df_integration_test") + ".sample_data"
        );
        expect(exampleSampleData.type).equals("view");
        expect(exampleSampleData.query.trim()).equals(
          "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
        );
        expect(exampleSampleData.dependencies).to.eql([]);
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
            t.name ===
            "override_database." +
              schemaWithSuffix("df_integration_test") +
              ".override_database_example"
        );

        expect(exampleUsingOverriddenDatabase.target.database).equals("override_database");
        expect(exampleUsingOverriddenDatabase.type).equals("view");
        expect(exampleUsingOverriddenDatabase.query.trim()).equals(
          "select 1 as test_database_override"
        );

        // Check schema overrides defined in "config {}"
        const exampleUsingOverriddenSchema = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." + schemaWithSuffix("override_schema") + ".override_schema_example"
        );

        expect(exampleUsingOverriddenSchema.target.schema).equals(
          schemaWithSuffix("override_schema")
        );
        expect(exampleUsingOverriddenSchema.type).equals("view");
        expect(exampleUsingOverriddenSchema.query.trim()).equals(
          "select 1 as test_schema_override"
        );

        // Check schema overrides defined in "config {}" -- case with schema unchanged
        const exampleUsingOverriddenSchemaUnchanged = graph.tables.find(
          (t: dataform.ITable) =>
            t.name ===
            "tada-analytics." +
              schemaWithSuffix("df_integration_test") +
              ".override_schema_example_unchanged"
        );

        expect(exampleUsingOverriddenSchemaUnchanged.target.schema).equals(
          schemaWithSuffix("df_integration_test")
        );
        expect(exampleUsingOverriddenSchemaUnchanged.type).equals("view");
        expect(exampleUsingOverriddenSchemaUnchanged.query.trim()).equals(
          "select 1 as test_schema_override"
        );

        // Check assertion
        const exampleAssertion = graph.assertions.find(
          (a: dataform.IAssertion) =>
            a.name === "tada-analytics." + schemaWithSuffix("hi_there") + ".example_assertion"
        );
        expect(exampleAssertion.target.schema).equals(schemaWithSuffix("hi_there"));
        expect(exampleAssertion.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\` where sample = 100`
        );
        expect(exampleAssertion.dependencies).to.eql([
          "tada-analytics." + schemaWithSuffix("df_integration_test") + ".sample_data"
        ]);
        expect(exampleAssertion.dependencyTargets).eql([
          dataform.Target.create({
            database: "tada-analytics",
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
            a.name ===
            "tada-analytics." +
              schemaWithSuffix("df_integration_test_assertions") +
              ".example_assertion_with_tags"
        );
        expect(exampleAssertionWithTags.target.schema).equals(
          schemaWithSuffix("df_integration_test_assertions")
        );
        expect(exampleAssertionWithTags.tags).to.eql(["tag1", "tag2"]);

        // Check example operations file
        const exampleOperations = graph.operations.find(
          (o: dataform.IOperation) =>
            o.name ===
            "tada-analytics." + schemaWithSuffix("df_integration_test") + ".example_operations"
        );
        expect(exampleOperations.hasOutput).equals(false);
        expect(exampleOperations.queries).to.eql([
          "\n\nCREATE OR REPLACE VIEW someschema.someview AS (SELECT 1 AS test)\n",
          `\nDROP VIEW IF EXISTS \`tada-analytics.${schemaWithSuffix(
            "override_schema"
          )}.override_schema_example\`\n`,
          `\nDROP VIEW IF EXISTS \`override_database.${schemaWithSuffix(
            "df_integration_test"
          )}.override_database_example\`\n`
        ]);
        expect(exampleOperations.dependencies).to.eql([
          "tada-analytics." + schemaWithSuffix("override_schema") + ".override_schema_example",
          "override_database." +
            schemaWithSuffix("df_integration_test") +
            ".override_database_example",
          "tada-analytics." + schemaWithSuffix("df_integration_test") + ".sample_data"
        ]);
        expect(exampleOperations.dependencyTargets).eql([
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("override_schema"),
            name: "override_schema_example"
          }),
          dataform.Target.create({
            database: "override_database",
            schema: schemaWithSuffix("df_integration_test"),
            name: "override_database_example"
          }),
          dataform.Target.create({
            database: "tada-analytics",
            schema: schemaWithSuffix("df_integration_test"),
            name: "sample_data"
          })
        ]);
        expect(exampleOperations.tags).to.eql([]);

        // Check example operation with output.
        const exampleOperationWithOutput = graph.operations.find(
          (o: dataform.IOperation) =>
            o.name ===
            "tada-analytics." +
              schemaWithSuffix("df_integration_test") +
              ".example_operation_with_output"
        );
        expect(exampleOperationWithOutput.target.schema).equals(
          schemaWithSuffix("df_integration_test")
        );
        expect(exampleOperationWithOutput.target.name).equals("example_operation_with_output");
        expect(exampleOperationWithOutput.queries).to.eql([
          `\nCREATE OR REPLACE VIEW \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_operation_with_output\` AS (SELECT * FROM \`some_database_name.some_external_schema_name.very_important_external_table\`)`
        ]);
        expect(exampleOperationWithOutput.dependencies).to.eql([
          "some_database_name.some_external_schema_name.very_important_external_table"
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
            o.name ===
            "tada-analytics." +
              schemaWithSuffix("df_integration_test") +
              ".example_operations_with_tags"
        );
        expect(exampleOperationsWithTags.tags).to.eql(["tag1"]);

        // Check declaration.
        const exampleDeclaration = graph.declarations.find(
          d =>
            d.name === "some_database_name.some_external_schema_name.very_important_external_table"
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
            t.name ===
            "tada-analytics." +
              schemaWithSuffix("df_integration_test") +
              ".example_double_backslash"
        );
        expect(cleanSql(exampleDoubleBackslash.query)).equals(
          "select * from regexp_extract('01a_data_engine', '^(\\\\d{2}\\\\w)') select * from regexp_extract('01a_data_engine', r'^(\\d{2}\\w)')"
        );
        expect(cleanSql(exampleDoubleBackslash.preOps[0])).equals(
          "select * from regexp_extract('\\\\\\\\', '\\\\')"
        );
      });
    }
  });

  suite("common_v1", async () => {
    test("bigquery compiles", async () => {
      const graph = await compile({
        projectDir: path.resolve("examples/common_v1"),
        projectConfigOverride: { warehouse: "bigquery", defaultDatabase: "tada-analytics" }
      });
      const tableNames = graph.tables.map((t: dataform.ITable) => t.name);

      expect(graph.graphErrors).to.eql(dataform.GraphErrors.create());

      // Check JS blocks get processed.
      expect(tableNames).includes("tada-analytics.df_integration_test.example_js_blocks");
      const exampleJsBlocks = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.example_js_blocks"
      )[0];
      expect(exampleJsBlocks.type).equals("table");
      expect(exampleJsBlocks.query).equals("select 1 as foo");

      // Check we can import and use an external package.
      expect(tableNames).includes("tada-analytics.df_integration_test.example_incremental");
      const exampleIncremental = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.example_incremental"
      )[0];
      expect(exampleIncremental.query).equals("select current_timestamp() as ts");
      expect(exampleIncremental.where.trim()).equals(
        "ts > (select max(ts) from `tada-analytics.df_integration_test.example_incremental`) or (select max(ts) from `tada-analytics.df_integration_test.example_incremental`) is null"
      );

      // Check tables defined in includes are not included.
      expect(tableNames).not.includes("example_ignore");

      // Check SQL files with raw back-ticks get escaped.
      expect(tableNames).includes("tada-analytics.df_integration_test.example_backticks");
      const exampleBackticks = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.example_backticks"
      )[0];
      expect(cleanSql(exampleBackticks.query)).equals(
        "select * from `tada-analytics.df_integration_test.sample_data`"
      );

      // Check deferred calls to table resolve to the correct definitions file.
      expect(tableNames).includes("tada-analytics.df_integration_test.example_deferred");
      const exampleDeferred = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.example_deferred"
      )[0];
      expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");

      // Check inline tables
      expect(tableNames).includes("tada-analytics.df_integration_test.example_inline");
      const exampleInline = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.example_inline"
      )[0];
      expect(exampleInline.type).equals("inline");
      expect(exampleInline.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleInline.dependencies).eql(["tada-analytics.df_integration_test.sample_data"]);
      expect(exampleInline.dependencyTargets).eql([
        dataform.Target.create({
          database: "tada-analytics",
          schema: "df_integration_test",
          name: "sample_data"
        })
      ]);

      expect(tableNames).includes("tada-analytics.df_integration_test.example_using_inline");
      const exampleUsingInline = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.example_using_inline"
      )[0];
      expect(exampleUsingInline.type).equals("table");
      expect(exampleUsingInline.query).equals(
        "\nselect * from (\nselect * from `tada-analytics.df_integration_test.sample_data`)\nwhere true"
      );
      expect(exampleUsingInline.dependencies).eql([
        "tada-analytics.df_integration_test.sample_data"
      ]);
      expect(exampleUsingInline.dependencyTargets).eql([
        dataform.Target.create({
          database: "tada-analytics",
          schema: "df_integration_test",
          name: "sample_data"
        })
      ]);

      // Check view
      expect(tableNames).includes("tada-analytics.df_integration_test.example_view");
      const exampleView = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.example_view"
      )[0];
      expect(exampleView.type).equals("view");
      expect(exampleView.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleView.dependencies).deep.equals([
        "tada-analytics.df_integration_test.sample_data"
      ]);
      expect(exampleView.dependencyTargets).eql([
        dataform.Target.create({
          database: "tada-analytics",
          schema: "df_integration_test",
          name: "sample_data"
        })
      ]);

      // Check table
      expect(tableNames).includes("tada-analytics.df_integration_test.example_table");
      const exampleTable = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.example_table"
      )[0];
      expect(exampleTable.type).equals("table");
      expect(exampleTable.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleTable.dependencies).deep.equals([
        "tada-analytics.df_integration_test.sample_data"
      ]);
      expect(exampleTable.dependencyTargets).eql([
        dataform.Target.create({
          database: "tada-analytics",
          schema: "df_integration_test",
          name: "sample_data"
        })
      ]);

      // Check sample data
      expect(tableNames).includes("tada-analytics.df_integration_test.sample_data");
      const exampleSampleData = graph.tables.filter(
        (t: dataform.ITable) => t.name === "tada-analytics.df_integration_test.sample_data"
      )[0];
      expect(exampleSampleData.type).equals("view");
      expect(exampleSampleData.query).equals(
        "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
      );
      expect(exampleSampleData.dependencies).to.eql([]);
      expect(exampleSampleData.dependencyTargets).eql([]);
    });

    test("bigquery compiles with schema override", async () => {
      const graph = await compile({
        projectDir: path.resolve("examples/common_v1"),
        schemaSuffixOverride: "suffix",
        projectConfigOverride: {
          warehouse: "redshift"
        }
      });
      expect(graph.projectConfig.schemaSuffix).to.equal("suffix");
      graph.tables.forEach(table =>
        expect(table.target.schema).to.match(
          /^(df_integration_test_suffix|override_schema_suffix)$/
        )
      );
    });

    test("bigquery compiles with database override", async () => {
      const graph = await compile({
        projectDir: path.resolve("examples/common_v2"),
        projectConfigOverride: {
          warehouse: "bigquery",
          defaultDatabase: "overridden-database"
        }
      });
      expect(graph.projectConfig.defaultDatabase).to.equal("overridden-database");
      const exampleTable = graph.tables.find(
        table => table.name === "overridden-database.df_integration_test.example_table"
      );
      expect(exampleTable.target.database).equals("overridden-database");
    });

    test("redshift compiles", () => {
      return compile({
        projectDir: "examples/common_v1",
        projectConfigOverride: { warehouse: "redshift" }
      }).then(graph => {
        const tableNames = graph.tables.map((t: dataform.ITable) => t.name);

        // Check we can import and use an external package.
        expect(tableNames).includes("df_integration_test.example_incremental");
        const exampleIncremental = graph.tables.filter(
          (t: dataform.ITable) => t.name === "df_integration_test.example_incremental"
        )[0];
        expect(exampleIncremental.query).equals("select current_timestamp::timestamp as ts");

        // Check inline tables
        expect(tableNames).includes("df_integration_test.example_inline");
        const exampleInline = graph.tables.filter(
          (t: dataform.ITable) => t.name === "df_integration_test.example_inline"
        )[0];
        expect(exampleInline.type).equals("inline");
        expect(exampleInline.query).equals('\nselect * from "df_integration_test"."sample_data"');
        expect(exampleInline.dependencies).eql(["df_integration_test.sample_data"]);
        expect(exampleInline.dependencyTargets).eql([
          dataform.Target.create({
            schema: "df_integration_test",
            name: "sample_data"
          })
        ]);

        expect(tableNames).includes("df_integration_test.example_using_inline");
        const exampleUsingInline = graph.tables.filter(
          (t: dataform.ITable) => t.name === "df_integration_test.example_using_inline"
        )[0];
        expect(exampleUsingInline.type).equals("table");
        expect(exampleUsingInline.query).equals(
          '\nselect * from (\nselect * from "df_integration_test"."sample_data")\nwhere true'
        );
        expect(exampleUsingInline.dependencies).eql(["df_integration_test.sample_data"]);
        expect(exampleUsingInline.dependencyTargets).eql([
          dataform.Target.create({
            schema: "df_integration_test",
            name: "sample_data"
          })
        ]);
      });
    });

    test("snowflake compiles", async () => {
      const graph = await compile({
        projectDir: "examples/common_v1",
        projectConfigOverride: { warehouse: "snowflake" }
      }).catch(error => error);
      expect(graph).to.not.be.an.instanceof(Error);
      expect(graph.graphErrors).deep.equals(dataform.GraphErrors.create({}));

      const mNames = graph.tables.map((t: dataform.ITable) => t.name);

      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL");
      const mIncremental = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL"
      )[0];
      expect(mIncremental.type).equals("incremental");
      expect(mIncremental.query).equals(
        "select convert_timezone('UTC', current_timestamp())::timestamp as ts"
      );
      expect(mIncremental.dependencies).deep.equals([]);
      expect(mIncremental.dependencyTargets).eql([]);

      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_TABLE");
      const mTable = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_TABLE"
      )[0];
      expect(mTable.type).equals("table");
      expect(mTable.query).equals('\nselect * from "DF_INTEGRATION_TEST"."SAMPLE_DATA"');
      expect(mTable.dependencies).deep.equals(["DF_INTEGRATION_TEST.SAMPLE_DATA"]);
      expect(mTable.dependencyTargets).eql([
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "SAMPLE_DATA"
        })
      ]);

      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_VIEW");
      const mView = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_VIEW"
      )[0];
      expect(mView.type).equals("view");
      expect(mView.query).equals('\nselect * from "DF_INTEGRATION_TEST"."SAMPLE_DATA"');
      expect(mView.dependencies).deep.equals(["DF_INTEGRATION_TEST.SAMPLE_DATA"]);
      expect(mView.dependencyTargets).eql([
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "SAMPLE_DATA"
        })
      ]);

      expect(mNames).includes("DF_INTEGRATION_TEST.SAMPLE_DATA");
      const mSampleData = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.SAMPLE_DATA"
      )[0];
      expect(mSampleData.type).equals("view");
      expect(mSampleData.query).equals(
        "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
      );
      expect(mSampleData.dependencies).deep.equals([]);
      expect(mSampleData.dependencyTargets).eql([]);

      // Check inline tables
      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_INLINE");
      const exampleInline = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_INLINE"
      )[0];
      expect(exampleInline.type).equals("inline");
      expect(exampleInline.query).equals('\nselect * from "DF_INTEGRATION_TEST"."SAMPLE_DATA"');
      expect(exampleInline.dependencies).eql(["DF_INTEGRATION_TEST.SAMPLE_DATA"]);
      expect(exampleInline.dependencyTargets).eql([
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "SAMPLE_DATA"
        })
      ]);

      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_USING_INLINE");
      const exampleUsingInline = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_USING_INLINE"
      )[0];
      expect(exampleUsingInline.type).equals("table");
      expect(exampleUsingInline.query).equals(
        '\nselect * from (\nselect * from "DF_INTEGRATION_TEST"."SAMPLE_DATA")\nwhere true'
      );
      expect(exampleUsingInline.dependencies).eql(["DF_INTEGRATION_TEST.SAMPLE_DATA"]);
      expect(exampleUsingInline.dependencyTargets).eql([
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "SAMPLE_DATA"
        })
      ]);

      const aNames = graph.assertions.map((a: dataform.IAssertion) => a.name);

      expect(aNames).includes("DF_INTEGRATION_TEST_ASSERTIONS.SAMPLE_DATA_ASSERTION");
      const assertion = graph.assertions.filter(
        (a: dataform.IAssertion) =>
          a.name === "DF_INTEGRATION_TEST_ASSERTIONS.SAMPLE_DATA_ASSERTION"
      )[0];
      expect(assertion.query).equals(
        'select * from "DF_INTEGRATION_TEST"."SAMPLE_DATA" where sample > 3'
      );
      expect(assertion.dependencies).eql([
        "DF_INTEGRATION_TEST.EXAMPLE_BACKTICKS",
        "DF_INTEGRATION_TEST.EXAMPLE_DEFERRED",
        "DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL",
        "DF_INTEGRATION_TEST.EXAMPLE_JS_BLOCKS",
        "DF_INTEGRATION_TEST.EXAMPLE_TABLE",
        "DF_INTEGRATION_TEST.EXAMPLE_USING_INLINE",
        "DF_INTEGRATION_TEST.EXAMPLE_VIEW",
        "DF_INTEGRATION_TEST.SAMPLE_DATA"
      ]);
      expect(assertion.dependencyTargets).eql([
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "EXAMPLE_BACKTICKS"
        }),
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "EXAMPLE_DEFERRED"
        }),
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "EXAMPLE_INCREMENTAL"
        }),
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "EXAMPLE_JS_BLOCKS"
        }),
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "EXAMPLE_TABLE"
        }),
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "EXAMPLE_USING_INLINE"
        }),
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "EXAMPLE_VIEW"
        }),
        dataform.Target.create({
          schema: "DF_INTEGRATION_TEST",
          name: "SAMPLE_DATA"
        })
      ]);
    });
  });

  test("backwards_compatibility", async () => {
    const graph = await compile({ projectDir: "examples/backwards_compatibility" });

    const tableNames = graph.tables.map((t: dataform.ITable) => t.name);

    // Make sure it compiles.
    expect(tableNames).includes("example");
    const example = graph.tables.filter((t: dataform.ITable) => t.name === "example")[0];
    expect(example.type).equals("table");
    expect(example.query.trim()).equals("select 1 as foo_bar");

    // Make sure we can dry run.
    new Builder(graph, {}, { tables: [] }, computeAllTransitiveInputs(graph)).build();
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
