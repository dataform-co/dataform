import { Builder, compile } from "@dataform/api";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
import { fail } from "assert";
import { expect } from "chai";
import { cleanSql } from "df/tests/utils";
import * as path from "path";
import * as stackTrace from "stack-trace";

describe("examples", () => {
  describe("common_v2 bigquery", async () => {
    for (const schemaSuffix of ["", "suffix"]) {
      const schemaWithSuffix = (schema: string) =>
        schemaSuffix ? `${schema}_${schemaSuffix}` : schema;

      it(`compiles with suffix "${schemaSuffix}"`, async () => {
        const graph = await compile({
          projectDir: path.resolve("df/examples/common_v2"),
          projectConfigOverride: { schemaSuffix, warehouse: "bigquery" }
        });
        expect(graph.graphErrors).to.eql(
          dataform.GraphErrors.create({
            compilationErrors: [
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_bigquery.sqlx",
                message: "Actions may only specify 'bigquery: { ... }' if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_output.sqlx",
                message:
                  "Actions may only specify 'hasOutput: true' if they are of type 'operations' or create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_postops.sqlx",
                message: "Actions may only include post_operations if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_preops.sqlx",
                message: "Actions may only include pre_operations if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/assertion_with_redshift.sqlx",
                message: "Actions may only specify 'redshift: { ... }' if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/disabled_assertion.sqlx",
                message: "Actions may only specify 'disabled: true' if they create a dataset."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/protected_assertion.sqlx",
                message:
                  "Actions may only specify 'protected: true' if they are of type 'incremental'."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/view_with_incremental.sqlx",
                message:
                  "Actions may only include incremental_where if they are of type 'incremental'."
              }),
              dataform.CompilationError.create({
                fileName: "definitions/has_compile_errors/view_with_multiple_statements.sqlx",
                message:
                  "Actions may only contain more than one SQL statement if they are of type 'operations'."
              })
            ]
          })
        );

        // Check JS blocks get processed.
        const exampleJsBlocks = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".example_js_blocks"
        );
        expect(exampleJsBlocks).to.not.be.undefined;
        expect(exampleJsBlocks.type).equals("table");
        expect(exampleJsBlocks.query.trim()).equals("select 1 as foo");

        // Check we can import and use an external package.
        const exampleIncremental = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".example_incremental"
        );
        expect(exampleIncremental).to.not.be.undefined;
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
            t.name === schemaWithSuffix("df_integration_test") + ".example_is_incremental"
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
        expect(exampleIsIncremental.preOps).to.eql(["\n    select 2\n"]);
        expect(exampleIsIncremental.postOps).to.eql(["\n    select 1\n"]);

        // Check tables defined in includes are not included.
        const exampleIgnore = graph.tables.find(
          (t: dataform.ITable) => t.name === "example_ignore"
        );
        expect(exampleIgnore).to.be.undefined;
        const exampleIgnore_2 = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".example_ignore"
        );
        expect(exampleIgnore).to.be.undefined;

        // Check SQL files with raw back-ticks get escaped.
        const exampleBackticks = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".example_backticks"
        );
        expect(exampleBackticks).to.not.be.undefined;
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
            t.name === schemaWithSuffix("df_integration_test") + ".example_deferred"
        );
        expect(exampleDeferred).to.not.be.undefined;
        expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");

        // Check inline tables
        const exampleInline = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".example_inline"
        );
        expect(exampleInline).to.not.be.undefined;
        expect(exampleInline.type).equals("inline");
        expect(exampleInline.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix("df_integration_test")}.sample_data\``
        );
        expect(exampleInline.dependencies).includes(
          schemaWithSuffix("df_integration_test") + ".sample_data"
        );

        const exampleUsingInline = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".example_using_inline"
        );
        expect(exampleUsingInline).to.not.be.undefined;
        expect(exampleUsingInline.type).equals("table");
        expect(exampleUsingInline.query.trim()).equals(
          `select * from (\n\nselect * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\`\n)\nwhere true`
        );
        expect(exampleUsingInline.dependencies).includes(
          schemaWithSuffix("df_integration_test") + ".sample_data"
        );

        // Check view
        const exampleView = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".example_view"
        );
        expect(exampleView).to.not.be.undefined;
        expect(exampleView.type).equals("view");
        expect(exampleView.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix("df_integration_test")}.sample_data\``
        );
        expect(exampleView.dependencies).deep.equals([
          schemaWithSuffix("df_integration_test") + ".sample_data"
        ]);
        expect(exampleView.tags).to.eql([]);

        // Check table
        const exampleTable = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".example_table"
        );
        expect(exampleTable).to.not.be.undefined;
        expect(exampleTable.type).equals("table");
        expect(exampleTable.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\`\n\n-- here \${"is"} a \`comment\n\n/* \${"another"} \` backtick \` containing \`\`\`comment */`
        );
        expect(exampleTable.dependencies).deep.equals([
          schemaWithSuffix("df_integration_test") + ".sample_data"
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
            t.name === schemaWithSuffix("df_integration_test") + ".example_table_with_tags"
        );
        expect(exampleTableWithTags).to.not.be.undefined;
        expect(exampleTableWithTags.tags).to.eql(["tag1", "tag2", "tag3"]);

        // Check sample data
        const exampleSampleData = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("df_integration_test") + ".sample_data"
        );
        expect(exampleSampleData).to.not.be.undefined;
        expect(exampleSampleData.type).equals("view");
        expect(exampleSampleData.query.trim()).equals(
          "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
        );
        expect(exampleSampleData.dependencies).to.eql([]);
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

        // Check schema overrides defined in "config {}"
        const exampleUsingOverriddenSchema = graph.tables.find(
          (t: dataform.ITable) =>
            t.name === schemaWithSuffix("override_schema") + ".override_schema_example"
        );

        expect(exampleUsingOverriddenSchema).to.not.be.undefined;
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
            schemaWithSuffix("df_integration_test") + ".override_schema_example_unchanged"
        );

        expect(exampleUsingOverriddenSchemaUnchanged).to.not.be.undefined;
        expect(exampleUsingOverriddenSchemaUnchanged.target.schema).equals(
          schemaWithSuffix("df_integration_test")
        );
        expect(exampleUsingOverriddenSchemaUnchanged.type).equals("view");
        expect(exampleUsingOverriddenSchemaUnchanged.query.trim()).equals(
          "select 1 as test_schema_override"
        );

        // Check assertion
        const exampleAssertion = graph.assertions.find(
          (a: dataform.IAssertion) => a.name === schemaWithSuffix("hi_there") + ".example_assertion"
        );
        expect(exampleAssertion).to.not.be.undefined;
        expect(exampleAssertion.target.schema).equals(schemaWithSuffix("hi_there"));
        expect(exampleAssertion.query.trim()).equals(
          `select * from \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.sample_data\` where sample = 100`
        );
        expect(exampleAssertion.dependencies).to.eql([
          schemaWithSuffix("df_integration_test") + ".sample_data"
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
            schemaWithSuffix("df_integration_test_assertions") + ".example_assertion_with_tags"
        );
        expect(exampleAssertionWithTags).to.not.be.undefined;
        expect(exampleAssertionWithTags.target.schema).equals(
          schemaWithSuffix("df_integration_test_assertions")
        );
        expect(exampleAssertionWithTags.tags).to.eql(["tag1", "tag2"]);

        // Check example operations file
        const exampleOperations = graph.operations.find(
          (o: dataform.IOperation) =>
            o.name === schemaWithSuffix("df_integration_test") + ".example_operations"
        );
        expect(exampleOperations).to.not.be.undefined;
        expect(exampleOperations.hasOutput).is.false;
        expect(exampleOperations.queries).to.eql([
          "\n\nCREATE OR REPLACE VIEW someschema.someview AS (SELECT 1 AS test)\n",
          `\nDROP VIEW IF EXISTS \`tada-analytics.${schemaWithSuffix(
            "override_schema"
          )}.override_schema_example\`\n`
        ]);
        expect(exampleOperations.dependencies).to.eql([
          schemaWithSuffix("df_integration_test") + ".example_inline",
          schemaWithSuffix("override_schema") + ".override_schema_example"
        ]);
        expect(exampleOperations.tags).to.eql([]);

        // Check example operation with output.
        const exampleOperationWithOutput = graph.operations.find(
          (o: dataform.IOperation) =>
            o.name === schemaWithSuffix("df_integration_test") + ".example_operation_with_output"
        );
        expect(exampleOperationWithOutput).to.not.be.undefined;
        expect(exampleOperationWithOutput.target.schema).equals(
          schemaWithSuffix("df_integration_test")
        );
        expect(exampleOperationWithOutput.target.name).equals("example_operation_with_output");
        expect(exampleOperationWithOutput.queries).to.eql([
          `\nCREATE OR REPLACE VIEW \`tada-analytics.${schemaWithSuffix(
            "df_integration_test"
          )}.example_operation_with_output\` AS (SELECT * FROM \`tada-analytics.some_external_schema_name.very_important_external_table\`)`
        ]);
        expect(exampleOperationWithOutput.dependencies).to.eql([
          "some_external_schema_name.very_important_external_table"
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
            o.name === schemaWithSuffix("df_integration_test") + ".example_operations_with_tags"
        );
        expect(exampleOperationsWithTags).to.not.be.undefined;
        expect(exampleOperationsWithTags.tags).to.eql(["tag1"]);

        // Check declaration.
        const exampleDeclaration = graph.declarations.find(
          d => d.name === "some_external_schema_name.very_important_external_table"
        );
        expect(exampleDeclaration).to.not.be.undefined;
        expect(exampleDeclaration.target).eql(
          dataform.Target.create({
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
      });
    }
  });

  describe("common_v1", async () => {
    it("bigquery compiles", async () => {
      const graph = await compile({
        projectDir: path.resolve("df/examples/common_v1"),
        projectConfigOverride: { warehouse: "bigquery", defaultDatabase: "tada-analytics" }
      });
      const tableNames = graph.tables.map((t: dataform.ITable) => t.name);

      expect(graph.graphErrors).to.eql(dataform.GraphErrors.create());

      // Check JS blocks get processed.
      expect(tableNames).includes("df_integration_test.example_js_blocks");
      const exampleJsBlocks = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.example_js_blocks"
      )[0];
      expect(exampleJsBlocks.type).equals("table");
      expect(exampleJsBlocks.query).equals("select 1 as foo");

      // Check we can import and use an external package.
      expect(tableNames).includes("df_integration_test.example_incremental");
      const exampleIncremental = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.example_incremental"
      )[0];
      expect(exampleIncremental.query).equals("select current_timestamp() as ts");
      expect(exampleIncremental.where.trim()).equals(
        "ts > (select max(ts) from `tada-analytics.df_integration_test.example_incremental`) or (select max(ts) from `tada-analytics.df_integration_test.example_incremental`) is null"
      );

      // Check tables defined in includes are not included.
      expect(tableNames).not.includes("example_ignore");

      // Check SQL files with raw back-ticks get escaped.
      expect(tableNames).includes("df_integration_test.example_backticks");
      const exampleBackticks = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.example_backticks"
      )[0];
      expect(cleanSql(exampleBackticks.query)).equals(
        "select * from `tada-analytics.df_integration_test.sample_data`"
      );

      // Check deferred calls to table resolve to the correct definitions file.
      expect(tableNames).includes("df_integration_test.example_deferred");
      const exampleDeferred = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.example_deferred"
      )[0];
      expect(exampleDeferred.fileName).includes("definitions/example_deferred.js");

      // Check inline tables
      expect(tableNames).includes("df_integration_test.example_inline");
      const exampleInline = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.example_inline"
      )[0];
      expect(exampleInline.type).equals("inline");
      expect(exampleInline.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleInline.dependencies).includes("df_integration_test.sample_data");

      expect(tableNames).includes("df_integration_test.example_using_inline");
      const exampleUsingInline = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.example_using_inline"
      )[0];
      expect(exampleUsingInline.type).equals("table");
      expect(exampleUsingInline.query).equals(
        "\nselect * from (\nselect * from `tada-analytics.df_integration_test.sample_data`)\nwhere true"
      );
      expect(exampleUsingInline.dependencies).includes("df_integration_test.sample_data");

      // Check view
      expect(tableNames).includes("df_integration_test.example_view");
      const exampleView = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.example_view"
      )[0];
      expect(exampleView.type).equals("view");
      expect(exampleView.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleView.dependencies).deep.equals(["df_integration_test.sample_data"]);

      // Check table
      expect(tableNames).includes("df_integration_test.example_table");
      const exampleTable = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.example_table"
      )[0];
      expect(exampleTable.type).equals("table");
      expect(exampleTable.query).equals(
        "\nselect * from `tada-analytics.df_integration_test.sample_data`"
      );
      expect(exampleTable.dependencies).deep.equals(["df_integration_test.sample_data"]);

      // Check sample data
      expect(tableNames).includes("df_integration_test.sample_data");
      const exampleSampleData = graph.tables.filter(
        (t: dataform.ITable) => t.name === "df_integration_test.sample_data"
      )[0];
      expect(exampleSampleData.type).equals("view");
      expect(exampleSampleData.query).equals(
        "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
      );
      expect(exampleSampleData.dependencies).to.eql([]);
    });

    it("bigquery compiles with schema override", async () => {
      const graph = await compile({
        projectDir: path.resolve("df/examples/common_v1"),
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

    it("redshift compiles", () => {
      return compile({
        projectDir: "df/examples/common_v1",
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
        expect(exampleInline.dependencies).includes("df_integration_test.sample_data");

        expect(tableNames).includes("df_integration_test.example_using_inline");
        const exampleUsingInline = graph.tables.filter(
          (t: dataform.ITable) => t.name === "df_integration_test.example_using_inline"
        )[0];
        expect(exampleUsingInline.type).equals("table");
        expect(exampleUsingInline.query).equals(
          '\nselect * from (\nselect * from "df_integration_test"."sample_data")\nwhere true'
        );
        expect(exampleUsingInline.dependencies).includes("df_integration_test.sample_data");
      });
    });

    it("snowflake compiles", async () => {
      const graph = await compile({
        projectDir: "df/examples/common_v1",
        projectConfigOverride: { warehouse: "snowflake" }
      }).catch(error => error);
      expect(graph).to.not.be.an.instanceof(Error);

      const gErrors = utils.validate(graph);

      expect(gErrors)
        .to.have.property("compilationErrors")
        .to.be.an("array").that.is.empty;
      expect(gErrors)
        .to.have.property("validationErrors")
        .to.be.an("array").that.is.empty;

      const mNames = graph.tables.map((t: dataform.ITable) => t.name);

      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL");
      const mIncremental = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL"
      )[0];
      expect(mIncremental.type).equals("incremental");
      expect(mIncremental.query).equals(
        "select convert_timezone('UTC', current_timestamp())::timestamp as ts"
      );
      expect(mIncremental.dependencies).to.be.an("array").that.is.empty;

      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_TABLE");
      const mTable = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_TABLE"
      )[0];
      expect(mTable.type).equals("table");
      expect(mTable.query).equals('\nselect * from "DF_INTEGRATION_TEST"."SAMPLE_DATA"');
      expect(mTable.dependencies).deep.equals(["DF_INTEGRATION_TEST.SAMPLE_DATA"]);

      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_VIEW");
      const mView = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_VIEW"
      )[0];
      expect(mView.type).equals("view");
      expect(mView.query).equals('\nselect * from "DF_INTEGRATION_TEST"."SAMPLE_DATA"');
      expect(mView.dependencies).deep.equals(["DF_INTEGRATION_TEST.SAMPLE_DATA"]);

      expect(mNames).includes("DF_INTEGRATION_TEST.SAMPLE_DATA");
      const mSampleData = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.SAMPLE_DATA"
      )[0];
      expect(mSampleData.type).equals("view");
      expect(mSampleData.query).equals(
        "select 1 as sample union all\nselect 2 as sample union all\nselect 3 as sample"
      );
      expect(mSampleData.dependencies).to.be.an("array").that.is.empty;

      // Check inline tables
      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_INLINE");
      const exampleInline = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_INLINE"
      )[0];
      expect(exampleInline.type).equals("inline");
      expect(exampleInline.query).equals('\nselect * from "DF_INTEGRATION_TEST"."SAMPLE_DATA"');
      expect(exampleInline.dependencies).includes("DF_INTEGRATION_TEST.SAMPLE_DATA");

      expect(mNames).includes("DF_INTEGRATION_TEST.EXAMPLE_USING_INLINE");
      const exampleUsingInline = graph.tables.filter(
        (t: dataform.ITable) => t.name === "DF_INTEGRATION_TEST.EXAMPLE_USING_INLINE"
      )[0];
      expect(exampleUsingInline.type).equals("table");
      expect(exampleUsingInline.query).equals(
        '\nselect * from (\nselect * from "DF_INTEGRATION_TEST"."SAMPLE_DATA")\nwhere true'
      );
      expect(exampleUsingInline.dependencies).includes("DF_INTEGRATION_TEST.SAMPLE_DATA");

      const aNames = graph.assertions.map((a: dataform.IAssertion) => a.name);

      expect(aNames).includes("DF_INTEGRATION_TEST_ASSERTIONS.SAMPLE_DATA_ASSERTION");
      const assertion = graph.assertions.filter(
        (a: dataform.IAssertion) =>
          a.name === "DF_INTEGRATION_TEST_ASSERTIONS.SAMPLE_DATA_ASSERTION"
      )[0];
      expect(assertion.query).equals(
        'select * from "DF_INTEGRATION_TEST"."SAMPLE_DATA" where sample > 3'
      );
      expect(assertion.dependencies).to.include.members([
        "DF_INTEGRATION_TEST.SAMPLE_DATA",
        "DF_INTEGRATION_TEST.EXAMPLE_TABLE",
        "DF_INTEGRATION_TEST.EXAMPLE_INCREMENTAL",
        "DF_INTEGRATION_TEST.EXAMPLE_VIEW"
      ]);
    });
  });

  it("backwards_compatibility", async () => {
    const graph = await compile({ projectDir: "df/examples/backwards_compatibility" });

    const tableNames = graph.tables.map((t: dataform.ITable) => t.name);

    // Make sure it compiles.
    expect(tableNames).includes("example");
    const example = graph.tables.filter((t: dataform.ITable) => t.name === "example")[0];
    expect(example.type).equals("table");
    expect(example.query.trim()).equals("select 1 as foo_bar");

    // Make sure we can dry run.
    new Builder(graph, {}, { tables: [] }).build();
  });

  it("times out after timeout period during compilation", async () => {
    try {
      await compile({ projectDir: "df/examples/never_finishes_compiling" });
      fail("Compilation timeout Error expected.");
    } catch (e) {
      expect(e.message).to.equal("Compilation timed out");
    }
  });

  it("invalid dataform json throws error", async () => {
    try {
      await compile({
        projectDir: path.resolve("df/examples/invalid_dataform_json")
      });
      fail("Should have failed.");
    } catch (e) {
      // OK
    }
  });

  it("version is correctly set", async () => {
    const graph = await compile({
      projectDir: "df/examples/common_v2",
      projectConfigOverride: { warehouse: "bigquery" }
    });
    const { version: expectedVersion } = require("@dataform/core/package.json");
    expect(graph.dataformCoreVersion).equals(expectedVersion);
  });
});
