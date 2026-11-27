import { expect } from "chai";
import * as fs from "fs-extra";

import { ExecutionSql } from "df/cli/api/dbadapters/execution_sql";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("ExecutionSql with 'onSchemaChange'", () => {
  const executionSql = new ExecutionSql(
    {
      defaultDatabase: "project-id",
      defaultSchema: "dataset-id",
    },
    "2.0.0",
    () => "test_uuid",
  );

  const baseTable: dataform.ITable = {
    type: "incremental",
    enumType: dataform.TableType.INCREMENTAL,
    target: {
      database: "project-id",
      schema: "dataset-id",
      name: "incremental_on_schema_change",
    },
    query: "select 1 as id, 'a' as field1",
    incrementalQuery: "select 1 as id, 'a' as field1, 'new' as field2",
  };

  const tableMetadata: dataform.ITableMetadata = {
    type: dataform.TableMetadata.Type.TABLE,
    fields: [
      {
        name: "id",
        primitive: dataform.Field.Primitive.INTEGER,
      },
      {
        name: "field1",
        primitive: dataform.Field.Primitive.STRING,
      },
    ],
  };

  test("generates script for FAIL strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.FAIL,
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const sql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/on_schema_change_fail.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("generates script for EXTEND strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.EXTEND,
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const sql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/on_schema_change_extend.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("generates script for SYNCHRONIZE strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.SYNCHRONIZE,
      uniqueKey: ["id"],
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const sql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/on_schema_change_synchronize.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("generates simple merge for IGNORE strategy", () => {
    const table = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.IGNORE,
      uniqueKey: ["id"],
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const sql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/on_schema_change_ignore.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("generates INSERT_OVERWRITE script for IGNORE strategy", () => {
    const table = {
      ...baseTable,
      incrementalStrategy: dataform.IncrementalStrategy.INSERT_OVERWRITE,
      bigquery: {
        partitionBy: "DATE(ts)",
        incrementalPredicates: [
          "DATAFORM_DEST.ts >= '2024-01-01'",
          "DATAFORM_SOURCE.ts >= '2024-01-01'",
        ],
      },
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const sql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/insert_overwrite_ignore.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("generates INSERT_OVERWRITE script for EXTEND strategy", () => {
    const table = {
      ...baseTable,
      incrementalStrategy: dataform.IncrementalStrategy.INSERT_OVERWRITE,
      onSchemaChange: dataform.OnSchemaChange.EXTEND,
      bigquery: {
        partitionBy: "DATE(ts)",
      },
    };
    const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
    const sql = tasks
      .build()
      .map((t) => t.statement)
      .join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/insert_overwrite_extend.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("places all DECLARE statements before any executable statement in generated script body", () => {
    for (const strategy of [
      dataform.OnSchemaChange.FAIL,
      dataform.OnSchemaChange.EXTEND,
      dataform.OnSchemaChange.SYNCHRONIZE,
    ]) {
      const table = {
        ...baseTable,
        onSchemaChange: strategy,
        uniqueKey: ["id"],
      };
      const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
      const scriptSql = tasks.build()[0].statement;
      expect(scriptSql).to.match(/^BEGIN\n/);

      const scriptBody = scriptSql.split("BEGIN\n")[1].split("\nEXCEPTION WHEN ERROR THEN")[0];
      const statements = scriptBody
        .split(";")
        .map((s) =>
          s
            .split("\n")
            .filter((line) => !line.trim().startsWith("--"))
            .join("\n")
            .trim(),
        )
        .filter((s) => s.length > 0);

      let seenNonDeclare = false;
      for (const stmt of statements) {
        if (stmt.toUpperCase().startsWith("DECLARE ")) {
          expect(
            seenNonDeclare,
            `DECLARE statement appeared after non-DECLARE statement in strategy ${dataform.OnSchemaChange[strategy]}: "${stmt}"`,
          ).to.equal(false);
        } else {
          seenNonDeclare = true;
        }
      }
    }
  });

  test("executes dynamic DML inside the script block for onSchemaChange", () => {
    for (const strategy of [
      dataform.OnSchemaChange.FAIL,
      dataform.OnSchemaChange.EXTEND,
      dataform.OnSchemaChange.SYNCHRONIZE,
    ]) {
      const table = {
        ...baseTable,
        onSchemaChange: strategy,
        uniqueKey: ["id"],
      };
      const tasks = executionSql.publishTasks(table, { fullRefresh: false }, tableMetadata);
      const builtTasks = tasks.build();
      expect(builtTasks.length).to.equal(1);

      const fullSql = builtTasks[0].statement;

      const parts = fullSql.split("\nEXCEPTION WHEN ERROR THEN\n");
      expect(parts.length).to.equal(2);
      const [bodyPart, errorHandlerPart] = parts;
      expect(bodyPart).to.match(/^BEGIN\n/);
      expect(bodyPart).to.include("SET dataform_columns_list =");
      expect(bodyPart).to.include("SET dataform_columns_merge =");
      expect(bodyPart).to.include("EXECUTE IMMEDIATE");
      expect(errorHandlerPart).to.match(/\n  RAISE;\nEND;$/);
      expect(errorHandlerPart).to.not.include("insert into");
      expect(errorHandlerPart).to.not.include("merge ");
      expect(errorHandlerPart).to.not.include("field1");
    }
  });

  test("error handler only drops fully qualified tables", () => {
    // The handler only needs to drop the empty temp table used for the schema comparison. The
    // staging table is a script-scoped temporary table, which BigQuery drops with the script.
    for (const strategy of [
      dataform.OnSchemaChange.FAIL,
      dataform.OnSchemaChange.EXTEND,
      dataform.OnSchemaChange.SYNCHRONIZE,
    ]) {
      const table = {
        ...baseTable,
        onSchemaChange: strategy,
        uniqueKey: ["id"],
      };
      const fullSql = executionSql
        .publishTasks(table, { fullRefresh: false }, tableMetadata)
        .build()[0].statement;

      const errorHandler = fullSql.split("EXCEPTION WHEN ERROR THEN")[1];
      const droppedTables = [...errorHandler.matchAll(/DROP TABLE IF EXISTS `([^`]+)`/g)].map(
        (match) => match[1],
      );
      expect(droppedTables).to.have.lengthOf(1);
      for (const droppedTable of droppedTables) {
        expect(
          droppedTable,
          `error handler for ${dataform.OnSchemaChange[strategy]} drops an unqualified table`,
        ).to.match(/^[^.]+\.[^.]+\.[^.]+$/);
      }
    }
  });

  test("escapes double quotes and backslashes of updatePartitionFilter and incrementalPredicates in dynamic DML for onSchemaChange", () => {
    const updatePartitionFilter = String.raw`region = "EU"`;
    // The triple quotes would terminate the EXECUTE IMMEDIATE string literal if left unescaped.
    const incrementalPredicate = String.raw`DATAFORM_DEST.field1 != """a\\b"""`;
    const bigquery = { updatePartitionFilter, incrementalPredicates: [incrementalPredicate] };

    const tables: { [name: string]: dataform.ITable } = {
      MERGE: {
        ...baseTable,
        onSchemaChange: dataform.OnSchemaChange.EXTEND,
        uniqueKey: ["id"],
        bigquery,
      },
      INSERT_OVERWRITE: {
        ...baseTable,
        onSchemaChange: dataform.OnSchemaChange.EXTEND,
        incrementalStrategy: dataform.IncrementalStrategy.INSERT_OVERWRITE,
        bigquery: { ...bigquery, partitionBy: "DATE(ts)" },
      },
    };

    for (const [name, table] of Object.entries(tables)) {
      const fullSql = executionSql
        .publishTasks(table, { fullRefresh: false }, tableMetadata)
        .build()[0].statement;

      const dynamicDmlBlocks = fullSql.split('EXECUTE IMMEDIATE ("""');
      expect(dynamicDmlBlocks.length, `${name}: number of dynamic DML blocks`).to.equal(2);
      // The part of the string literal up to its first closing triple quote (which precedes the
      // concatenated column list) holds the whole condition built from the user-provided values.
      const literal = dynamicDmlBlocks[1].split('"""')[0];

      expect(literal, name).to.include(String.raw`and DATAFORM_DEST.region = \"EU\"`);
      expect(literal, name).to.include(
        String.raw`and (DATAFORM_DEST.field1 != \"\"\"a\\\\b\"\"\")`,
      );

      // Evaluating the escape sequences of the literal yields the original values back.
      const evaluatedLiteral = literal.replace(/\\(.)/g, "$1");
      expect(evaluatedLiteral, name).to.include(`and DATAFORM_DEST.${updatePartitionFilter}`);
      expect(evaluatedLiteral, name).to.include(`and (${incrementalPredicate})`);
    }
  });

  test("places incrementalPreOps and incrementalPostOps inside script block with inner BEGIN...END block for onSchemaChange", () => {
    const table: dataform.ITable = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.EXTEND,
      incrementalPreOps: ["DECLARE watermark TIMESTAMP;\nSET watermark = CURRENT_TIMESTAMP();"],
      incrementalPostOps: ["SELECT watermark;"],
    };
    const builtTasks = executionSql
      .publishTasks(table, { fullRefresh: false }, tableMetadata)
      .build();
    expect(builtTasks.length).to.equal(1);

    const fullSql = builtTasks[0].statement;
    expect(fullSql).to.match(/^BEGIN\nDECLARE watermark TIMESTAMP;/);
    expect(fullSql).to.include(
      "BEGIN\nDECLARE watermark TIMESTAMP;\nSET watermark = CURRENT_TIMESTAMP();\n\nBEGIN\n\n-- Declare variables for schema comparison and strategy execution.",
    );
    expect(fullSql).to.include(
      "-- Cleanup staging table\nDROP TABLE IF EXISTS `staging_table_temp_test_uuid`;\nEND;\n\nSELECT watermark;\nEXCEPTION WHEN ERROR THEN",
    );
  });

  test("wraps core body in inner BEGIN...END block when only incrementalPostOps are set for onSchemaChange", () => {
    const table: dataform.ITable = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.SYNCHRONIZE,
      incrementalPostOps: ["SELECT 1;"],
    };
    const builtTasks = executionSql
      .publishTasks(table, { fullRefresh: false }, tableMetadata)
      .build();
    expect(builtTasks.length).to.equal(1);

    const fullSql = builtTasks[0].statement;
    // The internal variables are declared at the start of the inner block...
    expect(fullSql).to.match(
      /^BEGIN\nBEGIN\n\n-- Declare variables for schema comparison and strategy execution.\nDECLARE dataform_columns ARRAY<STRING>;/,
    );
    // ...which is closed before the post-operations, so they are out of scope there.
    const [beforePostOps] = fullSql.split("SELECT 1;");
    expect(beforePostOps).to.match(/\nEND;\n\n$/);
    expect((beforePostOps.match(/^BEGIN$/gm) || []).length).to.equal(
      (beforePostOps.match(/^END;$/gm) || []).length + 1,
    );
    const declaredVariables = [...fullSql.matchAll(/^\s*DECLARE (\w+)/gm)].map((match) => match[1]);
    expect(declaredVariables.length).to.be.greaterThan(0);
    for (const declaredVariable of declaredVariables) {
      expect(declaredVariable).to.match(/^dataform_/);
    }
  });

  test("extracts lowercased top-level column names from uniqueKey entries in SYNCHRONIZE drop guard while keeping MERGE ON keys intact", () => {
    const table: dataform.ITable = {
      ...baseTable,
      onSchemaChange: dataform.OnSchemaChange.SYNCHRONIZE,
      uniqueKey: [
        "`My-Id`",
        " `First Name` ",
        "`X`",
        "`My-Rec`.`sub-id`",
        "`my-rec`.other",
        "Other_Rec.`sub-id`",
        "SAFE.PARSE_DATE('%Y', dt)",
        "`",
        "``",
        "Plain_Id",
      ],
    };
    const fullSql = executionSql
      .publishTasks(table, { fullRefresh: false }, tableMetadata)
      .build()[0].statement;

    expect(fullSql).to.include(
      `WHERE LOWER(col) IN UNNEST(["my-id","first name","x","my-rec","other_rec","safe.parse_date('%y', dt)","\`","\`\`","plain_id"])`,
    );
    expect(fullSql).to.include(
      "on DATAFORM_DEST.`My-Id` = DATAFORM_SOURCE.`My-Id` " +
        "and DATAFORM_DEST. `First Name`  = DATAFORM_SOURCE. `First Name`  " +
        "and DATAFORM_DEST.`X` = DATAFORM_SOURCE.`X` " +
        "and DATAFORM_DEST.`My-Rec`.`sub-id` = DATAFORM_SOURCE.`My-Rec`.`sub-id` " +
        "and DATAFORM_DEST.`my-rec`.other = DATAFORM_SOURCE.`my-rec`.other " +
        "and DATAFORM_DEST.Other_Rec.`sub-id` = DATAFORM_SOURCE.Other_Rec.`sub-id` " +
        "and DATAFORM_DEST.SAFE.PARSE_DATE('%Y', dt) = DATAFORM_SOURCE.SAFE.PARSE_DATE('%Y', dt) " +
        "and DATAFORM_DEST.` = DATAFORM_SOURCE.` " +
        "and DATAFORM_DEST.`` = DATAFORM_SOURCE.`` " +
        "and DATAFORM_DEST.Plain_Id = DATAFORM_SOURCE.Plain_Id",
    );
  });
});

suite("ExecutionSql for property graphs", () => {
  const executionSql = new ExecutionSql(
    {
      defaultDatabase: "project-id",
      defaultSchema: "dataset-id",
    },
    "2.0.0",
    () => "test_uuid",
  );

  test("emits CREATE OR REPLACE PROPERTY GRAPH for FinGraph", () => {
    const graphBody = `NODE TABLES (
  \`project-id.dataset-id.account\` AS Account KEY (id) LABEL Account PROPERTIES ARE ALL COLUMNS,
  \`project-id.dataset-id.person\` AS Person KEY (id) LABEL Person PROPERTIES ARE ALL COLUMNS
)
EDGE TABLES (
  \`project-id.dataset-id.person_own_account\` AS PersonOwnAccount SOURCE KEY (id) REFERENCES Person (id) DESTINATION KEY (account_id) REFERENCES Account (id) LABEL Owns PROPERTIES ARE ALL COLUMNS,
  \`project-id.dataset-id.account_transfer_account\` AS AccountTransferAccount SOURCE KEY (id) REFERENCES Account (id) DESTINATION KEY (to_id) REFERENCES Account (id) LABEL Transfers PROPERTIES (amount, create_time)
)`;
    const propertyGraph: dataform.IPropertyGraph = {
      target: { database: "project-id", schema: "dataset-id", name: "FinGraph" },
      graphBody,
    };
    const tasks = executionSql.createPropertyGraphTasks(propertyGraph);
    const sql = tasks.map((t) => t.statement).join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/property_graph_fingraph.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });

  test("emits CREATE OR REPLACE PROPERTY GRAPH for HRGraph", () => {
    const graphBody = `NODE TABLES (
  \`project-id.dataset-id.employee\` AS Employee KEY (id) LABEL Employee PROPERTIES ARE ALL COLUMNS EXCEPT (ssn, salary),
  \`project-id.dataset-id.manager\` AS Manager KEY (id) LABEL Manager PROPERTIES ARE ALL COLUMNS EXCEPT (bonus)
)
EDGE TABLES (
  \`project-id.dataset-id.reports\` AS Reports SOURCE KEY (employee_id) REFERENCES Employee (id) DESTINATION KEY (manager_id) REFERENCES Manager (id) LABEL Reports PROPERTIES ARE ALL COLUMNS
)`;
    const propertyGraph: dataform.IPropertyGraph = {
      target: { database: "project-id", schema: "dataset-id", name: "HRGraph" },
      graphBody,
    };
    const tasks = executionSql.createPropertyGraphTasks(propertyGraph);
    const sql = tasks.map((t) => t.statement).join("\n;\n");
    const expectedSql = fs.readFileSync("cli/api/goldens/property_graph_hrgraph.sql", "utf8");
    expect(sql).to.equal(expectedSql.trim());
  });
});

suite("ExecutionSql for operations", () => {
  const executionSql = new ExecutionSql(
    { defaultDatabase: "project-id", defaultSchema: "dataset-id" },
    "2.0.0",
  );
  const target = { database: "project-id", schema: "dataset-id", name: "op" };

  test("runs all queries as a single script", () => {
    expect(
      executionSql.createOperationTasks({
        target,
        queries: ["DECLARE min_id INT64 DEFAULT 0", "SELECT * FROM t WHERE id >= min_id -- c"],
      }),
    ).deep.equals([
      dataform.ExecutionTask.create({
        type: "statement",
        statement: "DECLARE min_id INT64 DEFAULT 0;\nSELECT * FROM t WHERE id >= min_id -- c",
      }),
    ]);
  });

  test("terminates queries ending with a comment, and keeps existing semicolons", () => {
    expect(
      executionSql.createOperationTasks({
        target,
        queries: ["\n  CREATE TABLE a AS SELECT 1 -- comment\n", "DROP TABLE b;", "SELECT 2"],
      })[0].statement,
    ).equals("CREATE TABLE a AS SELECT 1; -- comment\nDROP TABLE b;\nSELECT 2");
  });

  test("uses a single query as is", () => {
    const query = "\nSELECT '--' AS dashes -- comment\n";
    expect(executionSql.createOperationTasks({ target, queries: [query] })).deep.equals([
      dataform.ExecutionTask.create({ type: "statement", statement: query }),
    ]);
  });

  test("skips empty queries", () => {
    expect(
      executionSql.createOperationTasks({ target, queries: ["  \n", "SELECT 1", ""] }),
    ).deep.equals([dataform.ExecutionTask.create({ type: "statement", statement: "SELECT 1" })]);
    expect(executionSql.createOperationTasks({ target, queries: [] })).deep.equals([]);
  });

  test("returns no tasks for disabled operations", () => {
    expect(
      executionSql.createOperationTasks({ target, queries: ["SELECT 1"], disabled: true }),
    ).deep.equals([]);
  });
});
