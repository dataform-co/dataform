import { expect } from "chai";

import {
  createExecutedGraphPrinter,
  warnAboutOverrideProtectedFullRefresh,
} from "df/cli/commands/run_command";
import { targetStringifier } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("warnAboutOverrideProtectedFullRefresh", ({ beforeEach, afterEach }) => {
  const originalStderrWrite = process.stderr.write;
  let stderrOutput: string[];

  beforeEach("capture stderr", () => {
    stderrOutput = [];
    process.stderr.write = ((chunk: string | Uint8Array) => {
      stderrOutput.push(String(chunk));
      return true;
    }) as typeof process.stderr.write;
  });

  afterEach("restore stderr", () => {
    process.stderr.write = originalStderrWrite;
  });

  test("warns and lists existing protected incremental tables", () => {
    warnAboutOverrideProtectedFullRefresh(
      {
        actions: [
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_unprotected" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "table",
            target: { database: "db", schema: "schema", name: "regular_table" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_protected" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_disabled" },
            tasks: [],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_jit" },
            tasks: [],
            jitCode: "return 1;",
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_new" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_was_view" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
        ],
        warehouseState: {
          tables: [
            {
              target: { database: "db", schema: "schema", name: "inc_unprotected" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "regular_table" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "inc_protected" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "inc_disabled" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "inc_jit" },
              type: dataform.TableMetadata.Type.TABLE,
            },
            {
              target: { database: "db", schema: "schema", name: "inc_was_view" },
              type: dataform.TableMetadata.Type.VIEW,
            },
          ],
        },
      },
      [
        {
          target: { database: "db", schema: "schema", name: "inc_protected" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_unprotected" },
          protected: false,
        },
        {
          target: { database: "db", schema: "schema", name: "regular_table" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_disabled" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_jit" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_new" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_was_view" },
          protected: true,
        },
      ],
    );

    expect(stderrOutput).to.have.length(1);
    const plainOutput = stderrOutput.join("").replace(/\x1b\[[0-9;]*m/g, "");
    expect(plainOutput).equals(
      [
        "Warning: --full-refresh and --override-protected are both set. " +
          "The following protected incremental tables will be dropped and recreated: ",
        "  db.schema.inc_protected",
        "  db.schema.inc_jit",
        "",
      ].join("\n"),
    );
  });

  test("does not warn when no protected incremental tables are present", () => {
    warnAboutOverrideProtectedFullRefresh(
      {
        actions: [
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_unprotected" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
        ],
        warehouseState: {
          tables: [
            {
              target: { database: "db", schema: "schema", name: "inc_unprotected" },
              type: dataform.TableMetadata.Type.TABLE,
            },
          ],
        },
      },
      [
        {
          target: { database: "db", schema: "schema", name: "inc_unprotected" },
          protected: false,
        },
      ],
    );

    expect(stderrOutput).deep.equals([]);
  });

  test("does not warn when no incremental tables are present", () => {
    warnAboutOverrideProtectedFullRefresh({
      actions: [
        {
          type: "table",
          tableType: "table",
          target: { database: "db", schema: "schema", name: "regular_table" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "table",
          tableType: "view",
          target: { database: "db", schema: "schema", name: "some_view" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "assertion",
          target: { database: "db", schema: "schema", name: "some_assertion" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
      ],
      warehouseState: {
        tables: [
          {
            target: { database: "db", schema: "schema", name: "regular_table" },
            type: dataform.TableMetadata.Type.TABLE,
          },
        ],
      },
    });

    expect(stderrOutput).deep.equals([]);
  });

  test("does not warn when incremental tables do not exist in warehouse yet", () => {
    warnAboutOverrideProtectedFullRefresh(
      {
        actions: [
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_new" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_was_view" },
            tasks: [{ type: "statement", statement: "SELECT 1" }],
          },
        ],
        warehouseState: {
          tables: [
            {
              target: { database: "db", schema: "schema", name: "inc_was_view" },
              type: dataform.TableMetadata.Type.VIEW,
            },
          ],
        },
      },
      [
        {
          target: { database: "db", schema: "schema", name: "inc_new" },
          protected: true,
        },
        {
          target: { database: "db", schema: "schema", name: "inc_was_view" },
          protected: true,
        },
      ],
    );

    expect(stderrOutput).deep.equals([]);
  });

  test("does not warn when only disabled incremental tables are present", () => {
    warnAboutOverrideProtectedFullRefresh(
      {
        actions: [
          {
            type: "table",
            tableType: "incremental",
            target: { database: "db", schema: "schema", name: "inc_disabled" },
            tasks: [],
          },
        ],
        warehouseState: {
          tables: [
            {
              target: { database: "db", schema: "schema", name: "inc_disabled" },
              type: dataform.TableMetadata.Type.TABLE,
            },
          ],
        },
      },
      [
        {
          target: { database: "db", schema: "schema", name: "inc_disabled" },
          protected: true,
        },
      ],
    );

    expect(stderrOutput).deep.equals([]);
  });
});

function stripAnsi(text: string): string {
  return text.replace(/\x1b\[[0-9;]*m/g, "");
}

function captureStreams(fn: () => void): { stdout: string; stderr: string } {
  const stdoutChunks: string[] = [];
  const stderrChunks: string[] = [];
  const origStdoutWrite = process.stdout.write.bind(process.stdout);
  const origStderrWrite = process.stderr.write.bind(process.stderr);
  process.stdout.write = ((chunk: string | Uint8Array) => {
    stdoutChunks.push(String(chunk));
    return true;
  }) as typeof process.stdout.write;
  process.stderr.write = ((chunk: string | Uint8Array) => {
    stderrChunks.push(String(chunk));
    return true;
  }) as typeof process.stderr.write;
  try {
    fn();
  } finally {
    process.stdout.write = origStdoutWrite;
    process.stderr.write = origStderrWrite;
  }
  return {
    stdout: stripAnsi(stdoutChunks.join("")),
    stderr: stripAnsi(stderrChunks.join("")),
  };
}

suite("createExecutedGraphPrinter", () => {
  test("prints colliding readable targets with their own action types across separate ticks", () => {
    const viewTarget: dataform.ITarget = { database: "proj", schema: "b", name: "c" };
    const assertionTarget: dataform.ITarget = { schema: "proj", name: "b.c" };

    const viewAction: dataform.IExecutionAction = {
      type: "table",
      tableType: "view",
      target: viewTarget,
      tasks: [{ type: "statement", statement: "SELECT 1" }],
    };
    const assertionAction: dataform.IExecutionAction = {
      type: "assertion",
      target: assertionTarget,
      tasks: [{ type: "assertion", statement: "SELECT 2" }],
    };

    const actionsByTarget = new Map<string, dataform.IExecutionAction>([
      [targetStringifier.stringify(viewTarget), viewAction],
      [targetStringifier.stringify(assertionTarget), assertionAction],
    ]);
    const printExecutedGraph = createExecutedGraphPrinter(actionsByTarget, true);

    const viewCompleted: dataform.IActionResult = {
      target: viewTarget,
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      tasks: [{ status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL }],
    };
    const assertionRunning: dataform.IActionResult = {
      target: assertionTarget,
      status: dataform.ActionResult.ExecutionStatus.RUNNING,
      tasks: [],
    };
    const assertionCompleted: dataform.IActionResult = {
      target: assertionTarget,
      status: dataform.ActionResult.ExecutionStatus.SUCCESSFUL,
      tasks: [{ status: dataform.TaskResult.ExecutionStatus.SUCCESSFUL }],
    };

    const tick1 = captureStreams(() => {
      printExecutedGraph({ actions: [viewCompleted, assertionRunning] });
    });
    expect(tick1.stdout.trim()).to.equal("Table dry run success:  b.c [view]");

    const tick2 = captureStreams(() => {
      printExecutedGraph({ actions: [viewCompleted, assertionCompleted] });
    });
    expect(tick2.stdout.trim()).to.equal("Assertion dry run success:  proj.b.c");
  });
});
