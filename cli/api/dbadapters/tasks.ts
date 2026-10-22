import { endsWithStatementTerminator } from "df/cli/api/utils/sql_parsing";
import { dataform } from "df/protos/ts";

function formatStatement(statement: string, isLast: boolean, modifier?: (mod: string) => string) {
  // Only treat a comment as trailing if it lives on the statement's last line.
  // Searching from the last newline avoids misreading an interior comment (e.g.
  // inside a multi-line procedure body) as a trailing comment.
  const lastLineStart = statement.lastIndexOf("\n") + 1;
  const commentIndex = statement.indexOf("--", lastLineStart);

  let code: string;
  let comment: string;

  if (commentIndex === 0) {
    code = "";
    comment = statement;
  } else if (commentIndex > 0) {
    code = statement.substring(0, commentIndex).trimEnd();
    comment = statement.substring(commentIndex);
  } else {
    code = statement;
    comment = "";
  }

  if (modifier && code.length > 0) {
    code = modifier(code);
  }

  if (!isLast && code.length > 0 && !code.endsWith(";")) {
    code = code + ";";
  }

  if (code.length > 0 && comment.length > 0) {
    return code + " " + comment;
  }
  return code || comment;
}

export function concatenateQueries(statements: string[], modifier?: (mod: string) => string) {
  const processed = statements
    .filter((statement) => !!statement)
    .map((statement) => statement.trim())
    .filter((statement) => statement.length > 0);
  const formattedStatements = processed.map((statement, index) =>
    formatStatement(statement, index === processed.length - 1, modifier),
  );
  return formattedStatements.join("\n");
}

/**
 * Wraps the given statement (or script) so that the `database.schema` dataset is created, if it
 * doesn't exist yet, in the same BigQuery job. This lets BigQuery infer the location of the new
 * dataset from the tables read by the statement.
 *
 * The statement is wrapped in a `BEGIN ... END` block, rather than just prepended with the
 * `CREATE SCHEMA` statement, because `DECLARE` statements are only allowed at the start of a
 * script or block.
 */
export function wrapWithCreateSchemaIfNotExists(
  database: string | undefined,
  schema: string,
  statement: string,
): string {
  const schemaRef = database ? `${database}.${schema}` : schema;
  const trimmedStatement = statement.trim();
  // Statements inside a block must be terminated with `;`. Empty statements (`;;`) are not
  // allowed, so only add one if missing. It goes on its own line, so it's not swallowed by a
  // trailing `--` comment.
  const terminatedStatement = endsWithStatementTerminator(trimmedStatement)
    ? trimmedStatement
    : `${trimmedStatement}\n;`;
  return `create schema if not exists \`${schemaRef}\`;\nbegin\n${terminatedStatement}\nend;`;
}

export class Tasks {
  private tasks: Task[] = [];

  public add(task: Task) {
    this.tasks.push(task);
    return this;
  }

  public addAll(tasks: Tasks) {
    this.tasks = this.tasks.concat(tasks.tasks);
    return this;
  }

  public build() {
    return this.tasks.map((task) => task.build());
  }

  public concatenate() {
    return new Tasks().add(
      Task.statement(concatenateQueries(this.tasks.map((task) => task.getStatement()))),
    );
  }
}

export class Task {
  public static statement(statement: string) {
    return new Task().type("statement").statement(statement);
  }

  public static assertion(statement: string) {
    return new Task().type("assertion").statement(statement);
  }
  private proto: dataform.IExecutionTask = dataform.ExecutionTask.create();

  public type(v: string) {
    this.proto.type = v;
    return this;
  }

  public statement(v: string) {
    this.proto.statement = v;
    return this;
  }

  public getStatement() {
    return this.proto.statement;
  }

  public build() {
    return dataform.ExecutionTask.create(this.proto);
  }
}
