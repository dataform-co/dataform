import { endsWithStatementTerminator } from "df/cli/api/utils/sql_parsing";
import { dataform } from "df/protos/ts";

/**
 * Returns the index just after the last character of code in the given GoogleSQL statement, i.e.
 * ignoring trailing whitespace and comments (`--`, `#` and `/* *\/`). String literals (single,
 * double and triple quoted) and quoted identifiers are skipped as a whole, so that e.g. a `--`
 * inside a string literal is not mistaken for a comment. Returns 0 if the statement has no code.
 */
function codeEndIndex(statement: string): number {
  let codeEnd = 0;
  let i = 0;
  while (i < statement.length) {
    const char = statement[i];
    const next = statement[i + 1];
    if ((char === "-" && next === "-") || char === "#") {
      const newline = statement.indexOf("\n", i);
      i = newline === -1 ? statement.length : newline + 1;
    } else if (char === "/" && next === "*") {
      const end = statement.indexOf("*/", i + 2);
      i = end === -1 ? statement.length : end + 2;
    } else if (char === "'" || char === '"' || char === "`") {
      const tripleQuote = char.repeat(3);
      const closing = char !== "`" && statement.startsWith(tripleQuote, i) ? tripleQuote : char;
      i += closing.length;
      while (i < statement.length && !statement.startsWith(closing, i)) {
        // Skip escaped characters, e.g. \' inside a single-quoted string.
        i += statement[i] === "\\" ? 2 : 1;
      }
      i = Math.min(i + closing.length, statement.length);
      codeEnd = i;
    } else {
      i++;
      if (!/\s/.test(char)) {
        codeEnd = i;
      }
    }
  }
  return codeEnd;
}

function formatStatement(statement: string, isLast: boolean, modifier?: (mod: string) => string) {
  // Split off trailing comments, so that the modifier and the terminating semicolon only apply to
  // the code: a semicolon after a trailing `--` or `#` comment would be commented out.
  const codeEnd = codeEndIndex(statement);
  let code = statement.substring(0, codeEnd);
  const comment = statement.substring(codeEnd);

  if (modifier && code.length > 0) {
    code = modifier(code);
  }

  if (!isLast && code.length > 0 && !code.endsWith(";")) {
    code = code + ";";
  }

  return code + comment;
}

export function concatenateQueries(
  statements: string[],
  modifier?: (mod: string) => string,
  trimLeadingWhitespace = true,
) {
  const processed = statements
    .filter((statement) => !!statement)
    .map((statement) => (trimLeadingWhitespace ? statement.trim() : statement.trimEnd()))
    .filter((statement) => statement.length > 0);
  const formattedStatements = processed.map((statement, index) =>
    formatStatement(statement, index === processed.length - 1, modifier),
  );
  return formattedStatements.join("\n");
}

/**
 * Number of lines `wrapWithCreateSchemaIfNotExists()` adds before the wrapped statement: the
 * statement starts on line `1 + CREATE_SCHEMA_WRAPPER_LINE_OFFSET` of the script.
 */
export const CREATE_SCHEMA_WRAPPER_LINE_OFFSET = 2;

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
