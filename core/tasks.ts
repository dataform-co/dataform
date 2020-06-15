import { dataform } from "df/protos/ts";

export function concatenateQueries(statements: string[], modifier?: (mod: string) => string) {
  return statements
    .filter(statement => !!statement)
    .map(statement => statement.trim())
    .map(statement =>
      statement.length > 0 && statement.charAt(statement.length - 1) === ";"
        ? statement.substring(0, statement.length - 1)
        : statement
    )
    .map(statement => (!!modifier ? modifier(statement) : statement))
    .join(";");
}

export class Tasks {
  public static create() {
    return new Tasks();
  }
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
    return this.tasks.map(task => task.build());
  }

  public concatenate() {
    return Tasks.create().add(
      Task.statement(concatenateQueries(this.tasks.map(task => task.getStatement())))
    );
  }
}

export class Task {
  public static create() {
    return new Task();
  }

  public static statement(statement: string) {
    return Task.create()
      .type("statement")
      .statement(statement);
  }

  public static assertion(statement: string) {
    return Task.create()
      .type("assertion")
      .statement(statement);
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
