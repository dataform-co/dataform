import * as protos from "@dataform/protos";

export class Tasks {
  private tasks: Task[] = [];

  public static create() {
    return new Tasks();
  }

  add(task: Task) {
    this.tasks.push(task);
    return this;
  }

  addAll(tasks: Tasks) {
    this.tasks = this.tasks.concat(tasks.tasks);
    return this;
  }

  build() {
    return this.tasks.map(task => task.build());
  }
}

export class Task {
  private proto: protos.IExecutionTask = protos.ExecutionTask.create();

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

  public type(v: string) {
    this.proto.type = v;
    return this;
  }

  public statement(v: string) {
    this.proto.statement = v;
    return this;
  }

  public ignoreErrors(v: boolean) {
    this.proto.ignoreErrors = v;
    return this;
  }

  public build() {
    return protos.ExecutionTask.create(this.proto);
  }
}
