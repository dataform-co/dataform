import { verifyObjectMatchesProto } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { ColumnDescriptors } from "df/core/column_descriptors";
import {
  Contextable,
  IActionConfig,
  IColumnsDescriptor,
  ICommonContext,
  IDependenciesConfig,
  IDocumentableConfig,
  INamedConfig,
  ITargetableConfig,
  Resolvable
} from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  checkExcessProperties,
  nativeRequire,
  resolvableAsTarget,
  setNameAndTarget,
  strictKeysOf,
  toResolvable
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * Configuration options for `operations` action types.
 */
export interface IOperationConfig
  extends IActionConfig,
    IDependenciesConfig,
    IDocumentableConfig,
    INamedConfig,
    ITargetableConfig {
  /**
   * Declares that this `operations` action creates a dataset which should be referenceable using the `ref` function.
   *
   * If set to true, this action should create a dataset with its configured name, using the `self()` context function.
   *
   * For example:
   * ```sql
   * create or replace table ${self()} as select ...
   * ```
   */
  hasOutput?: boolean;
}

export const IIOperationConfigProperties = strictKeysOf<IOperationConfig>()([
  "columns",
  "database",
  "dependencies",
  "description",
  "disabled",
  "hasOutput",
  "hermetic",
  "name",
  "schema",
  "tags",
  "type"
]);

/**
 * @hidden
 */
export class Operation extends ActionBuilder<dataform.Operation> {
  // TODO(ekrekr): make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.IOperation = dataform.Operation.create();

  // Hold a reference to the Session instance.
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  private contextableQueries: Contextable<ICommonContext, string | string[]>;

  constructor(
    session?: Session,
    config?: dataform.ActionConfig.OperationConfig,
    configPath?: string
  ) {
    super(session);
    this.session = session;

    if (!config) {
      return;
    }

    if (!config.name) {
      config.name = Path.fileName(config.filename);
    }
    const target = actionConfigToCompiledGraphTarget(config);
    this.proto.target = this.applySessionToTarget(target);
    this.proto.canonicalTarget = this.applySessionCanonicallyToTarget(target);

    // Resolve the filename as its absolute path.
    config.filename = Path.join(Path.dirName(configPath), config.filename);
    this.proto.fileName = config.filename;

    // TODO(ekrekr): load config proto column descriptors.
    this.config({
      dependencies: config.dependencyTargets.map(dependencyTarget =>
        actionConfigToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
      ),
      tags: config.tags,
      disabled: config.disabled,
      hasOutput: config.hasOutput,
      description: config.description
    });

    this.queries(nativeRequire(config.filename).query);
  }

  public config(config: IOperationConfig) {
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      config,
      IIOperationConfigProperties,
      "operation config"
    );
    if (config.dependencies) {
      this.dependencies(config.dependencies);
    }
    if (config.hermetic !== undefined) {
      this.hermetic(config.hermetic);
    }
    if (config.disabled) {
      this.disabled();
    }
    if (config.tags) {
      this.tags(config.tags);
    }
    if (config.hasOutput) {
      this.hasOutput(config.hasOutput);
    }
    if (config.description) {
      this.description(config.description);
    }
    if (config.columns) {
      this.columns(config.columns);
    }
    if (config.database) {
      this.database(config.database);
    }
    if (config.schema) {
      this.schema(config.schema);
    }
    return this;
  }

  public queries(queries: Contextable<ICommonContext, string | string[]>) {
    this.contextableQueries = queries;
    return this;
  }

  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable => {
      this.proto.dependencyTargets.push(resolvableAsTarget(resolvable));
    });
    return this;
  }

  public hermetic(hermetic: boolean) {
    this.proto.hermeticity = hermetic
      ? dataform.ActionHermeticity.HERMETIC
      : dataform.ActionHermeticity.NON_HERMETIC;
  }

  public disabled() {
    this.proto.disabled = true;
    return this;
  }

  public tags(value: string | string[]) {
    const newTags = typeof value === "string" ? [value] : value;
    newTags.forEach(t => {
      if (this.proto.tags.indexOf(t) < 0) {
        this.proto.tags.push(t);
      }
    });
    return this;
  }

  public hasOutput(hasOutput: boolean) {
    this.proto.hasOutput = hasOutput;
    return this;
  }

  public description(description: string) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.description = description;
    return this;
  }

  public columns(columns: IColumnsDescriptor) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.columns = ColumnDescriptors.mapToColumnProtoArray(
      columns,
      (e: Error) => this.session.compileError(e)
    );
    return this;
  }

  public database(database: string) {
    setNameAndTarget(
      this.session,
      this.proto,
      this.proto.target.name,
      this.proto.target.schema,
      database
    );
    return this;
  }

  public schema(schema: string) {
    setNameAndTarget(
      this.session,
      this.proto,
      this.proto.target.name,
      schema,
      this.proto.target.database
    );
    return this;
  }

  /**
   * @hidden
   */
  public getFileName() {
    return this.proto.fileName;
  }

  /**
   * @hidden
   */
  public getTarget() {
    return dataform.Target.create(this.proto.target);
  }

  public compile() {
    if (this.proto.actionDescriptor?.columns?.length > 0 && !this.proto.hasOutput) {
      this.session.compileError(
        new Error(
          "Actions of type 'operations' may only describe columns if they specify 'hasOutput: true'."
        ),
        this.proto.fileName
      );
    }

    const context = new OperationContext(this);

    const appliedQueries = context.apply(this.contextableQueries);
    this.proto.queries = typeof appliedQueries === "string" ? [appliedQueries] : appliedQueries;

    return verifyObjectMatchesProto(dataform.Operation, this.proto);
  }
}

/**
 * @hidden
 */
export class OperationContext implements ICommonContext {
  private operation?: Operation;

  constructor(operation: Operation) {
    this.operation = operation;
  }

  public self(): string {
    return this.resolve(this.operation.proto.target);
  }

  public name(): string {
    return this.operation.session.finalizeName(this.operation.proto.target.name);
  }

  public ref(ref: Resolvable | string[], ...rest: string[]) {
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      this.operation.session.compileError(new Error(`Action name is not specified`));
      return "";
    }
    this.operation.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.operation.session.resolve(ref, ...rest);
  }

  public schema(): string {
    return this.operation.session.finalizeSchema(this.operation.proto.target.schema);
  }

  public database(): string {
    if (!this.operation.proto.target.database) {
      this.operation.session.compileError(
        new Error(`Warehouse does not support multiple databases`)
      );
      return "";
    }

    return this.operation.session.finalizeDatabase(this.operation.proto.target.database);
  }

  public dependencies(name: Resolvable | Resolvable[]) {
    this.operation.dependencies(name);
    return "";
  }

  public tags(name: string | string[]) {
    this.operation.tags(name);
    return "";
  }

  public hasOutput(hasOutput: boolean) {
    this.operation.hasOutput(hasOutput);
    return "";
  }

  public when(cond: boolean, trueCase: string, falseCase: string = "") {
    return cond ? trueCase : falseCase;
  }

  public apply<T>(value: Contextable<ICommonContext, T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
