import * as adapters from "@dataform/core/adapters";
import { AConfig, AContextable, Assertion } from "@dataform/core/assertion";
import { OConfig, OContextable, Operation } from "@dataform/core/operation";
import {
  DistStyleTypes,
  ignoredProps,
  SortStyleTypes,
  TableTypes,
  Table,
  TContextable,
  TConfig
} from "@dataform/core/table";
import * as test from "@dataform/core/test";
import * as utils from "@dataform/core/utils";
import { dataform } from "@dataform/protos";

interface IActionProto {
  name?: string;
  fileName?: string;
  dependencies?: string[];
  target?: dataform.ITarget;
}

interface ISqlxConfig extends TConfig, AConfig, OConfig, test.TConfig {
  type: "view" | "table" | "inline" | "incremental" | "assertion" | "operations" | "test";
  name: string;
}

export interface IColumnsDescriptor {
  [name: string]: string | IRecordDescriptor;
}

interface IRecordDescriptor {
  description?: string;
  columns?: IColumnsDescriptor;
}

export function mapToColumnProtoArray(columns: IColumnsDescriptor): dataform.IColumnDescriptor[] {
  return utils.flatten(
    Object.keys(columns).map(column => mapColumnDescriptionToProto([column], columns[column]))
  );
}

function mapColumnDescriptionToProto(
  currentPath: string[],
  description: string | IRecordDescriptor
): dataform.IColumnDescriptor[] {
  if (typeof description === "string") {
    return [
      dataform.ColumnDescriptor.create({
        description,
        path: currentPath
      })
    ];
  }
  const columnDescriptor: dataform.IColumnDescriptor[] = description.description
    ? [
        dataform.ColumnDescriptor.create({
          description: description.description,
          path: currentPath
        })
      ]
    : [];
  const nestedColumns = description.columns ? Object.keys(description.columns) : [];
  return columnDescriptor.concat(
    utils.flatten(
      nestedColumns.map(nestedColumn =>
        mapColumnDescriptionToProto(
          currentPath.concat([nestedColumn]),
          description.columns[nestedColumn]
        )
      )
    )
  );
}

export type FullyQualifiedName = { schema: string; name: string };
export type Resolvable = string | FullyQualifiedName;

export class Session {
  public rootDir: string;

  public config: dataform.IProjectConfig;

  public actions: Array<Table | Operation | Assertion>;
  public tests: { [name: string]: test.Test };

  public graphErrors: dataform.IGraphErrors;

  constructor(rootDir: string, projectConfig?: dataform.IProjectConfig) {
    this.init(rootDir, projectConfig);
  }

  public init(rootDir: string, projectConfig?: dataform.IProjectConfig) {
    this.rootDir = rootDir;
    this.config = projectConfig || {
      defaultSchema: "dataform",
      assertionSchema: "dataform_assertions"
    };
    this.actions = [];
    this.tests = {};
    this.graphErrors = { compilationErrors: [] };
  }

  public adapter(): adapters.IAdapter {
    return adapters.create(this.config);
  }

  public sqlxAction(actionOptions: {
    sqlxConfig: ISqlxConfig;
    sqlStatementCount: number;
    hasIncremental: boolean;
    hasPreOperations: boolean;
    hasPostOperations: boolean;
    hasInputs: boolean;
  }) {
    if (actionOptions.sqlStatementCount > 1 && actionOptions.sqlxConfig.type !== "operations") {
      this.compileError(
        "Actions may only contain more than one SQL statement if they are of type 'operations'."
      );
    }
    if (
      actionOptions.sqlxConfig.hasOutput &&
      (actionOptions.sqlxConfig.type !== "operations" ||
        this.isDatasetType(actionOptions.sqlxConfig.type))
    ) {
      this.compileError(
        "Actions may only specify 'hasOutput: true' if they are of type 'operations' or create a dataset."
      );
    }
    if (
      actionOptions.sqlxConfig.columns &&
      !(this.isDatasetType(actionOptions.sqlxConfig.type) || actionOptions.sqlxConfig.hasOutput)
    ) {
      this.compileError("Actions may only specify 'columns' if they create a dataset.");
    }
    if (actionOptions.sqlxConfig.protected && actionOptions.sqlxConfig.type !== "incremental") {
      this.compileError(
        "Actions may only specify 'protected: true' if they are of type 'incremental'."
      );
    }
    if (actionOptions.hasIncremental && actionOptions.sqlxConfig.type !== "incremental") {
      this.compileError(
        "Actions may only include incremental_where if they are of type 'incremental'."
      );
    }
    if (actionOptions.sqlxConfig.dataset && actionOptions.sqlxConfig.type !== "test") {
      this.compileError("Actions may only specify 'dataset' if they are of type 'test'.");
    }
    if (!actionOptions.sqlxConfig.dataset && actionOptions.sqlxConfig.type === "test") {
      this.compileError("Actions must specify 'dataset' if they are of type 'test'.");
    }
    if (actionOptions.hasInputs && actionOptions.sqlxConfig.type !== "test") {
      this.compileError("Actions may only include input blocks if they are of type 'test'.");
    }
    if (actionOptions.sqlxConfig.disabled && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'disabled: true' if they create a dataset.");
    }
    if (actionOptions.sqlxConfig.redshift && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'redshift: { ... }' if they create a dataset.");
    }
    if (
      actionOptions.sqlxConfig.sqldatawarehouse &&
      !this.isDatasetType(actionOptions.sqlxConfig.type)
    ) {
      this.compileError(
        "Actions may only specify 'sqldatawarehouse: { ... }' if they create a dataset."
      );
    }
    if (actionOptions.sqlxConfig.bigquery && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only specify 'bigquery: { ... }' if they create a dataset.");
    }
    if (actionOptions.hasPreOperations && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only include pre_operations if they create a dataset.");
    }
    if (actionOptions.hasPostOperations && !this.isDatasetType(actionOptions.sqlxConfig.type)) {
      this.compileError("Actions may only include post_operations if they create a dataset.");
    }

    const action = (() => {
      switch (actionOptions.sqlxConfig.type) {
        case "view":
        case "table":
        case "inline":
        case "incremental":
          return this.publish(actionOptions.sqlxConfig.name);
        case "assertion":
          return this.assert(actionOptions.sqlxConfig.name);
        case "operations":
          return this.operate(actionOptions.sqlxConfig.name);
        case "test":
          return this.test(actionOptions.sqlxConfig.name);
        default:
          throw new Error(`Unrecognized action type: ${actionOptions.sqlxConfig.type}`);
      }
    })().config(actionOptions.sqlxConfig);

    if (action instanceof test.Test) {
      return action;
    }
    const finalSchema =
      actionOptions.sqlxConfig.schema ||
      (actionOptions.sqlxConfig.type === "assertion"
        ? this.config.assertionSchema
        : this.config.defaultSchema);
    action.proto.target = this.target(actionOptions.sqlxConfig.name, finalSchema);
    action.proto.name = `${action.proto.target.schema}.${action.proto.target.name}`;
    return action;
  }

  public target(target: string, defaultSchema?: string): dataform.ITarget {
    if (target.includes(".")) {
      const [schema, name] = target.split(".");
      return dataform.Target.create({ name, schema: schema + this.getSuffixWithUnderscore() });
    }
    return dataform.Target.create({
      name: target,
      schema: (defaultSchema || this.config.defaultSchema) + this.getSuffixWithUnderscore()
    });
  }

  public resolve(ref: Resolvable): string {
    const allResolved = this.findActions(ref);
    if (allResolved.length > 1) {
      this.compileError(new Error(utils.ambiguousActionNameMsg(ref, allResolved)));
    }
    const resolved = allResolved.length > 0 ? allResolved[0] : undefined;

    if (resolved && resolved instanceof Table && resolved.proto.type === "inline") {
      // TODO: Pretty sure this is broken as the proto.query value may not
      // be set yet as it happens during compilation. We should evalute the query here.
      return `(${resolved.proto.query})`;
    }
    if (resolved && resolved instanceof Operation && !resolved.proto.hasOutput) {
      this.compileError(
        new Error("Actions cannot resolve operations which do not produce output.")
      );
    }

    // TODO: We fall back to using the plain 'name' here for backwards compatibility with projects that use .sql files.
    // In these projects, this session may not know about all actions (yet), and thus we need to fall back to assuming
    // that the target *will* exist in the future. Once we break backwards compatibility with .sql files, we should remove
    // the code that calls 'this.target(...)' below, and append a compile error if we can't find a dataset whose name is 'name'.

    const target = resolved
      ? resolved.proto.target
      : this.target(typeof ref === "string" ? ref : ref.name);
    return this.adapter().resolveTarget(target);
  }

  public operate(name: string, queries?: OContextable<string | string[]>): Operation {
    const operation = new Operation();
    operation.session = this;
    this.setNameAndTarget(operation.proto, name);
    if (queries) {
      operation.queries(queries);
    }
    operation.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(operation);
    return operation;
  }

  public publish(name: string, queryOrConfig?: TContextable<string> | TConfig): Table {
    const newTable = new Table();
    newTable.session = this;
    this.setNameAndTarget(newTable.proto, name);
    if (!!queryOrConfig) {
      if (typeof queryOrConfig === "object") {
        newTable.config(queryOrConfig);
      } else {
        newTable.query(queryOrConfig);
      }
    }
    newTable.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(newTable);
    return newTable;
  }

  public assert(name: string, query?: AContextable<string>): Assertion {
    const assertion = new Assertion();
    assertion.session = this;
    this.setNameAndTarget(assertion.proto, name, this.config.assertionSchema);
    if (query) {
      assertion.query(query);
    }
    assertion.proto.fileName = utils.getCallerFile(this.rootDir);
    this.actions.push(assertion);
    return assertion;
  }

  public test(name: string): test.Test {
    this.checkTestNameIsUnused(name);
    const newTest = new test.Test();
    newTest.session = this;
    newTest.proto.name = name;
    newTest.proto.fileName = utils.getCallerFile(this.rootDir);
    // Add it to global index.
    this.tests[name] = newTest;
    return newTest;
  }

  private setNameAndTarget(action: IActionProto, name: string, overrideSchema?: string) {
    action.target = overrideSchema ? this.target(name, overrideSchema) : this.target(name);
    this.checkTargetIsUnused(action.target);
    action.name = `${action.target.schema}.${action.target.name}`;
  }

  public compileError(err: Error | string, path?: string) {
    const fileName = path || utils.getCallerFile(this.rootDir) || __filename;

    const compileError = dataform.CompilationError.create({
      fileName
    });
    if (typeof err === "string") {
      compileError.message = err;
    } else {
      compileError.message = err.message;
      compileError.stack = err.stack;
    }
    this.graphErrors.compilationErrors.push(compileError);
  }

  public compileGraphChunk<T>(actions: Array<{ proto: IActionProto; compile(): T }>): T[] {
    const compiledChunks: T[] = [];

    actions.forEach(action => {
      try {
        const compiledChunk = action.compile();
        compiledChunks.push(compiledChunk);
      } catch (e) {
        this.compileError(e, action.proto.fileName);
      }
    });

    return compiledChunks;
  }

  public compile(): dataform.ICompiledGraph {
    const compiledGraph = dataform.CompiledGraph.create({
      projectConfig: this.config,
      tables: this.compileGraphChunk(this.actions.filter(action => action instanceof Table)),
      operations: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Operation)
      ),
      assertions: this.compileGraphChunk(
        this.actions.filter(action => action instanceof Assertion)
      ),
      tests: this.compileGraphChunk(Object.values(this.tests)),
      graphErrors: this.graphErrors
    });

    const allActions: IActionProto[] = [].concat(
      compiledGraph.tables,
      compiledGraph.assertions,
      compiledGraph.operations
    );

    allActions.forEach(action => {
      const fQDeps = action.dependencies.map(act => {
        const allActs = this.findActions(act);
        if (allActs.length === 1) {
          return `${allActs[0].proto.target.schema}.${allActs[0].proto.target.name}`;
        } else if (allActs.length >= 1) {
          this.compileError(new Error(utils.ambiguousActionNameMsg(act, allActs)));
          return act;
        } else {
          this.compileError(
            new Error(
              `Missing dependency detected: Node "${
                action.name
              }" depends on "${act}" which does not exist.`
            )
          );
          return act;
        }
      });
      action.dependencies = [...new Set(fQDeps || [])];
    });

    const actionsByName: { [name: string]: dataform.IExecutionAction } = {};
    allActions.forEach(action => (actionsByName[action.name] = action));

    // Check for circular dependencies.
    const checkCircular = (
      action: dataform.IExecutionAction,
      dependents: dataform.IExecutionAction[]
    ): boolean => {
      if (dependents.indexOf(action) >= 0) {
        const message = `Circular dependency detected in chain: [${dependents
          .map(d => d.name)
          .join(" > ")} > ${action.name}]`;
        this.compileError(new Error(message));
        return true;
      }
      return (action.dependencies || []).some(d => {
        return actionsByName[d] && checkCircular(actionsByName[d], dependents.concat([action]));
      });
    };

    for (const action of allActions) {
      if (checkCircular(action, [])) {
        break;
      }
    }

    compiledGraph.tables.forEach(action => {
      const actionName = action.name;
      // type
      if (!!action.type && !Object.values(TableTypes).includes(action.type)) {
        const predefinedTypes = utils.joinQuoted(Object.values(TableTypes));
        this.compileError(
          new Error(
            `Error on dataset ${actionName}: Wrong type of table detected. Should only use predefined types: ${predefinedTypes}`
          )
        );
      }
      // "where" property
      if (action.type === TableTypes.INCREMENTAL && (!action.where || action.where.length === 0)) {
        this.compileError(
          new Error(
            `Error on dataset ${actionName}: "where" property is not defined. With the type “incremental” you must also specify the property “where”!`
          )
        );
      }
      // sqldatawarehouse config
      if (action.sqlDataWarehouse && action.sqlDataWarehouse.distribution) {
        const distribution = action.sqlDataWarehouse.distribution.toUpperCase();
        if (
          distribution !== "REPLICATE" &&
          distribution !== "ROUND_ROBIN" &&
          !utils.SQL_DATA_WAREHOUSE_DIST_HASH_REGEXP.test(distribution)
        ) {
          this.compileError(
            new Error(
              `Error on dataset ${actionName}: Invalid value for sqldatawarehouse distribution: "${distribution}"`
            )
          );
        }
      }
      // redshift config
      if (!!action.redshift) {
        if (
          Object.keys(action.redshift).length === 0 ||
          Object.values(action.redshift).every((value: string) => !value.length)
        ) {
          this.compileError(
            new Error(`Error on dataset ${actionName}: Missing properties in redshift config`)
          );
        }
        const validatePropertyDefined = (
          opts: dataform.IRedshiftOptions,
          prop: keyof dataform.IRedshiftOptions
        ) => {
          if (!opts[prop] || !opts[prop].length) {
            this.compileError(
              new Error(`Error on dataset ${actionName}: Property "${prop}" is not defined`)
            );
          }
        };
        const validatePropertiesDefined = (
          opts: dataform.IRedshiftOptions,
          props: Array<keyof dataform.IRedshiftOptions>
        ) => props.forEach(prop => validatePropertyDefined(opts, prop));
        const validatePropertyValueInValues = (
          opts: dataform.IRedshiftOptions,
          prop: keyof dataform.IRedshiftOptions & ("distStyle" | "sortStyle"),
          values: string[]
        ) => {
          if (!!opts[prop] && !values.includes(opts[prop])) {
            this.compileError(
              new Error(
                `Error on dataset ${actionName}: Wrong value of "${prop}" property. Should only use predefined values: ${utils.joinQuoted(
                  values
                )}`
              )
            );
          }
        };
        if (action.redshift.distStyle || action.redshift.distKey) {
          validatePropertiesDefined(action.redshift, ["distStyle", "distKey"]);
          validatePropertyValueInValues(
            action.redshift,
            "distStyle",
            Object.values(DistStyleTypes)
          );
        }
        if (
          action.redshift.sortStyle ||
          (action.redshift.sortKeys && action.redshift.sortKeys.length)
        ) {
          validatePropertiesDefined(action.redshift, ["sortStyle", "sortKeys"]);
          validatePropertyValueInValues(
            action.redshift,
            "sortStyle",
            Object.values(SortStyleTypes)
          );
        }
      }
      // ignored properties in tables
      if (!!ignoredProps[action.type]) {
        ignoredProps[action.type].forEach(ignoredProp => {
          if (utils.objectExistsOrIsNonEmpty(action[ignoredProp])) {
            const message = `Error on dataset ${actionName}: Unused property was detected: "${ignoredProp}". This property is not used for tables with type "${
              action.type
            }" and will be ignored.`;
            this.compileError(new Error(message));
          }
        });
      }
    });
    return compiledGraph;
  }

  public isDatasetType(type: string) {
    return type === "view" || type === "table" || type === "inline" || type === "incremental";
  }

  public findActions(res: Resolvable) {
    return this.actions.filter(action => {
      if (typeof res === "string") {
        return action.proto.target.name === res;
      }
      return action.proto.target.schema === res.schema && action.proto.target.name === res.name;
    });
  }

  public checkTargetIsUnused(target: dataform.ITarget) {
    const duplicateActions = this.findActions({ schema: target.schema, name: target.name });
    if (duplicateActions && duplicateActions.length > 0) {
      this.compileError(
        new Error(
          `Duplicate action name detected. Names within a schema must be unique across tables, assertions, and operations: "${
            target.schema
          }.${target.name}"`
        )
      );
    }
  }

  private checkTestNameIsUnused(name: string) {
    // Check for duplicate names
    if (this.tests[name]) {
      const message = `Duplicate test name detected: "${name}"`;
      this.compileError(new Error(message));
    }
  }
  public getSuffixWithUnderscore() {
    return !!this.config.schemaSuffix ? `_${this.config.schemaSuffix}` : "";
  }
}
