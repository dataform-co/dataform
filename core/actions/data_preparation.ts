import { dump as dumpYaml } from "js-yaml";

import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { ITableContext } from "df/core/actions/index";
import {
  Contextable,
  Resolvable
} from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  addDependenciesToActionDependencyTargets,
  configTargetToCompiledGraphTarget,
  nativeRequire,
  resolvableAsTarget,
  resolveActionsConfigFilename,
  setNameAndTarget,
  toResolvable,
  validateQueryString
} from "df/core/utils";
import { dataform } from "df/protos/ts";

// Enum for Load configuration settings
export const LoadType = ["replace", "append", "maximum", "unique", "automatic"] as const;
export type LoadType = typeof LoadType[number];

// Properties for load configuration, including the column name for MAXIMUM and UNIQUE load configurations.
export interface ILoadConfig {
  type: LoadType;
  column?: string;
}


/**
 * @hidden
 */
export class DataPreparation extends ActionBuilder<dataform.DataPreparation> {
  public session: Session;

  // We delay contextification until the final compile step, so hold these here for now.
  public contextableQuery: Contextable<IDataPreparationContext, string>;

  // TODO: make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.IDataPreparation = dataform.DataPreparation.create();

  constructor(
    session?: Session,
    config?: dataform.ActionConfig.DataPreparationConfig,
    configPath?: string
  ) {
    super(session);
    this.session = session;

    if (!config.name) {
      // Ensure we extract only the file name.
      // This handles both .yaml and .dp.yaml extensions
      const fileName = Path.filename(config.filename);

      if (fileName.toLowerCase().endsWith('.dp.yaml')) {
        config.name = fileName.slice(0, -8);
      } else if (fileName.toLowerCase().endsWith('.yaml')
          || fileName.toLowerCase().endsWith('.sqlx')) {
        config.name = fileName.slice(0, -5);
      } else {
        throw new Error("Only YAML and SQLX files are supported");
      }
    }

    const extension = Path.fileExtension(config.filename);
    if (extension === "yaml") {
      this.configureYaml(session, config, configPath);
    } else if (extension === "sqlx") {
      this.configureSqlx(session, config);
    }
  }

  /**
   * @hidden
   */
  public config(config: any) {
    if (config.database) {
      this.database(config.database);
    }
    if (config.schema) {
      this.schema(config.schema);
    }
    return this;
  }

  public query(query: Contextable<IDataPreparationContext, string>) {
    this.contextableQuery = query;
    return this;
  }

  /**
   * @hidden
   */
  public dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable =>
      addDependenciesToActionDependencyTargets(this, resolvable)
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
    // Return only the first target for now.
    return dataform.Target.create(this.proto.target);
  }

  public compile() {
    if (this.contextableQuery != null) {
      const context = new DataPreparationContext(this);
      this.proto.query = context.apply(this.contextableQuery).trim();
      validateQueryString(this.session, this.proto.query, this.proto.fileName);
    }

    return verifyObjectMatchesProto(
      dataform.DataPreparation,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
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

  private configureYamlWithoutTargets(
      dataPreparationAsJson: {
        [key: string]: any;
      },
      session?: Session,
      config?: dataform.ActionConfig.DataPreparationConfig
  ) {
    const defaultTarget = dataform.Target.create({name: config.name});
    this.proto.target = this.finalizeTarget(
        this.applySessionToTarget(
            defaultTarget,
            session.projectConfig,
            config.filename,
            true
        )
    );
    this.proto.targets = [this.proto.target];
    this.proto.canonicalTarget =
        this.applySessionToTarget(
            defaultTarget,
            session.canonicalProjectConfig
        );
    this.proto.canonicalTargets = [this.proto.canonicalTarget];
    const resolvedDefinition = this.applySessionToDataPreparationContents(dataPreparationAsJson);
    this.proto.dataPreparationYaml = dumpYaml(resolvedDefinition);
  }

  private configureYamlWithTargets(
      targets: dataform.Target[],
      dataPreparationAsJson: {
        [key: string]: any;
      },
      session?: Session,
      config?: dataform.ActionConfig.DataPreparationConfig
      ) {
    const resolvedTargets = targets.map(target =>
        this.applySessionToTarget(target, session.projectConfig, config.filename, true)
    );
    // Finalize list of targets.
    this.proto.targets = resolvedTargets.map(target => this.finalizeTarget(target));
    this.proto.canonicalTargets = targets.map(target =>
        this.applySessionToTarget(target, session.canonicalProjectConfig)
    );
    // Resolve all table references with compilation overrides and encode resolved proto instance
    const resolvedDefinition = this.applySessionToDataPreparationContents(dataPreparationAsJson);
    this.proto.dataPreparationYaml = dumpYaml(resolvedDefinition);

    // Set the unique target key as the first target defined.
    // TODO: Remove once multiple targets are supported.
    this.proto.target = resolvedTargets[0];
    this.proto.canonicalTarget = this.proto.canonicalTargets[0];
  }

  private applySessionToDataPreparationContents(definition: {
    [key: string]: any;
  }): { [key: string]: any } {
    // Handle empty definitions
    if (!definition) {
      return definition;
    }

    // Resolve error table, if set
    // @ts-ignore
    const errorTable = definition.configuration?.errorTable;
    if (errorTable) {
      definition.configuration.errorTable = this.applySessionToTableReference(
        errorTable as { [key: string]: string }
      );
    }

    // Loop through all nodes and resolve the compilation overrides for
    // all source and destination tables.
    if (definition.nodes) {
      (definition.nodes as Array<{ [key: string]: any }>).forEach((node, index) => {
        // Resolve source tables, if set.
        const sourceTable = node.source?.table;
        if (sourceTable) {
          definition.nodes[index].source.table = this.applySessionToTableReference(
            sourceTable as { [key: string]: string }
          );
        }

        // Resolve destination tables, if set.
        const destinationTable = node.destination?.table;
        if (destinationTable) {
          definition.nodes[index].destination.table = this.applySessionToTableReference(
            destinationTable as { [key: string]: string }
          );
        }
      });
    }

    return definition;
  }

  private applySessionToTableReference(tableReference: { [key: string]: string }): object {
    const target: dataform.ITarget = {
      database: tableReference.project,
      schema: tableReference.dataset,
      name: tableReference.table
    };
    const resolvedTarget = this.applySessionToTarget(
      dataform.Target.create(target),
      this.session.projectConfig
    );
    // Convert resolved target into a Data Preparation Table Reference
    let resolvedTableReference: { [key: string]: string } = {
      table: this.session.finalizeName(resolvedTarget.name)
    };

    // Ensure project and dataset field are added in order
    if (resolvedTarget.schema) {
      resolvedTableReference = {
        dataset: this.session.finalizeSchema(resolvedTarget.schema),
        ...resolvedTableReference
      };
    }
    if (resolvedTarget.database) {
      resolvedTableReference = {
        project: this.session.finalizeDatabase(resolvedTarget.database),
        ...resolvedTableReference
      };
    }
    return resolvedTableReference;
  }

  private getTargets(definition: { [key: string]: any }): dataform.Target[] {
    const targets: dataform.Target[] = [];

    if (definition && definition.nodes) {
      (definition.nodes as Array<{ [key: string]: any }>).forEach(node => {
        const table = node.destination?.table;
        if (table) {
          const compiledGraphTarget: dataform.ITarget = {
            database: table.project,
            schema: table.dataset,
            name: table.table
          };
          targets.push(dataform.Target.create(compiledGraphTarget));
        }
      });
    }

    return targets;
  }
  private configureYaml(
      session?: Session,
      config?: dataform.ActionConfig.DataPreparationConfig,
      configPath?: string) {
    config.filename = resolveActionsConfigFilename(config.filename, configPath);
    const dataPreparationAsJson = nativeRequire(config.filename).asJson;

    // Find targets
    const targets = this.getTargets(
        dataPreparationAsJson as {
          [key: string]: any;
        }
    );

    // if there are targets in the data preparation, resolve and set targets.
    // Otherwise, treat this as an empty data preparation.
    if (targets.length > 0) {
      this.configureYamlWithTargets(targets, dataPreparationAsJson, session, config);
    } else {
      // Handle the case where the data preparation is empty
      this.configureYamlWithoutTargets(dataPreparationAsJson, session, config);
    }

    this.proto.tags = config.tags;
    if (config.dependencyTargets) {
      this.dependencies(
          config.dependencyTargets.map(dependencyTarget =>
              configTargetToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
          )
      );
    }
    this.proto.fileName = config.filename;
    if (config.disabled) {
      this.proto.disabled = config.disabled;
    }
  }

  private configureSqlx(
      session?: Session,
      config?: dataform.ActionConfig.DataPreparationConfig) {
    const targets: dataform.Target[] = [];

    // Add destination as target
    targets.push(actionConfigToCompiledGraphTarget(config));
    // Add Error Table if specified as a secondary target
    if (config.errorTable != null) {
      const errorTableConfig = dataform.ActionConfig.DataPreparationConfig.ErrorTableConfig.create(config.errorTable);
      const errorTableTarget = actionConfigToCompiledGraphTarget(errorTableConfig);

      this.proto.errorTableRetentionDays = errorTableConfig.retentionDays
      targets.push(errorTableTarget);
    }

    // Add Load configuration
    this.proto.load = this.mapLoadMode(config.load?.mode, config.load?.incrementalColumn);

    // Resolve targets
    this.proto.targets  = targets.map(target =>
        this.applySessionToTarget(target, session.projectConfig, config.filename, true)
    ).map(target =>
        this.finalizeTarget(target)
    );

    // Add target and error table to proto
    this.proto.target = this.proto.targets[0];
    if (this.proto.targets.length > 1) {
      this.proto.errorTable = this.proto.targets[1];
    }

    // resolve canonical targets
    this.proto.canonicalTargets = targets.map(target =>
        this.applySessionToTarget(target, session.canonicalProjectConfig)
    );
    this.proto.canonicalTarget = this.proto.canonicalTargets[0];

    if (config.dependencyTargets) {
      this.dependencies(
          config.dependencyTargets.map(dependencyTarget =>
              configTargetToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
          )
      );
    }

    this.proto.tags = config.tags;
    this.proto.fileName = config.filename;
    if (config.disabled) {
      this.proto.disabled = config.disabled;
    }
    this.query(nativeRequire(config.filename).query);
  }

  // The type of onSchemaChange depends on the source file:
  // - for sqlx it will have type "string"
  // - for action.yaml it will be converted to enum which is represented
  // in TypeScript as a "number".
  private mapLoadMode(loadMode?: string|number, incrementalColumn?: string): dataform.LoadConfiguration {
    if (!loadMode) {
      return dataform.LoadConfiguration.create({"replace": {}});
    }

    switch (loadMode.toString().toUpperCase()) {
      case "REPLACE_TABLE": return dataform.LoadConfiguration.create({"replace": {}});
      case "APPEND": return dataform.LoadConfiguration.create({"append": {}});
      case "MAXIMUM": return dataform.LoadConfiguration.create({"maximum": {"columnName": this.validateLoadModeColumnName(incrementalColumn)}});
      case "UNIQUE": return dataform.LoadConfiguration.create({"unique": {"columnName": this.validateLoadModeColumnName(incrementalColumn)}});
      default: throw new Error(`LoadMode value "${loadMode}" is not supported`);
    }
  }

  private validateLoadModeColumnName(columnName?: string) : string {
    if (!columnName || columnName === "") {
      throw new Error(`columnName is required for MAXIMUM and UNIQUE load modes.`);
    }
    return columnName;
  }
}

export interface IDataPreparationContext extends ITableContext {
  /**
   * Shorthand for a validation SQL expression. This converts the parameters
   * into a validation call supported by Data Preparation.
   */
  validate: (exp: string) => string;
}

export class DataPreparationContext implements IDataPreparationContext {
  constructor(private dataPreparation: DataPreparation, private isIncremental = false) {}

  public config(config: dataform.ActionConfig.DataPreparationConfig) {
    this.dataPreparation.config(config);
    return "";
  }

  public self(): string {
    return this.resolve(this.dataPreparation.proto.target);
  }

  public name(): string {
    return this.dataPreparation.session.finalizeName(this.dataPreparation.proto.target.name);
  }

  public ref(ref: Resolvable | string[], ...rest: string[]): string {
    ref = toResolvable(ref, rest);
    if (!resolvableAsTarget(ref)) {
      this.dataPreparation.session.compileError(new Error(`Action name is not specified`));
      return "";
    }
    this.dataPreparation.dependencies(ref);
    return this.resolve(ref);
  }

  public resolve(ref: Resolvable | string[], ...rest: string[]) {
    return this.dataPreparation.session.resolve(ref, ...rest);
  }

  public schema(): string {
    return this.dataPreparation.session.finalizeSchema(this.dataPreparation.proto.target.schema);
  }

  public database(): string {
    if (!this.dataPreparation.proto.target.database) {
      this.dataPreparation.session.compileError(new Error(`Warehouse does not support multiple databases`));
      return "";
    }

    return this.dataPreparation.session.finalizeDatabase(this.dataPreparation.proto.target.database);
  }

  // TODO: Add support for incremental conditions in compilation output
  public when(cond: boolean, trueCase: string, falseCase: string = "") {
    return cond ? trueCase : falseCase;
  }

  // TODO: Add support for incremental conditions in compilation output
  public incremental() {
    return !!this.isIncremental;
  }

  public dependencies(res: Resolvable) {
    this.dataPreparation.dependencies(res);
    return "";
  }

  public validate(exp: string): string {
    return `-- @@VALIDATION\n|> WHERE IF(${exp},true,ERROR(\"Validation Failed\"))`;
  }

  public apply<T>(value: Contextable<IDataPreparationContext, T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
