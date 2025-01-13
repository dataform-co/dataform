import { dump as dumpYaml } from "js-yaml";

import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import { ITableContext } from "df/core/actions/table";
import {
  Contextable,
  IActionConfig,
  INamedConfig,
  ITarget,
  ITargetableConfig,
  Resolvable
} from "df/core/common";
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

// Brings the properties for the destination table
export interface IDataPreparationConfig
  extends IActionConfig,
    INamedConfig,
    ITargetableConfig {
  // Error Table Settings
  errorTable?: IErrorTableConfig;

  load?: ILoadConfig;
}

// Includes project, dataset, table settings, as well as retention for the table
export interface IErrorTableConfig extends ITargetableConfig {
  name: string; 
  retentionDays?: number;
}


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
      config.name = Path.basename(config.filename);
    }

    const extension = Path.fileExtension(config.filename);
    if (extension == "yaml") {
      this.configureYaml(session, config, configPath);
    } else if (extension == "sqlx") {
      this.configureSqlx(session, config);
    }
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
      
      this.proto.fileName = config.filename
      this.query(nativeRequire(config.filename).query);
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

  private applySessionToDataPreparationContents(definition: {
    [key: string]: any;
  }): { [key: string]: any } {
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

    if (definition.nodes) {
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
}

export interface IDataPreparationContext extends ITableContext {
  /**
   * Shorthand for a validation SQL expression. This converts the parameters
   * into a validation call supported by Data Preparation.
   */
  validate: (exp: string, message: string) => string;
}

export class DataPreparationContext implements IDataPreparationContext {
  constructor(private dataPreparation: DataPreparation, private isIncremental = false) {}

  public config(config: IDataPreparationConfig) {
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

  public validate(exp: string, msg: string): string {
    return `# @@VALIDATION ${msg}\n|> WHERE IF(${exp},true,ERROR(\"${msg}\"))`;
  }

  public apply<T>(value: Contextable<IDataPreparationContext, T>): T {
    if (typeof value === "function") {
      return (value as any)(this);
    } else {
      return value;
    }
  }
}
