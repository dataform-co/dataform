import { dump as dumpYaml } from "js-yaml";

import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { Resolvable } from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  addDependenciesToActionDependencyTargets,
  configTargetToCompiledGraphTarget,
  nativeRequire,
  resolveActionsConfigFilename
} from "df/core/utils";
import { dataform } from "df/protos/ts";
import {filename} from 'df/core/path';

/**
 * @hidden
 */
export class DataPreparation extends ActionBuilder<dataform.DataPreparation> {
  public session: Session;

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
      this.resolveTargets(targets, dataPreparationAsJson, session, config);
    } else {
      // Handle the case where the data preparation is empty
      this.resolveEmpty(dataPreparationAsJson, session, config);
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

  /**
   * @hidden
   */
  public config(config: any) {
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
    return verifyObjectMatchesProto(
      dataform.DataPreparation,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
  }

  private resolveEmpty(
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
    this.proto.dataPreparationYaml = dumpYaml(dataPreparationAsJson);
  }

  private resolveTargets(
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
      return {};
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
}
