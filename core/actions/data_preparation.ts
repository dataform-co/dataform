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
      config.name = Path.basename(config.filename);
    }

    config.filename = resolveActionsConfigFilename(config.filename, configPath);
    const dataPreparationAsJson = nativeRequire(config.filename).asJson;
    const dataPreparationDefinition = parseDataPreparationDefinitionJson(dataPreparationAsJson);

    // Find targets
    const targets = getTargets(dataPreparationDefinition);
    this.proto.targets = targets.map(target =>
      this.applySessionToTarget(target, session.projectConfig, config.filename, true)
    );
    this.proto.canonicalTargets = targets.map(target =>
      this.applySessionToTarget(target, session.canonicalProjectConfig)
    );

    // Resolve all table references with compilation overrides and encode resolved proto instance
    const resolvedDefinition = applySessionToDataPreparationContents(this, dataPreparationDefinition);
    this.proto.dataPreparationContents = dataform.dataprep.DataPreparation.encode(resolvedDefinition).finish();

    // Set the unique target key as the first target defined.
    // TODO: Remove once multiple targets are supported.
    this.proto.target = this.proto.targets[0];
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
}

function parseDataPreparationDefinitionJson(dataPreparationAsJson: {
  [key: string]: unknown;
}): dataform.dataprep.DataPreparation {
  try {
    return dataform.dataprep.DataPreparation.create(
      verifyObjectMatchesProto(
        dataform.dataprep.DataPreparation,
        dataPreparationAsJson as {
          [key: string]: any;
        },
        VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
      )
    );
  } catch (e) {
    if (e instanceof ReferenceError) {
      throw ReferenceError(`Data Preparation parsing error: ${e.message}`);
    }
    throw e;
  }
}

function applySessionToDataPreparationContents(
    actionBuilder: ActionBuilder<dataform.DataPreparation>,
    definition: dataform.dataprep.DataPreparation
): dataform.dataprep.DataPreparation {
  const resolvedDataPreparation = dataform.dataprep.DataPreparation.create(definition);

  // Resolve error table, if set
  const errorTable = definition.configuration?.errorTable;
  if (errorTable) {
    resolvedDataPreparation.configuration.errorTable =
        applySessionToTableReference(actionBuilder, errorTable);
  }

  // Loop through all nodes and resolve the compilation overrides for
  // all source and destination tables.
  definition.nodes.forEach((node, index) => {

        // Resolve source tables, if set.
        const sourceTable = node.source.table;
        if (sourceTable) {
          resolvedDataPreparation.nodes[index].source.table =
              applySessionToTableReference(actionBuilder, sourceTable);
        }

        // Resolve destination tables, if set.
        const destinationTable = node.destination?.table;
        if (destinationTable) {
          resolvedDataPreparation.nodes[index].destination.table =
              applySessionToTableReference(actionBuilder, destinationTable);
        }
      }

    );

  return resolvedDataPreparation;
}

function applySessionToTableReference(
    actionBuilder: ActionBuilder<dataform.DataPreparation>,
    tableReference: dataform.dataprep.ITableReference
): dataform.dataprep.ITableReference {
  const target: dataform.ITarget = {
    database: tableReference.project,
    schema: tableReference.dataset,
    name: tableReference.table
  }
  const resolvedTarget =
      actionBuilder.applySessionToTarget(
          dataform.Target.create(target),
          actionBuilder.session.projectConfig)
  // Convert resolved target into a Data Preparation Table Reference
  const resolvedTableReference = dataform.dataprep.TableReference.create({
    table: resolvedTarget.name
  });
  if (resolvedTarget.database) {
    resolvedTableReference.project = resolvedTarget.database;
  }
  if (resolvedTarget.schema) {
    resolvedTableReference.dataset = resolvedTarget.schema;
  }
  return resolvedTableReference;
}


function getTargets(definition: dataform.dataprep.DataPreparation): dataform.Target[] {
  const targets: dataform.Target[] = [];

  definition.nodes.forEach(node => {
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

  return targets;
}
