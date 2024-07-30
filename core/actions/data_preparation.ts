import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { Resolvable } from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
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
    const dataPreparationContents = nativeRequire(config.filename).asJson;
    const dataPreparationDefinition = parseDataPreparationDefinitionJson(dataPreparationContents);
    this.proto.dataPreparation = dataPreparationDefinition;

    // Find targets
    const targets = getTargets(dataPreparationDefinition);
    this.proto.targets = targets.map(target =>
      this.applySessionToTarget(target, session.projectConfig, config.filename, true)
    );
    this.proto.canonicalTargets = targets.map(target =>
      this.applySessionToTarget(target, session.canonicalProjectConfig)
    );

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
}): dataform.DataPreparationDefinition {
  try {
    return dataform.DataPreparationDefinition.create(
      verifyObjectMatchesProto(
        dataform.DataPreparationDefinition,
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

function getTargets(definition: dataform.DataPreparationDefinition): dataform.Target[] {
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
