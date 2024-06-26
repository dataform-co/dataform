import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { Resolvable } from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  addDependenciesToActionDependencyTargets,
  nativeRequire,
  resolveActionsConfigFilename
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 */
export class Notebook extends ActionBuilder<dataform.Notebook> {
  public session: Session;

  // TODO: make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.INotebook = dataform.Notebook.create();

  // If true, adds the inline assertions of dependencies as direct dependencies for this action.
  public dependOnDependencyAssertions: boolean = false;

  constructor(
    session?: Session,
    config?: dataform.ActionConfig.NotebookConfig,
    configPath?: string
  ) {
    super(session);

    if (!config.name) {
      config.name = Path.basename(config.filename);
    }
    const target = actionConfigToCompiledGraphTarget(config);
    config.filename = resolveActionsConfigFilename(config.filename, configPath);

    this.session = session;
    this.proto.target = this.applySessionToTarget(
      target,
      session.projectConfig,
      config.filename,
      true
    );
    this.proto.canonicalTarget = this.applySessionToTarget(target, session.canonicalProjectConfig);
    this.proto.tags = config.tags;
    this.dependOnDependencyAssertions = config.dependOnDependencyAssertions;
    this.dependencies(config.dependencyTargets);
    this.proto.fileName = config.filename;
    if (config.disabled) {
      this.proto.disabled = config.disabled;
    }

    const notebookContents = nativeRequire(config.filename).asJson;
    this.proto.notebookContents = JSON.stringify(
      stripNotebookOutputs(notebookContents, config.filename)
    );
  }

  public ipynb(contents: object): Notebook {
    this.proto.notebookContents = JSON.stringify(contents);
    return this;
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
    return dataform.Target.create(this.proto.target);
  }

  public compile() {
    return verifyObjectMatchesProto(
      dataform.Notebook,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
  }
}

function stripNotebookOutputs(
  notebookAsJson: { [key: string]: unknown },
  path: string
): { [key: string]: unknown } {
  if (!("cells" in notebookAsJson)) {
    throw new Error(`Notebook at ${path} is invalid: cells field not present`);
  }
  (notebookAsJson.cells as Array<{ [key: string]: unknown }>).forEach((cell, index) => {
    if ("outputs" in cell) {
      cell.outputs = [];
      (notebookAsJson.cells as Array<{ [key: string]: unknown }>)[index] = cell;
    }
  });
  return notebookAsJson;
}
