import { verifyObjectMatchesProto } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import { actionConfigToCompiledGraphTarget, nativeRequire } from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 */
export class Notebook extends ActionBuilder<dataform.Notebook> {
  public session: Session;

  // TODO: make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.INotebook = dataform.Notebook.create();

  constructor(
    session?: Session,
    config?: dataform.ActionConfig.NotebookConfig,
    configPath?: string
  ) {
    super(session);

    if (!config.name) {
      config.name = Path.fileName(config.filename);
    }
    const target = actionConfigToCompiledGraphTarget(config);

    // Resolve the filename as its absolute path.
    config.filename = Path.join(Path.dirName(configPath), config.filename);

    this.session = session;
    this.proto.target = this.applySessionToTarget(target);
    this.proto.canonicalTarget = this.applySessionCanonicallyToTarget(target);
    this.proto.tags = config.tags;
    this.proto.dependencyTargets = config.dependencyTargets.map(dependencyTarget =>
      actionConfigToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
    );
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
    return verifyObjectMatchesProto(dataform.Notebook, this.proto);
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
