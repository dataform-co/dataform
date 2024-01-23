import { verifyObjectMatchesProto } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import { nativeRequire } from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 */
export class Notebook extends ActionBuilder<dataform.Notebook> {
  public session: Session;

  // TODO: make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.INotebook = dataform.Notebook.create();

  constructor(session?: Session, config?: dataform.ActionConfig.NotebookConfig) {
    super(session);

    const target = dataform.Target.create({
      name: config.name || Path.fileName(config.filename),
      schema: config.location,
      database: config.project
    });

    this.session = session;
    this.proto.target = this.applySessionToTarget(target);
    this.proto.canonicalTarget = this.applySessionCanonicallyToTarget(target);
    this.proto.tags = config.tags;
    this.proto.dependencyTargets = config.dependencyTargets;
    this.proto.fileName = config.filename;
    this.proto.disabled = config.disabled;

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
