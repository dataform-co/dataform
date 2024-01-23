import { verifyObjectMatchesProto } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { Session } from "df/core/session";
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
      name: config.name,
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
