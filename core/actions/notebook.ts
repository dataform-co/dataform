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

  constructor(session: Session, config: dataform.ActionConfig) {
    super(session);

    this.session = session;
    this.proto.config = config;

    this.proto.target = this.applySessionToTarget(this.proto.config.target);
    this.proto.config.target = this.applySessionCanonicallyToTarget(this.proto.config.target);
  }

  public notebookContents(notebookContents: string): Notebook {
    this.proto.notebookContents = notebookContents;
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
    return this.proto.config.fileName;
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
