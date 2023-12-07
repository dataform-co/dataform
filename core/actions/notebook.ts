import { verifyObjectMatchesProto } from "df/common/protos";
import { IActionBuilder } from "df/core/actions";
import { Session } from "df/core/session";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 */
export class Notebook implements IActionBuilder<dataform.Notebook> {
  public session: Session;

  // TODO: make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.INotebook = dataform.Notebook.create();

  constructor(session: Session, config: dataform.ActionConfig) {
    this.session = session;
    this.proto.config = config;

    // TODO(ekrekr): move this to an Action Builder utility method, once configs are on all protos.
    const canonicalTarget = this.proto.config.target;
    this.proto.config.target = dataform.Target.create({
      name: canonicalTarget.name,
      schema: canonicalTarget.schema || session.canonicalConfig.defaultSchema,
      database: canonicalTarget.database || session.canonicalConfig.defaultDatabase
    });
    this.proto.target = dataform.Target.create({
      name: canonicalTarget.name,
      schema: canonicalTarget.schema || session.config.defaultSchema,
      database: canonicalTarget.database || session.config.defaultDatabase
    });
  }

  public notebookContents(notebookContents: string) {
    this.proto.notebookContents = notebookContents;
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
