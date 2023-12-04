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
  }

  public setNotebookContents(contents: string) {
    this.proto.notebookContents = contents;
  }

  public getFileName() {
    return this.proto.config.fileName;
  }

  public getTarget() {
    return dataform.Target.create(this.proto.target);
  }

  public compile() {
    return verifyObjectMatchesProto(dataform.Notebook, this.proto);
  }
}
