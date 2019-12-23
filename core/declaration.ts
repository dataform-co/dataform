import { IColumnsDescriptor, mapToColumnProtoArray, Session } from "@dataform/core/session";
import { dataform } from "@dataform/protos";

export interface DConfig {
  description?: string;
  columns?: IColumnsDescriptor;
  database?: string;
  schema?: string;
}

export class Declaration {
  public proto: dataform.IDeclaration = dataform.Declaration.create();

  public session: Session;

  public config(config: DConfig) {
    if (config.description) {
      this.description(config.description);
    }
    if (config.columns) {
      this.columns(config.columns);
    }
    return this;
  }

  public description(description: string) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.description = description;
    return this;
  }

  public columns(columns: IColumnsDescriptor) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.columns = mapToColumnProtoArray(columns);
    return this;
  }

  public compile() {
    return this.proto;
  }
}
