import { ColumnDescriptors } from "@dataform/core/column_descriptors";
import { IColumnsDescriptor, IDocumentableConfig, ITargetableConfig } from "@dataform/core/common";
import { Session } from "@dataform/core/session";
import { checkExcessProperties, strictKeysOf } from "@dataform/core/utils";
import { dataform } from "@dataform/protos";
/**
 * Configuration options for `declaration` action types.
 */
export interface IDeclarationConfig extends IDocumentableConfig, ITargetableConfig {}

export const IDeclarationConfigProperties = strictKeysOf<IDeclarationConfig>()([
  "type",
  "name",
  "tags",
  "schema",
  "database",
  "columns",
  "description",
  "dependencies"
]);

/**
 * @hidden
 */
export class Declaration {
  public proto: dataform.IDeclaration = dataform.Declaration.create();

  public session: Session;

  public config(config: IDeclarationConfig) {
    checkExcessProperties(config, IDeclarationConfigProperties, "declaration config");
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
    this.proto.actionDescriptor.columns = ColumnDescriptors.mapToColumnProtoArray(columns);
    return this;
  }

  public compile() {
    return this.proto;
  }
}
