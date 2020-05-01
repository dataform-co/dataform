import { ColumnDescriptors } from "df/core/column_descriptors";
import { IColumnsDescriptor, IDocumentableConfig, ITargetableConfig } from "df/core/common";
import { Session } from "df/core/session";
import { checkExcessProperties, strictKeysOf } from "df/core/utils";
import { dataform } from "df/protos";
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
    checkExcessProperties(
      (e: Error) => this.session.compileError(e),
      config,
      IDeclarationConfigProperties,
      "declaration config"
    );
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
    this.proto.actionDescriptor.columns = ColumnDescriptors.mapToColumnProtoArray(
      columns,
      (e: Error) => this.session.compileError(e)
    );
    return this;
  }

  public compile() {
    return this.proto;
  }
}
