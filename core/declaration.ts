import { ColumnDescriptors } from "df/core/column_descriptors";
import {
  IActionConfig,
  IColumnsDescriptor,
  IDocumentableConfig,
  INamedConfig,
  ITargetableConfig
} from "df/core/common";
import { Session } from "df/core/session";
import { checkExcessProperties, strictKeysOf } from "df/core/utils";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";
/**
 * Configuration options for `declaration` action types.
 */
export interface IDeclarationConfig extends IDocumentableConfig, INamedConfig, ITargetableConfig {}

export const IDeclarationConfigProperties = strictKeysOf<IDeclarationConfig>()([
  "columns",
  "database",
  "description",
  "name",
  "schema",
  "type"
]);

/**
 * @hidden
 */
export class Declaration {
  public proto: core.Declaration = core.Declaration.create();

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
    this.proto.actionDescriptor.description = description;
    return this;
  }

  public columns(columns: IColumnsDescriptor) {
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
