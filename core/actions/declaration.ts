import { verifyObjectMatchesProto } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { ColumnDescriptors } from "df/core/column_descriptors";
import {
  IColumnsDescriptor,
  IDocumentableConfig,
  INamedConfig,
  ITargetableConfig
} from "df/core/common";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  checkExcessProperties,
  strictKeysOf
} from "df/core/utils";
import { dataform } from "df/protos/ts";

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
export class Declaration extends ActionBuilder<dataform.Declaration> {
  // TODO(ekrekr): make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.IDeclaration = dataform.Declaration.create();

  public session: Session;

  constructor(session?: Session, config?: dataform.ActionConfig.DeclarationConfig) {
    super(session);
    this.session = session;

    if (!config) {
      return;
    }

    if (!config.name) {
      throw Error("Declarations must have a populated 'name' field.");
    }

    const target = actionConfigToCompiledGraphTarget(config);
    this.proto.target = this.applySessionToTarget(target);
    this.proto.canonicalTarget = this.applySessionCanonicallyToTarget(target);

    // TODO(ekrekr): load config proto column descriptors.
    this.config({ description: config.description });
  }

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
    // ProtobufJS isn't strict with the fields available on protos, so we validate this here.
    return verifyObjectMatchesProto(dataform.Declaration, this.proto);
  }
}
