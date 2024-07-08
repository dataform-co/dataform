import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { ColumnDescriptors } from "df/core/column_descriptors";
import { Session } from "df/core/session";
import { actionConfigToCompiledGraphTarget } from "df/core/utils";
import { dataform } from "df/protos/ts";

interface ILegacyDeclarationConfig extends dataform.ActionConfig.DeclarationConfig {
  database: string;
  schema: string;
  fileName: string;
  type: string;
}

/**
 * @hidden
 */
export class Declaration extends ActionBuilder<dataform.Declaration> {
  // TODO(ekrekr): make this field private, to enforce proto update logic to happen in this class.
  public proto: dataform.IDeclaration = dataform.Declaration.create();

  public session: Session;

  constructor(session?: Session, unverifiedConfig?: any) {
    super(session);
    this.session = session;

    if (!unverifiedConfig) {
      return;
    }

    const config = this.verifyConfig(unverifiedConfig);

    if (!config.name) {
      throw Error("Declarations must have a populated 'name' field.");
    }

    const target = actionConfigToCompiledGraphTarget(config);
    this.proto.target = this.applySessionToTarget(target, session.projectConfig);
    this.proto.canonicalTarget = this.applySessionToTarget(target, session.canonicalProjectConfig);

    if (config.description) {
      this.description(config.description);
    }
    if (config.columns?.length) {
      this.columns(
        config.columns.map(columnDescriptor =>
          dataform.ActionConfig.ColumnDescriptor.create(columnDescriptor)
        )
      );
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

  public columns(columns: dataform.ActionConfig.ColumnDescriptor[]) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.columns = ColumnDescriptors.mapConfigProtoToCompilationProto(
      columns
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
    return verifyObjectMatchesProto(
      dataform.Declaration,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
  }

  private verifyConfig(
    unverifiedConfig: ILegacyDeclarationConfig
  ): dataform.ActionConfig.DeclarationConfig {
    // The "type" field only exists on legacy declaration configs. Here we convert them to the new
    // format.
    if (unverifiedConfig.type) {
      delete unverifiedConfig.type;
      if (unverifiedConfig.database) {
        unverifiedConfig.project = unverifiedConfig.database;
        delete unverifiedConfig.database;
      }
      if (unverifiedConfig.schema) {
        unverifiedConfig.dataset = unverifiedConfig.schema;
        delete unverifiedConfig.schema;
      }
      if (unverifiedConfig.columns) {
        unverifiedConfig.columns = ColumnDescriptors.mapLegacyObjectToConfigProto(
          unverifiedConfig.columns as any
        );
      }
    }

    return verifyObjectMatchesProto(
      dataform.ActionConfig.DeclarationConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    );
  }
}
