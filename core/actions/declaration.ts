import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { ColumnDescriptors } from "df/core/column_descriptors";
import { Session } from "df/core/session";
import { actionConfigToCompiledGraphTarget } from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 * @deprecated
 * This maintains backwards compatability with older versions.
 * TODO(ekrekr): consider breaking backwards compatability of these in v4.
 */
interface ILegacyDeclarationConfig extends dataform.ActionConfig.DeclarationConfig {
  database: string;
  schema: string;
  fileName: string;
  type: string;
}

/**
 * You can declare any BigQuery table as a data source in Dataform. Declaring BigQuery data
 * sources that are external to Dataform lets you treat those data sources as Dataform objects.
 *
 * Declaring data sources is optional, but can be useful when you want to do the following:
 * * Reference or resolve declared sources in the same way as any other table in Dataform.
 * * View declared sources in the visualized Dataform graph.
 * * Use Dataform to manage the table-level and column-level descriptions of externally created
 *   tables.
 * * Trigger workflow invocations that include all the dependents of an external data source.
 *
 * You can create declarations in the following ways. Available config options are defined in
 * [DeclarationConfig](configs#dataform-ActionConfig-DeclarationConfig), and are shared across all
 * the followiing ways of creating declarations.
 *
 * **Using a SQLX file:**
 *
 * ```sql
 * -- definitions/name.sqlx
 * config {
 *   type: "declaration"
 * }
 * -- Note: no SQL should be present.
 * ```
 *
 * **Using action configs files:**
 *
 * ```yaml
 * # definitions/actions.yaml
 * actions:
 * - declare:
 *   name: name
 * ```
 *
 * **Using the Javascript API:**
 *
 * ```js
 * // definitions/file.js
 * declare("name")
 * ```
 */
export class Declaration extends ActionBuilder<dataform.Declaration> {
  /** @hidden Hold a reference to the Session instance. */
  public session: Session;

  /**
   * @hidden Stores the generated proto for the compiled graph.
   */
  private proto = dataform.Declaration.create();

  /** @hidden */
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

  /**
   * @deprecated Deprecated in favor of
   * [DeclarationConfig.description](configs#dataform-ActionConfig-DeclarationConfig).
   *
   * Sets the description of this assertion.
   */
  public description(description: string) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.description = description;
    return this;
  }

  /**
   * @deprecated Deprecated in favor of
   * [DeclarationConfig.columns](configs#dataform-ActionConfig-DeclarationConfig).
   *
   * Sets the column descriptors of columns in this table.
   */
  public columns(columns: dataform.ActionConfig.ColumnDescriptor[]) {
    if (!this.proto.actionDescriptor) {
      this.proto.actionDescriptor = {};
    }
    this.proto.actionDescriptor.columns = ColumnDescriptors.mapConfigProtoToCompilationProto(
      columns
    );
    return this;
  }

  /** @hidden */
  public getFileName() {
    return this.proto.fileName;
  }

  /** @hidden */
  public getTarget() {
    return dataform.Target.create(this.proto.target);
  }

  /** @hidden */
  public setFilename(filename: string) {
    return (this.proto.fileName = filename);
  }

  /** @hidden */
  public compile() {
    return verifyObjectMatchesProto(
      dataform.Declaration,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
  }

  /**
   * @hidden Verify config checks that the constructor provided config matches the expected proto
   * structure, or the previously accepted legacy structure. If the legacy structure is used, it is
   * converted to the new structure.
   */
  private verifyConfig(
    unverifiedConfig: ILegacyDeclarationConfig
  ): dataform.ActionConfig.DeclarationConfig {
    if (unverifiedConfig.database) {
      unverifiedConfig.project = unverifiedConfig.database;
      delete unverifiedConfig.database;
    }
    if (unverifiedConfig.schema) {
      unverifiedConfig.dataset = unverifiedConfig.schema;
      delete unverifiedConfig.schema;
    }
    if (unverifiedConfig.columns) {
      // TODO(ekrekr) columns in their current config format are a difficult structure to represent
      // as protos. They are nested, and use the object keys as the names. Consider a forced
      // migration to the proto style column names.
      unverifiedConfig.columns = ColumnDescriptors.mapLegacyObjectToConfigProto(
        unverifiedConfig.columns as any
      );
    }

    // TODO(ekrekr): consider moving this to a shared location after all action builders have proto
    // config verifiers.
    if (unverifiedConfig.type) {
      delete unverifiedConfig.type;
    }

    return verifyObjectMatchesProto(
      dataform.ActionConfig.DeclarationConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    );
  }
}
