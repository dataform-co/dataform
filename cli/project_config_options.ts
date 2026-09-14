import yargs from "yargs";

import { INamedOption } from "df/cli/yargswrapper";
import { dataform } from "df/protos/ts";

export interface IProjectConfigArgs {
  defaultDatabase?: string;
  defaultSchema?: string;
  defaultLocation?: string;
  assertionSchema?: string;
  vars?: { [key: string]: string };
  databaseSuffix?: string;
  schemaSuffix?: string;
  tablePrefix?: string;
  disableAssertions?: boolean;
  defaultReservation?: string;
}

export class ProjectConfigOptions {
  public static defaultDatabase: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "default-database",
    option: {
      describe:
        "The default database to use, equivalent to Google Cloud Project ID. If unset, " +
        "the value from workflow_settings.yaml is used.",
      type: "string"
    }
  };

  public static defaultSchema: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "default-schema",
    option: {
      describe:
        "Override for the default schema name. If unset, the value from workflow_settings.yaml is used."
    }
  };

  public static defaultLocation: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "default-location",
    option: {
      describe:
        "The default location to use. See " +
        "https://cloud.google.com/bigquery/docs/locations for supported values. If unset, the " +
        "value from workflow_settings.yaml is used."
    }
  };

  public static assertionSchema: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "assertion-schema",
    option: {
      describe: "Default assertion schema. If unset, the value from workflow_settings.yaml is used."
    }
  };

  public static databaseSuffix: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "database-suffix",
    option: {
      describe:
        "A suffix to be appended to output database names. If unset, the value from workflow_settings.yaml is used."
    }
  };

  public static vars: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "vars",
    option: {
      describe:
        "Override for variables to inject via '--vars=someKey=someValue,a=b', referenced by " +
        "`dataform.projectConfig.vars.someValue`.  If unset, the value from workflow_settings.yaml is used.",
      type: "string",
      default: null,
      coerce: (rawVarsString: string | null) => {
        const variables: { [key: string]: string } = {};
        rawVarsString?.split(",").forEach(keyValueStr => {
          const [key, value] = keyValueStr.split("=");
          variables[key] = value;
        });
        return variables;
      }
    }
  };

  public static schemaSuffix: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "schema-suffix",
    option: {
      describe:
        "A suffix to be appended to output schema names. If unset, the value from workflow_settings.yaml " +
        "is used."
    },
    check: (argv: yargs.Arguments<IProjectConfigArgs>) => {
      if (
        argv.schemaSuffix &&
        !/^[a-zA-Z_0-9]+$/.test(argv.schemaSuffix)
      ) {
        throw new Error(
          `--${ProjectConfigOptions.schemaSuffix.name} should contain only ` +
            `alphanumeric characters and/or underscores.`
        );
      }
    }
  };

  public static tablePrefix: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "table-prefix",
    option: {
      describe:
        "Adds a prefix for all table names. If unset, the value from workflow_settings.yaml is used."
    }
  };

  public static disableAssertions: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "disable-assertions",
    option: {
      describe:
        "Disables all assertions including built-in assertions (uniqueKey, nonNull, rowConditions) and manual assertions (type: assertion).",
      type: "boolean",
      default: false
    }
  };

  public static defaultReservation: INamedOption<yargs.Options, IProjectConfigArgs> = {
    name: "default-reservation",
    option: {
      describe:
        "The default BigQuery reservation to use for execution. If unset, the value from " +
        "workflow_settings.yaml is used. If neither is set, default BigQuery behavior applies.",
      type: "string"
    }
  };

  public static allYargsOptions: Array<INamedOption<yargs.Options, IProjectConfigArgs>> = [
    ProjectConfigOptions.defaultDatabase,
    ProjectConfigOptions.defaultSchema,
    ProjectConfigOptions.defaultLocation,
    ProjectConfigOptions.assertionSchema,
    ProjectConfigOptions.vars,
    ProjectConfigOptions.databaseSuffix,
    ProjectConfigOptions.schemaSuffix,
    ProjectConfigOptions.tablePrefix,
    ProjectConfigOptions.disableAssertions,
    ProjectConfigOptions.defaultReservation
  ];

  public static constructProjectConfigOverride(
    argv: yargs.Arguments<IProjectConfigArgs>
  ): dataform.IProjectConfig {
    const projectConfigOptions: dataform.IProjectConfig = {};

    if (argv.defaultDatabase) {
      projectConfigOptions.defaultDatabase = argv.defaultDatabase;
    }
    if (argv.defaultSchema) {
      projectConfigOptions.defaultSchema = argv.defaultSchema;
    }
    if (argv.defaultLocation) {
      projectConfigOptions.defaultLocation = argv.defaultLocation;
    }
    if (argv.assertionSchema) {
      projectConfigOptions.assertionSchema = argv.assertionSchema;
    }
    if (argv.vars) {
      projectConfigOptions.vars = argv.vars;
    }
    if (argv.databaseSuffix) {
      projectConfigOptions.databaseSuffix = argv.databaseSuffix;
    }
    if (argv.schemaSuffix) {
      projectConfigOptions.schemaSuffix = argv.schemaSuffix;
    }
    if (argv.tablePrefix) {
      projectConfigOptions.tablePrefix = argv.tablePrefix;
    }
    if (argv.disableAssertions) {
      projectConfigOptions.disableAssertions = argv.disableAssertions;
    }
    if (argv.defaultReservation) {
      projectConfigOptions.defaultReservation = argv.defaultReservation;
    }
    return projectConfigOptions;
  }
}
