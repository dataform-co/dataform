import yargs from "yargs";

import { INamedOption } from "df/cli/yargswrapper";
import { dataform } from "df/protos/ts";

export class ProjectConfigOptions {
  public static defaultDatabase: INamedOption<yargs.Options> = {
    name: "default-database",
    option: {
      describe:
        "The default database to use, equivalent to Google Cloud Project ID. If unset, " +
        "the value from workflow_settings.yaml is used.",
      type: "string"
    }
  };

  public static defaultSchema: INamedOption<yargs.Options> = {
    name: "default-schema",
    option: {
      describe:
        "Override for the default schema name. If unset, the value from workflow_settings.yaml is used."
    }
  };

  public static defaultLocation: INamedOption<yargs.Options> = {
    name: "default-location",
    option: {
      describe:
        "The default location to use. See " +
        "https://cloud.google.com/bigquery/docs/locations for supported values. If unset, the " +
        "value from workflow_settings.yaml is used."
    }
  };

  public static assertionSchema: INamedOption<yargs.Options> = {
    name: "assertion-schema",
    option: {
      describe: "Default assertion schema. If unset, the value from workflow_settings.yaml is used."
    }
  };

  public static databaseSuffix: INamedOption<yargs.Options> = {
    name: "database-suffix",
    option: {
      describe: "Default assertion schema. If unset, the value from workflow_settings.yaml is used."
    }
  };

  public static vars: INamedOption<yargs.Options> = {
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

  public static schemaSuffix: INamedOption<yargs.Options> = {
    name: "schema-suffix",
    option: {
      describe:
        "A suffix to be appended to output schema names. If unset, the value from workflow_settings.yaml " +
        "is used."
    },
    check: (argv: yargs.Arguments<any>) => {
      if (
        argv[ProjectConfigOptions.schemaSuffix.name] &&
        !/^[a-zA-Z_0-9]+$/.test(argv[ProjectConfigOptions.schemaSuffix.name])
      ) {
        throw new Error(
          `--${ProjectConfigOptions.schemaSuffix.name} should contain only ` +
            `alphanumeric characters and/or underscores.`
        );
      }
    }
  };

  public static tablePrefix: INamedOption<yargs.Options> = {
    name: "table-prefix",
    option: {
      describe:
        "Adds a prefix for all table names. If unset, the value from workflow_settings.yaml is used."
    }
  };

  public static disableAssertions: INamedOption<yargs.Options> = {
    name: "disable-assertions",
    option: {
      describe:
        "Disables all assertions including built-in assertions (uniqueKey, nonNull, rowConditions) and manual assertions (type: assertion).",
      type: "boolean",
      default: false
    }
  };

  public static defaultReservation: INamedOption<yargs.Options> = {
    name: "default-reservation",
    option: {
      describe:
        "The default BigQuery reservation to use for execution. If unset, the value from " +
        "workflow_settings.yaml is used. If neither is set, default BigQuery behavior applies.",
      type: "string"
    }
  };

  public static allYargsOptions = [
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
    argv: yargs.Arguments<any>
  ): dataform.IProjectConfig {
    const projectConfigOptions: dataform.IProjectConfig = {};

    if (argv[ProjectConfigOptions.defaultDatabase.name]) {
      projectConfigOptions.defaultDatabase = argv[ProjectConfigOptions.defaultDatabase.name];
    }
    if (argv[ProjectConfigOptions.defaultSchema.name]) {
      projectConfigOptions.defaultSchema = argv[ProjectConfigOptions.defaultSchema.name];
    }
    if (argv[ProjectConfigOptions.defaultLocation.name]) {
      projectConfigOptions.defaultLocation = argv[ProjectConfigOptions.defaultLocation.name];
    }
    if (argv[ProjectConfigOptions.assertionSchema.name]) {
      projectConfigOptions.assertionSchema = argv[ProjectConfigOptions.assertionSchema.name];
    }
    if (argv[ProjectConfigOptions.vars.name]) {
      projectConfigOptions.vars = argv[ProjectConfigOptions.vars.name];
    }
    if (argv[ProjectConfigOptions.databaseSuffix.name]) {
      projectConfigOptions.databaseSuffix = argv[ProjectConfigOptions.databaseSuffix.name];
    }
    if (argv[ProjectConfigOptions.schemaSuffix.name]) {
      projectConfigOptions.schemaSuffix = argv[ProjectConfigOptions.schemaSuffix.name];
    }
    if (argv[ProjectConfigOptions.tablePrefix.name]) {
      projectConfigOptions.tablePrefix = argv[ProjectConfigOptions.tablePrefix.name];
    }
    if (argv[ProjectConfigOptions.disableAssertions.name]) {
      projectConfigOptions.disableAssertions = argv[ProjectConfigOptions.disableAssertions.name];
    }
    if (argv[ProjectConfigOptions.defaultReservation.name]) {
      projectConfigOptions.defaultReservation = argv[ProjectConfigOptions.defaultReservation.name];
    }
    return projectConfigOptions;
  }
}
