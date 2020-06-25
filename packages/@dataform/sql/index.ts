import { ISqlDialect, Sql } from "df/sql";

const currentDataformDialect = ({
  bigquery: "standard",
  redshift: "postgres",
  snowflake: "snowflake",
  sqldatawarehouse: "mssql"
} as { [key: string]: ISqlDialect })[(global as any).session.config.warehouse];

module.exports = (dialect: ISqlDialect = currentDataformDialect) => new Sql(dialect);
