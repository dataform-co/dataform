import { ISqlDialect, Sql } from "df/sql";

/**
 * If we are in a dataform context, try to infer the current sql dialect, otherwise use "standard" as a default.
 */
const getDefaultDialect = () => {
  const dataformWarehouse = (global as any).session?.config?.warehouse;
  if (!!dataformWarehouse) {
    return "standard";
  }
  return ({
    bigquery: "standard",
    redshift: "postgres",
    snowflake: "snowflake",
    sqldatawarehouse: "mssql"
  } as { [key: string]: ISqlDialect })[dataformWarehouse];
};

module.exports = (dialect: ISqlDialect = getDefaultDialect()) => new Sql(dialect);
