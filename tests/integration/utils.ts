import * as dfapi from "@dataform/api";
import { create, IAdapter } from "@dataform/core/adapters";
import * as protos from "@dataform/protos";
import { expect } from "chai";
import { asPlainObject } from "df/tests/utils";
import * as fs from "fs";
import * as path from "path";

interface ITableInfo {
  schema: string;
  table: string;
  type: string;
}

interface ITestConfig {
  warehouse: string;
  profile: protos.IProfile;
  projectDir: string;
  projectConf: protos.IProjectConfig;
  defaultSchema: string;
  assertionSchema: string;
  adapter: IAdapter;
}

interface IExpectedResult {
  id: string;
  data: object[];
}

export function queryRun(sqlQuery: string, testConfig: ITestConfig) {
  dfapi.utils.validateProfile(testConfig.profile);
  const profile = protos.Profile.create(testConfig.profile);

  return dfapi.query.run(profile, sqlQuery, {
    projectDir: path.resolve(testConfig.projectDir)
  });
}

function getTarget(schema: string, table: string, testConfig: ITestConfig) {
  const target = testConfig.adapter.resolveTarget({ schema, name: table });

  return target.replace(/`/g, "\\`");
}

function deleteTables(tables: ITableInfo[], testConfig: ITestConfig) {
  return Promise.all(
    tables.map(item => {
      const target = { schema: item.schema, name: item.table };
      const query = testConfig.adapter.dropIfExists(target, item.type);
      const sqlDelete = query.replace(/`/g, "\\`");

      return queryRun(sqlDelete, testConfig);
    })
  );
}

function getTables(schema: string, testConfig: ITestConfig) {
  const tablesQuery = {
    bigquery: `
      select
        table_id as table,
        dataset_id as schema,
        case type when 2 then 'view' else 'table' end as type
      from ${schema}.__TABLES__`,
    redshift: `
      select
        table_name as table,
        table_schema as schema,
        case table_type when 'VIEW' then 'view' else 'table' end as type
      from information_schema.tables
      where table_schema != 'information_schema'
        and table_schema != 'pg_catalog'
        and table_schema != 'pg_internal'
        and table_schema = '${schema}'`,
    snowflake: `
      select
        table_name as "table",
        table_schema as "schema",
        case table_type when 'VIEW' then 'view' else 'table' end as "type"
      from information_schema.tables
      where LOWER(table_schema) != 'information_schema'
        and LOWER(table_schema) != 'pg_catalog'
        and LOWER(table_schema) != 'pg_internal'
        and LOWER(table_schema) = '${schema}'`
  };

  return queryRun(tablesQuery[testConfig.warehouse], testConfig);
}

function getData(expectedResult: IExpectedResult[], schema: string, testConfig: ITestConfig) {
  return Promise.all(
    expectedResult.map(item => {
      const target = getTarget(schema, item.id, testConfig);
      const sqlSelect = `select * from ${target}`;

      return queryRun(sqlSelect, testConfig);
    })
  );
}

export function getTestConfig(warehouse: string): ITestConfig {
  const profilePath = `df/test_profiles/${warehouse}.json`;
  const profile = fs.existsSync(profilePath)
    ? JSON.parse(fs.readFileSync(profilePath, "utf8"))
    : null;
  const projectDir = `df/examples/${warehouse}`;
  const projectConf = JSON.parse(fs.readFileSync(path.join(projectDir, "./dataform.json"), "utf8"));
  const adapter = create({ ...projectConf, gcloudProjectId: null });

  return {
    warehouse,
    profile,
    projectDir,
    projectConf,
    defaultSchema: projectConf.defaultSchema,
    assertionSchema: projectConf.assertionSchema,
    adapter
  };
}

export function getTestRunCommand(
  testConfig: ITestConfig,
  expectedResult: IExpectedResult[],
  incrementalLength: number
) {
  return async () => {
    // run the command
    const graph = await dfapi
      .compile({ projectDir: testConfig.projectDir })
      .then(cg => dfapi.build(cg, {}, testConfig.profile))
      .then(eg => dfapi.run(eg, testConfig.profile).resultPromise());

    expect(graph).to.have.property("ok").that.to.be.true;
    expect(graph)
      .to.have.property("nodes")
      .to.be.an("array").that.is.not.empty;

    // check for errors in database
    const data = await getData(expectedResult, testConfig.defaultSchema, testConfig);
    expectedResult.forEach((item, i) => {
      const isIncremental = item.id === "example_incremental";
      const dataLength = isIncremental ? incrementalLength : item.data.length;

      expect(data[i])
        .to.be.an("array")
        .that.have.lengthOf(dataLength);

      if (!isIncremental) {
        expect(asPlainObject(data[i])).to.have.deep.members(asPlainObject(item.data));
      }
    });
  };
}

export function getHookBefore(testConfig: ITestConfig) {
  return async () => {
    // get all tables in dataset
    const dTables = await getTables(testConfig.defaultSchema, testConfig);
    const aTables = await getTables(testConfig.assertionSchema, testConfig);

    // delete existing tables
    if (aTables.length > 0) {
      await deleteTables(aTables, testConfig);
    }
    if (dTables.length > 0) {
      await deleteTables(dTables, testConfig);
    }
  };
}
