import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

export async function test(
  graph: dataform.ICompiledGraph,
  credentials: Credentials
): Promise<dataform.ITestResult[]> {
  const dbadapter = dbadapters.create(credentials, graph.projectConfig.warehouse);
  return await Promise.all(graph.tests.map(testCase => runTest(dbadapter, testCase)));
}

async function runTest(
  dbadapter: dbadapters.DbAdapter,
  testCase: dataform.ITest
): Promise<dataform.ITestResult> {
  const [actualResults, expectedResults] = await Promise.all([
    dbadapter.execute(testCase.testQuery),
    dbadapter.execute(testCase.expectedOutputQuery)
  ]);

  // Check row counts.
  if (actualResults.length !== expectedResults.length) {
    return {
      name: testCase.name,
      successful: false,
      message: `Expected ${expectedResults.length} rows, but saw ${actualResults.length} rows.`
    };
  }
  if (actualResults.length === 0) {
    return {
      name: testCase.name,
      successful: true
    };
  }

  // Check column sets.
  const actualColumns = Object.keys(actualResults[0]);
  const expectedColumns = Object.keys(expectedResults[0]);
  if (actualColumns.length !== expectedColumns.length) {
    return {
      name: testCase.name,
      successful: false,
      message: `Expected columns "${expectedColumns}", but saw "${actualColumns}".`
    };
  }
  for (const expectedColumn of expectedColumns) {
    if (!actualColumns.some(actualColumn => actualColumn === expectedColumn)) {
      return {
        name: testCase.name,
        successful: false,
        message: `Expected columns "${expectedColumns}", but saw "${actualColumns}".`
      };
    }
  }

  // Check row contents.
  for (let i = 0; i < actualResults.length; i++) {
    const actualResultRow = actualResults[i];
    const expectedResultRow = expectedResults[i];

    for (const column of actualColumns) {
      if (actualResultRow[column] !== expectedResultRow[column]) {
        return {
          name: testCase.name,
          successful: false,
          message: `For row ${i} and column "${column}": expected "${expectedResultRow[column]}", but saw "${actualResultRow[column]}".`
        };
      }
    }
  }

  return {
    name: testCase.name,
    successful: true
  };
}
