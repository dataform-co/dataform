import { Credentials } from "@dataform/api/commands/credentials";
import * as dbadapters from "@dataform/api/dbadapters";
import { dataform } from "@dataform/protos";

export async function test(
  credentials: Credentials,
  warehouse: string,
  tests: dataform.ITest[]
): Promise<dataform.ITestResult[]> {
  const dbadapter = dbadapters.create(credentials, warehouse);
  try {
    return await Promise.all(tests.map(testCase => runTest(dbadapter, testCase)));
  } finally {
    await dbadapter.close();
  }
}

async function runTest(
  dbadapter: dbadapters.IDbAdapter,
  testCase: dataform.ITest
): Promise<dataform.ITestResult> {
  // TODO: Test results are currently limited to 1000 rows.
  // We should paginate test results to remove this limit.
  const [actualResults, expectedResults] = await Promise.all([
    dbadapter.execute(testCase.testQuery, { maxResults: 1000 }),
    dbadapter.execute(testCase.expectedOutputQuery, { maxResults: 1000 })
  ]);

  // Check row counts.
  if (actualResults.length !== expectedResults.length) {
    return {
      name: testCase.name,
      successful: false,
      messages: [`Expected ${expectedResults.length} rows, but saw ${actualResults.length} rows.`]
    };
  }
  // If the result set is empty and the number of actual rows is equal to the number of expected rows
  // (asserted above), this test is therefore successful.
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
      messages: [`Expected columns "${expectedColumns}", but saw "${actualColumns}".`]
    };
  }
  // We assume: (a) column order does not matter, and (b) column names are unique.
  for (const expectedColumn of expectedColumns) {
    if (
      !actualColumns.some(
        actualColumn => normalizeColumnName(actualColumn) === normalizeColumnName(expectedColumn)
      )
    ) {
      return {
        name: testCase.name,
        successful: false,
        messages: [`Expected columns "${expectedColumns}", but saw "${actualColumns}".`]
      };
    }
  }

  // Check row contents.
  const rowMessages: string[] = [];
  for (let i = 0; i < actualResults.length; i++) {
    const actualResultRow = normalizeRow(actualResults[i]);
    const expectedResultRow = normalizeRow(expectedResults[i]);

    for (const column of actualColumns) {
      const normalizedColumn = normalizeColumnName(column);
      if (actualResultRow[normalizedColumn] !== expectedResultRow[normalizedColumn]) {
        rowMessages.push(
          `For row ${i} and column "${column}": expected "${
            expectedResultRow[normalizedColumn]
          }" (${typeof expectedResultRow[normalizedColumn]}), but saw "${
            actualResultRow[normalizedColumn]
          }" (${typeof actualResultRow[normalizedColumn]}).`
        );
      }
    }
  }
  if (rowMessages.length > 0) {
    return {
      name: testCase.name,
      successful: false,
      messages: rowMessages
    };
  }

  return {
    name: testCase.name,
    successful: true
  };
}

function normalizeColumnName(name: string) {
  return name.toUpperCase();
}

function normalizeRow(row: any) {
  const newRow: { [col: string]: any } = {};
  Object.keys(row).forEach(colName => {
    newRow[normalizeColumnName(colName)] = row[colName];
  });
  return newRow;
}
