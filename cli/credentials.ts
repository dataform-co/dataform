import { intQuestion, passwordQuestion, question, selectionQuestion } from "@dataform/cli/console";
import { actuallyResolve } from "@dataform/cli/util";
import { dataform } from "@dataform/protos";
import * as fs from "fs";

export function getBigQueryCredentials(): dataform.IBigQuery {
  const cloudCredentialsPath = actuallyResolve(
    question(
      "Please follow the instructions at https://docs.dataform.co/platform_guides/set_up_datawarehouse/\n" +
        "to create and download a private key from the Google Cloud Console in JSON format.\n" +
        "(You can delete this file after credential initialization is complete.)\n\n" +
        "Enter the path to your Google Cloud private key file:"
    )
  );
  if (!fs.existsSync(cloudCredentialsPath)) {
    throw new Error(`Google Cloud private key file "${cloudCredentialsPath}" does not exist!`);
  }
  const cloudCredentials = require(cloudCredentialsPath);
  const locationIndex = selectionQuestion("Enter the location of your datasets:", [
    "US (default)",
    "EU",
    "other"
  ]);
  let location = locationIndex === 0 ? "US" : "EU";
  if (locationIndex === 2) {
    location = question("Enter the location's region name (e.g. 'us-west2'):");
  }
  return {
    projectId: cloudCredentials.project_id,
    credentials: fs.readFileSync(cloudCredentialsPath, "utf8"),
    location
  };
}

export function getPostgresCredentials() {
  return getJdbcCredentials("Enter the hostname of your Postgres database:", 5432);
}

export function getRedshiftCredentials() {
  return getJdbcCredentials(
    "Enter the hostname of your Redshift instance (in the form 'name.id.region.redshift.amazonaws.com'):",
    5439
  );
}

export function getSQLDataWarehouseCredentials(): dataform.ISQLDataWarehouse {
  const server = question("Enter your server name (for example 'name.database.windows.net'):");
  const port = intQuestion("Enter your server port:", 1433);
  const username = question("Enter your datawarehouse user:");
  const password = passwordQuestion("Enter your datawarehouse password:");
  const database = question("Enter the database name:");

  return {
    server,
    port,
    username,
    password,
    database
  };
}

export function getSnowflakeCredentials(): dataform.ISnowflake {
  const accountId = question(
    "Enter your Snowflake account identifier, including region (for example 'myaccount.us-east-1'):"
  );
  const role = question("Enter your database role:");
  const username = question("Enter your database username:");
  const password = passwordQuestion("Enter your database password:");
  const databaseName = question("Enter the database name:");
  const warehouse = question("Enter your warehouse name:");
  return {
    accountId,
    role,
    username,
    password,
    databaseName,
    warehouse
  };
}

function getJdbcCredentials(hostQuestion: string, defaultPort: number): dataform.IJDBC {
  const host = question(hostQuestion);
  const port = intQuestion(
    "Enter the port that Dataform should connect to (leave blank to use default):",
    defaultPort
  );
  const username = question("Enter your database username:");
  const password = passwordQuestion("Enter your database password:");
  const databaseName = question("Enter the database name:");
  return {
    host,
    port,
    username,
    password,
    databaseName
  };
}
