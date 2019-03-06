import * as fs from "fs";
import * as protos from "@dataform/protos";

export enum WarehouseTypes {
  BIGQUERY = "bigquery",
  REDSHIFT = "redshift",
  SNOWFLAKE = "snowflake"
}

export function readProfile(profilePath: string): protos.IProfile {
  return fs.existsSync(profilePath) ? JSON.parse(fs.readFileSync(profilePath, "utf8")) : null;
}

export function validateProfile(profile: protos.IProfile): void {
  // profile shouldn't be empty
  if (!profile || !Object.keys(profile).length) {
    throw new Error("Missing profile JSON file or file is empty.");
  }

  // warehouse check
  const warehouses = Object.keys(WarehouseTypes).map(key => WarehouseTypes[key]);
  if (!Object.keys(profile).every(key => warehouses.indexOf(key) !== -1)) {
    const predefinedW = warehouses.map(item => `"${item}"`).join(" | ");
    throw new Error(`Unsupported warehouse detected. Should only use predefined warehouses: ${predefinedW}`);
  }

  // props check
  const err = protos.Profile.verify(profile);
  if (err) {
    throw new Error(`Profile validation error: ${err}`);
  }
}

export function getProfile(profilePath: string): protos.IProfile {
  const profile = readProfile(profilePath);

  validateProfile(profile);

  return protos.Profile.create(profile);
}
