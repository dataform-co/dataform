import * as fs from "fs";
import * as protos from "@dataform/protos";
import { WarehouseTypes, requiredWarehouseProps } from "@dataform/core/adapters";

export function readProfile(profilePath: string): protos.IProfile {
  if (fs.existsSync(profilePath)) {
    return JSON.parse(fs.readFileSync(profilePath, "utf8"));
  }
  throw new Error("Missing profile JSON file.");
}

export function validateProfile(profile: protos.IProfile): void {
  // profile shouldn't be empty
  if (!profile || !Object.keys(profile).length) {
    throw new Error("Profile JSON file is empty.");
  }

  // warehouse check
  const supurtedWarehouses = Object.keys(WarehouseTypes).map(key => WarehouseTypes[key]);
  const warehouses = Object.keys(profile).filter(key => key !== "threads");

  if (warehouses.length === 0) {
    throw new Error(`Warehouse not specified.`);
  } else if (!warehouses.every(key => supurtedWarehouses.indexOf(key) !== -1)) {
    const predefinedW = supurtedWarehouses.map(item => `"${item}"`).join(" | ");
    throw new Error(`Unsupported warehouse detected. Should only use predefined warehouses: ${predefinedW}`);
  } else if (warehouses.length > 1) {
    throw new Error(`Multiple warehouses detected. Should be only one warehouse config.`);
  }

  // props check
  const warehouse = warehouses[0];
  const props = Object.keys(profile[warehouse]);
  const missingProps = requiredWarehouseProps[warehouse].filter(key => props.indexOf(key) === -1);

  if (missingProps.length > 0) {
    throw new Error(`Missing required properties: ${missingProps.join(", ")}`);
  }
}

export function getProfile(profilePath: string): protos.IProfile {
  const profile = readProfile(profilePath);

  validateProfile(profile);

  return protos.Profile.create(profile);
}
