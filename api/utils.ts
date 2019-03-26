import { requiredWarehouseProps, WarehouseTypes } from "@dataform/core/adapters";
import * as protos from "@dataform/protos";
import * as fs from "fs";

export function validateProfile(profileJson: any) {
  const errMsg = protos.Profile.verify(profileJson);
  if (errMsg) {
    throw new Error(`Profile JSON object does not conform to protobuf requirements: ${errMsg}`);
  }

  const profile = protos.Profile.create(profileJson);

  // profile shouldn't be empty
  if (!profile || !Object.keys(profile).length) {
    throw new Error("Profile is empty.");
  }

  // warehouse check
  const supportedWarehouses = Object.keys(WarehouseTypes).map(key => WarehouseTypes[key]);
  const warehouses = Object.keys(profile).filter(key => key !== "threads");

  if (warehouses.length === 0) {
    throw new Error(`Warehouse not specified.`);
  } else if (!warehouses.every(key => supportedWarehouses.indexOf(key) !== -1)) {
    const predefinedW = supportedWarehouses.map(item => `"${item}"`).join(" | ");
    throw new Error(
      `Unsupported warehouse detected. Should only use predefined warehouses: ${predefinedW}`
    );
  }

  // props check
  const warehouse = warehouses[0];
  const props = Object.keys(profile[warehouse]);
  const missingProps = requiredWarehouseProps[warehouse].filter(key => props.indexOf(key) === -1);

  if (missingProps.length > 0) {
    throw new Error(`Missing required properties: ${missingProps.join(", ")}`);
  }

  return profile;
}

export function readProfile(profilePath: string): protos.IProfile {
  if (!fs.existsSync(profilePath)) {
    throw new Error("Missing profile JSON file.");
  }
  return validateProfile(JSON.parse(fs.readFileSync(profilePath, "utf8")));
}
