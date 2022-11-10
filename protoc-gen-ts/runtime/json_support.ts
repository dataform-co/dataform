import Long from "long";

interface IToJsonable {
  toJson(): any;
}

type FieldType = boolean | string | number | Long | Uint8Array | IToJsonable;

export function toJsonValue(value: FieldType | FieldType[]): any {
  if (typeof value === "boolean") {
    return value;
  }
  if (typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : String(value);
  }
  if (isLong(value)) {
    return value.toString();
  }
  if (Array.isArray(value)) {
    return value.map(singleValue => toJsonValue(singleValue));
  }
  if (value instanceof Uint8Array) {
    return Buffer.from(value).toString("base64");
  }
  if (value.toJson) {
    return value.toJson();
  }
  throw new Error(`Cannot convert ${value} to JSON.`);
}

function isLong(value: any): value is Long {
  return Long.isLong(value);
}
