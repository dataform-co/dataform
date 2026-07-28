import { google } from "df/protos/ts";

export type AnyValue = null | number | string | boolean | undefined | AnyValue[] | { [key: string]: AnyValue };

export class Structs {
  public static toObject(struct?: google.protobuf.IStruct): { [key: string]: AnyValue } | undefined {
    if (!struct || !struct.fields) {
      return undefined;
    }
    const result: { [key: string]: AnyValue } = {};
    for (const [key, value] of Object.entries(struct.fields)) {
      result[key] = this.fromValue(value);
    }
    return result;
  }

  public static fromObject(obj: { [key: string]: AnyValue }): google.protobuf.IStruct {
    const fields: { [key: string]: google.protobuf.IValue } = {};
    for (const [key, val] of Object.entries(obj)) {
      fields[key] = this.toValue(val);
    }
    return { fields };
  }

  private static fromValue(value: google.protobuf.IValue): AnyValue {
    if (value.nullValue !== null && value.nullValue !== undefined) {
      return null;
    }
    if (value.numberValue !== null && value.numberValue !== undefined) {
      return value.numberValue;
    }
    if (value.stringValue !== null && value.stringValue !== undefined) {
      return value.stringValue;
    }
    if (value.boolValue !== null && value.boolValue !== undefined) {
      return value.boolValue;
    }
    if (value.structValue !== null && value.structValue !== undefined) {
      return this.toObject(value.structValue);
    }
    if (value.listValue !== null && value.listValue !== undefined) {
      return (value.listValue.values || []).map((v: google.protobuf.IValue) => this.fromValue(v));
    }
    return undefined;
  }

  private static toValue(val: AnyValue): google.protobuf.IValue {
    if (typeof val === "number") {
      return { numberValue: val };
    }
    if (typeof val === "string") {
      return { stringValue: val };
    }
    if (typeof val === "boolean") {
      return { boolValue: val };
    }
    if (val === null || val === undefined) {
      return { nullValue: 0 };
    }
    if (Array.isArray(val)) {
      return {
        listValue: {
          values: val.map(v => this.toValue(v))
        }
      };
    }
    if (typeof val === "object") {
      return { structValue: this.fromObject(val as { [key: string]: AnyValue }) };
    }
    return { nullValue: 0 };
  }
}
