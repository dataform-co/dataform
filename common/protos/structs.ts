import { util } from "protobufjs";

import { google } from "df/protos/ts";

export type AnyValue =
  null | number | string | boolean | undefined | AnyValue[] | { [key: string]: AnyValue };

const PROTO_VALUE_KEYS = new Set([
  "nullValue",
  "numberValue",
  "stringValue",
  "boolValue",
  "structValue",
  "listValue",
]);

let protoHandlersInstalled = false;
let plainStructSeen = false;
let mutatePlainStructsInPlace = false;

export class UnsupportedStructValueError extends TypeError {
  constructor(
    public readonly key: string | undefined,
    public readonly valueType: string,
  ) {
    super(
      `Unsupported Struct value of type "${valueType}"${key !== undefined ? ` at "${key}"` : ""}`,
    );
  }
}

export function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (
    !value ||
    typeof value !== "object" ||
    Object.prototype.toString.call(value) !== "[object Object]"
  ) {
    return false;
  }
  const proto = Object.getPrototypeOf(value);
  return proto === null || Object.getPrototypeOf(proto) === null;
}

export function isProtoMessageInstance(value: unknown): boolean {
  if (
    !value ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    (util.Long && value instanceof util.Long)
  ) {
    return false;
  }
  const ctor = value.constructor;
  return typeof ctor === "function" && typeof Reflect.get(ctor, "verify") === "function";
}

function strictJsonToProtoValue(val: unknown, key: string | undefined): google.protobuf.IValue {
  if (val === null || val === undefined) {
    return { nullValue: 0 };
  }
  if (typeof val === "number") {
    return { numberValue: val };
  }
  if (typeof val === "string") {
    return { stringValue: val };
  }
  if (typeof val === "boolean") {
    return { boolValue: val };
  }
  if (Array.isArray(val)) {
    return {
      listValue: {
        values: val.map((item) => strictJsonToProtoValue(item, key)),
      },
    };
  }
  if (isPlainObject(val)) {
    return { structValue: strictPlainObjectToStruct(val) };
  }
  throw new UnsupportedStructValueError(key, typeof val);
}

function setOwnProperty<T>(target: Record<string, T>, key: string, value: T): void {
  Object.defineProperty(target, key, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
}

function strictPlainObjectToStruct(obj: Record<string, unknown>): google.protobuf.IStruct {
  const fields: { [key: string]: google.protobuf.IValue } = {};
  for (const [key, val] of Object.entries(obj)) {
    setOwnProperty(fields, key, strictJsonToProtoValue(val, key));
  }
  return { fields };
}

function cloneRecord(record: object): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(record)) {
    setOwnProperty(out, key, cloneForProtoVerification(val));
  }
  return out;
}

function cloneForProtoVerification(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => cloneForProtoVerification(item));
  }
  if (
    !value ||
    typeof value !== "object" ||
    value instanceof google.protobuf.Struct ||
    value instanceof Uint8Array ||
    (util.Long && value instanceof util.Long)
  ) {
    return value;
  }
  if (!isPlainObject(value) && !isProtoMessageInstance(value)) {
    return value;
  }
  return cloneRecord(value);
}

function toStructCandidate(object: { [k: string]: unknown }): { [k: string]: unknown } {
  if (!object || typeof object !== "object" || Array.isArray(object)) {
    return object;
  }
  if (Structs.isWrappedStructObject(object)) {
    return object;
  }
  if (!isPlainObject(object)) {
    throw new UnsupportedStructValueError(undefined, typeof object);
  }
  plainStructSeen = true;
  const converted = strictPlainObjectToStruct(object);
  if (mutatePlainStructsInPlace) {
    for (const key of Object.keys(object)) {
      delete object[key];
    }
    object.fields = converted.fields;
    Object.setPrototypeOf(object, google.protobuf.Struct.prototype);
    return object;
  }
  return { fields: converted.fields };
}

export class Structs {
  public static toObject(
    struct?: google.protobuf.IStruct,
  ): { [key: string]: AnyValue } | undefined {
    if (!struct || !struct.fields) {
      return undefined;
    }
    const result: { [key: string]: AnyValue } = {};
    for (const [key, value] of Object.entries(struct.fields)) {
      setOwnProperty(result, key, this.fromValue(value));
    }
    return result;
  }

  public static fromObject(obj: { [key: string]: AnyValue }): google.protobuf.IStruct {
    const fields: { [key: string]: google.protobuf.IValue } = {};
    for (const [key, val] of Object.entries(obj)) {
      setOwnProperty(fields, key, this.toValue(val));
    }
    return { fields };
  }

  /**
   * Returns true if `object` is a `google.protobuf.Struct` instance or a plain `IStruct` literal
   * (`{ fields: { <key>: { <protoValueKey>: ... } } }`), which SQLX config blocks can pass
   * without importing the `Struct` constructor. Plain JSON payloads shaped identically to an
   * `IStruct` wrapper are intentionally treated as pre-wrapped.
   */
  public static isWrappedStructObject(object: unknown): boolean {
    if (object instanceof google.protobuf.Struct) {
      return true;
    }
    if (!isPlainObject(object)) {
      return false;
    }
    const keys = Object.keys(object);
    if (keys.length !== 1 || keys[0] !== "fields") {
      return false;
    }
    const fields = object.fields;
    if (!isPlainObject(fields)) {
      return false;
    }
    return Object.values(fields).every((entry) => {
      if (!isPlainObject(entry)) {
        return false;
      }
      const entryKeys = Object.keys(entry);
      return entryKeys.length === 1 && PROTO_VALUE_KEYS.has(entryKeys[0]);
    });
  }

  public static installNonMutatingProtoHandlers(): void {
    if (protoHandlersInstalled) {
      return;
    }
    protoHandlersInstalled = true;
    const Struct = google.protobuf.Struct;
    // Save references to the original generated methods to prevent infinite recursion.
    const originalVerify = Struct.verify;
    const originalFromObject = Struct.fromObject;

    Struct.verify = function (object: { [k: string]: unknown }) {
      if (!object || typeof object !== "object" || Array.isArray(object)) {
        return "object expected";
      }
      try {
        return originalVerify.call(this, toStructCandidate(object));
      } catch (err) {
        if (err instanceof UnsupportedStructValueError) {
          return err.message;
        }
        throw err;
      }
    };

    Struct.fromObject = function (object: { [k: string]: unknown }) {
      return originalFromObject.call(this, toStructCandidate(object));
    };
  }

  /**
   * Normalizes plain JSON `google.protobuf.Struct` fields via `protoType.fromObject` without
   * mutating `object`. When `object` contains no plain JSON `Struct` fields, returns `object`
   * directly without cloning; otherwise clones `object` first and normalizes on the clone.
   */
  public static normalizeForProtoVerification(
    protoType: { fromObject(obj: object): unknown },
    object: object,
  ): object {
    plainStructSeen = false;
    try {
      protoType.fromObject(object);
    } catch (err) {
      if (err instanceof UnsupportedStructValueError && err.key !== undefined) {
        throw err;
      }
    }
    if (!plainStructSeen) {
      return object;
    }

    const cloned = Array.isArray(object)
      ? object.map((item) => cloneForProtoVerification(item))
      : cloneRecord(object);
    mutatePlainStructsInPlace = true;
    try {
      protoType.fromObject(cloned);
    } catch (err) {
      if (err instanceof UnsupportedStructValueError && err.key !== undefined) {
        throw err;
      }
    } finally {
      mutatePlainStructsInPlace = false;
    }
    return cloned;
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
    return strictJsonToProtoValue(val, undefined);
  }
}
