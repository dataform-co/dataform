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

export interface IProtoMessageType {
  readonly prototype: object;
  fromObject(object: { [k: string]: unknown }): unknown;
  verify(object: unknown): string | null;
}

interface IStructFieldMeta {
  kind: "none" | "struct" | "message" | "repeated" | "map";
  childProto?: IProtoMessageType;
  isChildStruct?: boolean;
}

const structFieldMetaCache = new WeakMap<object, Map<string, IStructFieldMeta>>();

function toStructCandidate(object: unknown): unknown {
  if (!object || typeof object !== "object" || Array.isArray(object)) {
    return object;
  }
  if (Structs.isWrappedStructObject(object)) {
    return object;
  }
  return Structs.fromObject(object as { [key: string]: AnyValue });
}

function probeChildProto(
  protoCtor: IProtoMessageType,
  sampleInput: { [key: string]: unknown },
  extractSample: (probe: Record<string, unknown>) => unknown,
): { childProto: IProtoMessageType; isStruct: boolean } | undefined {
  try {
    const probe = protoCtor.fromObject(sampleInput);
    if (!probe || typeof probe !== "object") {
      return undefined;
    }
    const candidate = extractSample(probe as Record<string, unknown>);
    if (
      candidate &&
      typeof candidate === "object" &&
      !(util.Long && candidate instanceof util.Long)
    ) {
      const ctor = (candidate as { constructor?: Partial<IProtoMessageType> }).constructor;
      if (ctor && typeof ctor.fromObject === "function" && typeof ctor.verify === "function") {
        return {
          childProto: ctor as IProtoMessageType,
          isStruct: candidate instanceof google.protobuf.Struct,
        };
      }
    }
  } catch {
    // Non-message fields (such as int64, bytes or enums) throw when probed with `{}`.
  }
  return undefined;
}

function getStructFieldMeta(protoCtor: IProtoMessageType, key: string): IStructFieldMeta {
  let ctorCache = structFieldMetaCache.get(protoCtor);
  if (!ctorCache) {
    ctorCache = new Map<string, IStructFieldMeta>();
    structFieldMetaCache.set(protoCtor, ctorCache);
  }
  const cached = ctorCache.get(key);
  if (cached) {
    return cached;
  }

  const descriptor = Object.getOwnPropertyDescriptor(protoCtor.prototype, key);
  if (
    !descriptor ||
    descriptor.get !== undefined ||
    descriptor.set !== undefined ||
    typeof descriptor.value === "function"
  ) {
    const noneMeta: IStructFieldMeta = { kind: "none" };
    ctorCache.set(key, noneMeta);
    return noneMeta;
  }

  const defaultVal = descriptor.value;
  let meta: IStructFieldMeta = { kind: "none" };

  if (Array.isArray(defaultVal)) {
    const childInfo = probeChildProto(protoCtor, { [key]: [{}] }, (probe) => {
      const arr = probe[key];
      return Array.isArray(arr) ? arr[0] : undefined;
    });
    if (childInfo) {
      meta = {
        kind: "repeated",
        childProto: childInfo.childProto,
        isChildStruct: childInfo.isStruct,
      };
    }
  } else if (defaultVal === util.emptyObject) {
    const childInfo = probeChildProto(protoCtor, { [key]: { k: {} } }, (probe) => {
      const mapObj = probe[key];
      return mapObj && typeof mapObj === "object"
        ? (mapObj as Record<string, unknown>).k
        : undefined;
    });
    if (childInfo) {
      meta = {
        kind: "map",
        childProto: childInfo.childProto,
        isChildStruct: childInfo.isStruct,
      };
    }
  } else if (defaultVal === null) {
    const childInfo = probeChildProto(protoCtor, { [key]: {} }, (probe) => probe[key]);
    if (childInfo?.isStruct) {
      meta = { kind: "struct", childProto: childInfo.childProto };
    } else if (childInfo?.childProto) {
      meta = { kind: "message", childProto: childInfo.childProto };
    }
  }

  ctorCache.set(key, meta);
  return meta;
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

  public static isWrappedStructObject(object: unknown): boolean {
    if (object instanceof google.protobuf.Struct) {
      return true;
    }
    if (!object || typeof object !== "object" || Array.isArray(object)) {
      return false;
    }
    const keys = Object.keys(object);
    if (keys.length !== 1 || keys[0] !== "fields") {
      return false;
    }
    const fields = (object as { fields?: unknown }).fields;
    if (!fields || typeof fields !== "object" || Array.isArray(fields)) {
      return false;
    }
    return Object.values(fields).every(
      (entry) =>
        Boolean(entry) &&
        typeof entry === "object" &&
        !Array.isArray(entry) &&
        Object.keys(entry as object).every((entryKey) => PROTO_VALUE_KEYS.has(entryKey)),
    );
  }

  public static installNonMutatingProtoHandlers(): void {
    const Struct = google.protobuf.Struct;
    const originalVerify = Struct.verify;
    const originalFromObject = Struct.fromObject;

    Struct.verify = function (object: { [k: string]: unknown }) {
      return originalVerify.call(this, toStructCandidate(object));
    };

    Struct.fromObject = function (object: { [k: string]: unknown }) {
      return originalFromObject.call(this, toStructCandidate(object) as { [k: string]: unknown });
    };
  }

  /**
   * Returns an object suitable for `protoType.create(...)` where any plain-JSON
   * `google.protobuf.Struct` fields are converted into `google.protobuf.Struct` instances on
   * shallow-cloned ancestor paths without mutating the caller's input object in place.
   */
  public static normalizePlainStructsNonMutating<T extends object>(
    protoCtor: IProtoMessageType,
    presentObject: T,
  ): T {
    if (!presentObject || typeof presentObject !== "object" || Array.isArray(presentObject)) {
      return presentObject;
    }

    let clonedObject: Record<string, unknown> | undefined;

    for (const [key, presentValue] of Object.entries(presentObject)) {
      if (!presentValue || typeof presentValue !== "object") {
        continue;
      }

      const meta = getStructFieldMeta(protoCtor, key);
      if (meta.kind === "none") {
        continue;
      }

      if (meta.kind === "struct") {
        if (!Array.isArray(presentValue) && !Structs.isWrappedStructObject(presentValue)) {
          clonedObject = clonedObject ?? { ...(presentObject as Record<string, unknown>) };
          clonedObject[key] = google.protobuf.Struct.fromObject(
            presentValue as Record<string, unknown>,
          );
        }
        continue;
      }

      if (meta.kind === "message" && meta.childProto && !Array.isArray(presentValue)) {
        const normalizedChild = Structs.normalizePlainStructsNonMutating(
          meta.childProto,
          presentValue,
        );
        if (normalizedChild !== presentValue) {
          clonedObject = clonedObject ?? { ...(presentObject as Record<string, unknown>) };
          clonedObject[key] = normalizedChild;
        }
        continue;
      }

      if (meta.kind === "repeated" && Array.isArray(presentValue) && meta.childProto) {
        let clonedArray: unknown[] | undefined;
        for (let i = 0; i < presentValue.length; i++) {
          const elem: unknown = presentValue[i];
          if (!elem || typeof elem !== "object" || Array.isArray(elem)) {
            continue;
          }
          if (meta.isChildStruct) {
            if (!Structs.isWrappedStructObject(elem)) {
              clonedArray = clonedArray ?? presentValue.slice();
              clonedArray[i] = google.protobuf.Struct.fromObject(elem as Record<string, unknown>);
            }
          } else {
            const normalizedElem = Structs.normalizePlainStructsNonMutating(meta.childProto, elem);
            if (normalizedElem !== elem) {
              clonedArray = clonedArray ?? presentValue.slice();
              clonedArray[i] = normalizedElem;
            }
          }
        }
        if (clonedArray) {
          clonedObject = clonedObject ?? { ...(presentObject as Record<string, unknown>) };
          clonedObject[key] = clonedArray;
        }
        continue;
      }

      if (meta.kind === "map" && !Array.isArray(presentValue) && meta.childProto) {
        const presentMap = presentValue as Record<string, unknown>;
        let clonedMap: Record<string, unknown> | undefined;
        for (const [mapKey, mapVal] of Object.entries(presentMap)) {
          if (!mapVal || typeof mapVal !== "object" || Array.isArray(mapVal)) {
            continue;
          }
          if (meta.isChildStruct) {
            if (!Structs.isWrappedStructObject(mapVal)) {
              clonedMap = clonedMap ?? { ...presentMap };
              clonedMap[mapKey] = google.protobuf.Struct.fromObject(
                mapVal as Record<string, unknown>,
              );
            }
          } else {
            const normalizedMapVal = Structs.normalizePlainStructsNonMutating(
              meta.childProto,
              mapVal,
            );
            if (normalizedMapVal !== mapVal) {
              clonedMap = clonedMap ?? { ...presentMap };
              clonedMap[mapKey] = normalizedMapVal;
            }
          }
        }
        if (clonedMap) {
          clonedObject = clonedObject ?? { ...(presentObject as Record<string, unknown>) };
          clonedObject[key] = clonedMap;
        }
      }
    }

    return (clonedObject as T | undefined) ?? presentObject;
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
          values: val.map((v) => this.toValue(v)),
        },
      };
    }
    if (typeof val === "object") {
      return { structValue: this.fromObject(val as { [key: string]: AnyValue }) };
    }
    return { nullValue: 0 };
  }
}
