import { util } from "protobufjs";

import { google } from "df/protos/ts";

const CONFIGS_PROTO_DOCUMENTATION_URL =
  "https://dataform-co.github.io/dataform/docs/configs-reference";
const REPORT_ISSUE_URL = "https://github.com/dataform-co/dataform/issues";



export interface IProtoClass<IProto, Proto> {
  new (): Proto;

  create(iProto?: IProto | Proto): Proto;

  encode(proto: IProto | Proto): { finish(): Uint8Array };
  decode(bytes: Uint8Array): Proto;

  toObject(proto: Proto): { [k: string]: any };
  fromObject(obj: { [k: string]: any }): Proto;

  getTypeUrl(prefix: string): string;
}

export enum VerifyProtoErrorBehaviour {
  DEFAULT,
  SUGGEST_REPORTING_TO_DATAFORM_TEAM,
  SHOW_DOCS_LINK
}

// This is a minimalist Typescript equivalent for the validation part of Profobuf's JsonFormat's
// mergeMessage method:
// https://github.com/protocolbuffers/protobuf/blob/670e0c2a0d0b64c994f743a73ee9b8926c47580d/java/util/src/main/java/com/google/protobuf/util/JsonFormat.java#L1455
// This is used because:
// * ProtobufJS's native verify method does not check that only defined fields are present.
// * Other protobuf libraries, such as ProtobufTS, incur significant performance hits.
// A key downside of using ProtobufJS is that it does not record the expected types of fields,
// meaning that the type of fields cannot be verified; an int can be confused with a string.
export function verifyObjectMatchesProto<Proto>(
  protoType: IProtoClass<any, Proto>,
  object: object,
  errorBehaviour: VerifyProtoErrorBehaviour = VerifyProtoErrorBehaviour.DEFAULT
): Proto {
  if (Array.isArray(object)) {
    throw ReferenceError(`Expected a top-level object, but found an array`);
  }

  // 1. First Pass (The Probe)
  const probeProto = protoType.create(object);
  const probeObject = (protoType as any).toObject(probeProto, { defaults: true });

  // 2. Detection
  const lostPaths = findLostPaths(object, probeObject);
  const lostPathsSet = new Set(lostPaths);

  // 3. Targeted Conversion
  const convertedObject = applyStructConversions(object, lostPathsSet);

  // 4. Final Build
  const finalProto = protoType.create(convertedObject);
  const finalProtoObject = protoType.toObject(finalProto);

  function checkFields(present: { [k: string]: any }, desired: { [k: string]: any }) {
    Object.entries(present).forEach(([presentKey, presentValue]) => {
      let desiredKey = presentKey;
      if (desired[presentKey] === undefined) {
        if (desired[toSnakeCase(presentKey)] !== undefined) {
          desiredKey = toSnakeCase(presentKey);
        } else if (desired[toCamelCase(presentKey)] !== undefined) {
          desiredKey = toCamelCase(presentKey);
        }
      }
      const desiredValue = desired[desiredKey];

      if (typeof desiredValue !== typeof presentValue) {
        if (Array.isArray(presentValue) && presentValue.length === 0) {
          return;
        }
        if (!presentValue) {
          throw ReferenceError(
            `Unexpected empty value for "${presentKey}".` +
              maybeGetDocsLinkPrefix(errorBehaviour, protoType)
          );
        }
        if (typeof presentValue === "object" && Object.keys(presentValue).length === 0) {
          return;
        }
        if (errorBehaviour === VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM) {
          throw ReferenceError(
            `Unexpected property "${presentKey}" for "${protoType
              .getTypeUrl("")
              .replace("/", "")}", please report this to the Dataform team at ` +
              `${REPORT_ISSUE_URL}.`
          );
        }
        throw ReferenceError(
          `Unexpected property "${presentKey}", or property value type of ` +
            `"${typeof presentValue}" is incorrect.` +
            maybeGetDocsLinkPrefix(errorBehaviour, protoType)
        );
      }
      if (typeof presentValue === "object") {
        checkFields(presentValue, desiredValue);
      }
    });
  }

  checkFields(convertedObject, finalProtoObject);
  return finalProto;
}

function applyStructConversions(obj: any, lostPaths: Set<string>, currentPath: string = ""): any {
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
    return obj;
  }

  const result = { ...obj };

  for (const key of Object.keys(obj)) {
    const value = obj[key];
    if (value === undefined || value === null) {
      continue;
    }

    const path = currentPath ? `${currentPath}.${key}` : key;

    if (lostPaths.has(path)) {
      if (Array.isArray(value)) {
        result[key] = {
          listValue: {
            values: value.map(item => unknownToValue(item))
          }
        };
      } else if (typeof value === "object" && !value.fields) {
        const converted = unknownToValue(value);
        result[key] = converted.structValue;
      }
    } else if (typeof value === "object") {
      result[key] = applyStructConversions(value, lostPaths, path);
    }
  }

  return result;
}

function maybeGetDocsLinkPrefix<Proto>(
  errorBehaviour: VerifyProtoErrorBehaviour,
  protoType: IProtoClass<any, Proto>
) {
  return errorBehaviour === VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    ? ` See ${CONFIGS_PROTO_DOCUMENTATION_URL}#${protoType
        .getTypeUrl("")
        // Clean up the proto type into its URL form.
        .replace(/\./g, "-")
        .replace(/\//, "")} for allowed properties.`
    : "";
}

export function encode64<IProto, Proto>(
  protoType: IProtoClass<IProto, Proto>,
  value: IProto | Proto = {} as IProto
): string {
  return toBase64(protoType.encode(protoType.create(value)).finish());
}

export function decode64<Proto>(protoType: IProtoClass<any, Proto>, encodedValue?: string): Proto {
  if (!encodedValue) {
    return protoType.create();
  }
  return protoType.decode(fromBase64(encodedValue));
}

export function equals<IProto, Proto>(
  protoType: IProtoClass<IProto, Proto>,
  valueA: IProto | Proto,
  valueB: IProto | Proto
): boolean {
  return encode64(protoType, valueA) === encode64(protoType, valueB);
}

export function deepClone<IProto, Proto>(
  protoType: IProtoClass<IProto, Proto>,
  value: IProto | Proto
) {
  return protoType.fromObject(protoType.toObject(protoType.create(value)));
}

function toBase64(value: Uint8Array): string {
  return util.base64.encode(value, 0, value.length);
}

function fromBase64(value: string): Uint8Array {
  const buf = new Uint8Array(util.base64.length(value));
  util.base64.decode(value, buf, 0);
  return buf;
}

export function unknownToValue(raw: unknown): google.protobuf.IValue {
  if (raw === null || typeof raw === "undefined") {
    return { nullValue: 0 };
  }
  if (typeof raw === "string") {
    return { stringValue: raw };
  }
  if (typeof raw === "number") {
    return { numberValue: raw };
  }
  if (typeof raw === "boolean") {
    return { boolValue: raw };
  }
  if (Array.isArray(raw)) {
    return { listValue: { values: raw.map(unknownToValue) } };
  }
  if (typeof raw === "object") {
    return {
      structValue: {
        fields: Object.fromEntries(
          Object.entries(raw as object).map(([key, value]) => [key, unknownToValue(value)])
        )
      }
    };
  }
  throw new Error(`Unsupported value: ${raw}`);
}

function findLostPaths(raw: any, probe: any, path: string = ""): string[] {
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return [];
  }

  let lost: string[] = [];

  for (const key of Object.keys(raw)) {
    const rawVal = raw[key];
    if (!rawVal || typeof rawVal !== "object" || Object.keys(rawVal).length === 0) {
      continue;
    }

    const currentPath = path ? `${path}.${key}` : key;
    const probeVal = probe ? (probe[key] || probe[toSnakeCase(key)] || probe[toCamelCase(key)]) : undefined;

    if (probeVal === undefined) {
      lost.push(currentPath);
    } else {
      // Heuristic 1: If raw is an object, probe has a 'fields' property that is an empty object,
      // and raw does not have 'fields', it's likely a plain JSON object for a Struct.
      // Heuristic 2: If raw is a non-empty array and probe is an empty array or missing, it's lost.
      if (
        typeof rawVal === "object" &&
        !Array.isArray(rawVal) &&
        probeVal &&
        typeof probeVal === "object" &&
        probeVal.fields &&
        typeof probeVal.fields === "object" &&
        Object.keys(probeVal.fields).length === 0 &&
        !rawVal.fields
      ) {
        lost.push(currentPath);
      } else if (
        Array.isArray(rawVal) &&
        rawVal.length > 0 &&
        (!probeVal || (Array.isArray(probeVal) && probeVal.length === 0))
      ) {
        lost.push(currentPath);
      } else {
        lost.push(...findLostPaths(rawVal, probeVal, currentPath));
      }
    }
  }

  return lost;
}

function toSnakeCase(str: string): string {
  return str.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
}

function toCamelCase(str: string): string {
  return str.replace(/_([a-z])/g, (_, char) => char.toUpperCase());
}
