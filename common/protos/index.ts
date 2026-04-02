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

  // 1. Single Pass Create
  const proto = protoType.create(object);
  const probeObject = (protoType as any).toObject(proto, { defaults: true });

  // 2. Validate and Convert In-Place
  checkAndConvertFields(object, probeObject, proto, errorBehaviour, protoType);

  return proto;
}

function checkAndConvertFields(
  raw: { [k: string]: any },
  probe: { [k: string]: any },
  protoInstance: any,
  errorBehaviour: VerifyProtoErrorBehaviour,
  protoType: any
) {
  const docLinkPrefix = maybeGetDocsLinkPrefix(errorBehaviour, protoType);
  Object.entries(raw).forEach(([rawKey, rawValue]) => {
    if (rawValue === undefined) {
      return;
    }
    if (
      rawValue === null &&
      errorBehaviour === VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    ) {
      return;
    }

    let probeKey = rawKey;
    if (probe[rawKey] === undefined) {
      if (probe[toSnakeCase(rawKey)] !== undefined) {
        probeKey = toSnakeCase(rawKey);
      } else if (probe[toCamelCase(rawKey)] !== undefined) {
        probeKey = toCamelCase(rawKey);
      }
    }
    const probeValue = probe[probeKey];

    if (
      Array.isArray(probeValue) &&
      rawValue === null &&
      errorBehaviour === VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    ) {
      throw ReferenceError(`Unexpected empty value for "${rawKey}".${docLinkPrefix}`);
    }

    // Heuristic 1: Object Struct Detection
    if (
      typeof rawValue === "object" &&
      !Array.isArray(rawValue) &&
      probeValue &&
      typeof probeValue === "object" &&
      probeValue.fields &&
      typeof probeValue.fields === "object" &&
      Object.keys(probeValue.fields).length === 0 &&
      !rawValue.fields
    ) {
      protoInstance[probeKey] = unknownToValue(rawValue).structValue;
      return;
    }

    // Heuristic 2: Array List/Struct Detection
    if (
      Array.isArray(rawValue) &&
      rawValue.length > 0 &&
      probeValue &&
      Array.isArray(probeValue) &&
      probeValue.length === 0
    ) {
      protoInstance[probeKey] = {
        listValue: {
          values: rawValue.map(item => unknownToValue(item))
        }
      };
      return;
    }

    if (typeof probeValue !== typeof rawValue) {
      if (Array.isArray(rawValue) && rawValue.length === 0) {
        return;
      }
      if (!rawValue) {
        throw ReferenceError(
          `Unexpected empty value for "${rawKey}".${docLinkPrefix}`
        );
      }
      if (typeof rawValue === "object" && Object.keys(rawValue).length === 0) {
        return;
      }
      if (errorBehaviour === VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM) {
        throw ReferenceError(
          `Unexpected property "${rawKey}" for "${protoType
            .getTypeUrl("")
            .replace("/", "")}", please report this to the Dataform team at ${REPORT_ISSUE_URL}.`
        );
      }
      throw ReferenceError(
        `Unexpected property "${rawKey}", or property value type of "${typeof rawValue}" is incorrect.${docLinkPrefix}`
      );
    }

    if (typeof rawValue === "object" && rawValue !== null) {
      checkAndConvertFields(rawValue, probeValue, protoInstance[probeKey], errorBehaviour, protoType);
    }
  });
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
          Object.entries(raw).map(([key, value]) => [key, unknownToValue(value)])
        )
      }
    };
  }
  throw new Error(`Unsupported value: ${raw}`);
}



function toSnakeCase(str: string): string {
  return str.replace(/([a-z0-9])([A-Z])/g, "$1_$2").toLowerCase();
}

function toCamelCase(str: string): string {
  return str.replace(/_([a-z])/g, (_, char) => char.toUpperCase());
}
