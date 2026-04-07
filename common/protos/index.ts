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

  toObject(proto: Proto, options?: any): { [k: string]: any };
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
  const probeObject = protoType.toObject(proto, { defaults: true });

  // 2. Validate and Convert In-Place
  checkAndConvertFields(object, probeObject, proto, { errorBehaviour, protoType });

  return proto;
}

interface IValidationContext<Proto = any> {
  errorBehaviour: VerifyProtoErrorBehaviour;
  protoType: IProtoClass<any, Proto>;
}

function checkAndConvertFields<Proto = any>(
  raw: { [k: string]: any },
  probe: { [k: string]: any },
  protoInstance: Proto,
  context: IValidationContext<Proto>
) {
  const docLinkPrefix = maybeGetDocsLinkPrefix(context.errorBehaviour, context.protoType);
  Object.entries(raw).forEach(([rawKey, rawValue]) => {
    if (rawValue === undefined) {
      return;
    }
    if (
      rawValue === null &&
      context.errorBehaviour === VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    ) {
      return;
    }

    const probeKey = rawKey;
    const probeValue = probe[probeKey];

    if (
      Array.isArray(probeValue) &&
      rawValue === null &&
      context.errorBehaviour === VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    ) {
      throw ReferenceError(`Unexpected empty value for "${rawKey}".${docLinkPrefix}`);
    }

    // Heuristic 1: Object Struct Detection
    if (isUnconvertedStruct(rawValue, probeValue)) {
      (protoInstance as any)[probeKey] = unknownToValue(rawValue).structValue;
      return;
    }

    // Heuristic 2: Array List/Struct Detection
    if (isUnconvertedList(rawValue, probeValue)) {
      (protoInstance as any)[probeKey] = {
        listValue: {
          values: rawValue.map((item: any) => unknownToValue(item))
        }
      };
      return;
    }

    if (typeof probeValue !== typeof rawValue) {
      // Ignore empty containers (arrays or objects) as they are valid fallback states.
      if (isEmptyObjectOrArray(rawValue)) {
        return;
      }
      if (!rawValue) {
        throw ReferenceError(
          `Unexpected empty value for "${rawKey}".${docLinkPrefix}`
        );
      }
      if (context.errorBehaviour === VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM) {
        throw ReferenceError(
          `Unexpected property "${rawKey}" for "${context.protoType
            .getTypeUrl("")
            .replace("/", "")}", please report this to the Dataform team at ${REPORT_ISSUE_URL}.`
        );
      }
      throw ReferenceError(
        `Unexpected property "${rawKey}", or property value type of "${typeof rawValue}" is incorrect.${docLinkPrefix}`
      );
    }

    if (typeof rawValue === "object" && rawValue !== null) {
      checkAndConvertFields(rawValue, probeValue, (protoInstance as any)[probeKey], context);
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

/**
 * Heuristic to detect if a raw value should be converted to a google.protobuf.Struct.
 * We use this heuristic because our static-stripped Protobuf build removes $type and
 * fields metadata at runtime, making reflection impossible.
 */
function isUnconvertedStruct(raw: any, probe: any): boolean {
  return (
    // 1. The user provided a plain object (not an array).
    typeof raw === "object" &&
    !Array.isArray(raw) &&
    // 2. The probe (created via ProtobufJS) is an object.
    probe &&
    typeof probe === "object" &&
    // 3. The probe has a 'fields' property which is an object. This is how
    // ProtobufJS represents an empty/default google.protobuf.Struct.
    probe.fields &&
    typeof probe.fields === "object" &&
    // 4. The 'fields' object is empty, indicating ProtobufJS couldn't map the raw data.
    Object.keys(probe.fields).length === 0 &&
    // 5. The user didn't already provide a pre-converted struct (with 'fields').
    !raw.fields
  );
}

/**
 * Heuristic to detect if a raw value should be converted to a google.protobuf.ListValue.
 * We use this heuristic because our static-stripped Protobuf build removes $type and
 * fields metadata at runtime, making reflection impossible.
 */
function isUnconvertedList(raw: any, probe: any): boolean {
  return (
    // 1. The user provided a non-empty array.
    Array.isArray(raw) &&
    raw.length > 0 &&
    // 2. The probe exists and is an array.
    probe &&
    Array.isArray(probe) &&
    // 3. The probe is empty. This suggests that the field is defined as a
    // google.protobuf.ListValue, and ProtobufJS fallback to an empty default
    // array because it couldn't map the raw array directly.
    probe.length === 0
  );
}

/**
 * Checks if a value is an empty array or an empty object.
 * We ignore these during type mismatch checks to allow users to provide empty
 * containers as valid fallback values.
 */
function isEmptyObjectOrArray(val: any): boolean {
  if (Array.isArray(val)) {
    return val.length === 0;
  }
  if (typeof val === "object" && val !== null) {
    return Object.keys(val).length === 0;
  }
  return false;
}
