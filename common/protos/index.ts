import { configure, IConversionOptions, util } from "protobufjs";

import {
  isPlainObject,
  isProtoMessageInstance,
  Structs,
  UnsupportedStructValueError,
} from "df/common/protos/structs";
import { google } from "df/protos/ts";

const CONFIGS_PROTO_DOCUMENTATION_URL =
  "https://dataform-co.github.io/dataform/docs/reference/configs";
const REPORT_ISSUE_URL = "https://github.com/dataform-co/dataform/issues";

/**
 * Whether `candidate` is a `Buffer` class belonging to a realm other than this one.
 *
 * A `Buffer` from another realm does not share this realm's `Uint8Array`, so its prototype fails
 * an `instanceof` check against it.
 */
export function isForeignBuffer(candidate: any): boolean {
  try {
    if (!candidate?.prototype) {
      return false;
    }
    return !(candidate.prototype instanceof Uint8Array);
  } catch (e) {
    // A `Buffer` we cannot even introspect is certainly not one of ours.
    return true;
  }
}

/**
 * Makes protobufjs encode into plain `Uint8Array`s rather than `Buffer`s when `bufferClass` does
 * not belong to this realm. Encoding through a `Buffer` from another realm is very slow, and the
 * encoded bytes are the same either way. When `Buffer` belongs to this realm this is a no-op, so
 * protobufjs keeps its native `Buffer` fast paths.
 *
 * See https://github.com/dataform-co/dataform/issues/2298.
 *
 * @returns whether protobufjs was reconfigured.
 */
export function avoidForeignBufferEncoding(bufferClass: any): boolean {
  if (!isForeignBuffer(bufferClass)) {
    return false;
  }
  (util as any).Buffer = null;
  configure();
  return true;
}

avoidForeignBufferEncoding(typeof global === "undefined" ? undefined : (global as any).Buffer);

export interface IProtoClass<IProto, Proto> {
  new (): Proto;

  create(iProto?: IProto | Proto): Proto;

  encode(proto: IProto | Proto): { finish(): Uint8Array };
  decode(bytes: Uint8Array): Proto;

  toObject(proto: Proto, options?: IConversionOptions): { [k: string]: any };
  fromObject(obj: { [k: string]: any }): Proto;

  verify(obj: any): string | null;

  getTypeUrl(prefix: string): string;
}

export enum VerifyProtoErrorBehaviour {
  DEFAULT,
  SUGGEST_REPORTING_TO_DATAFORM_TEAM,
  SHOW_DOCS_LINK,
}

Structs.installNonMutatingProtoHandlers();

// This is a minimalist Typescript equivalent for the validation part of Profobuf's JsonFormat's
// mergeMessage method:
// https://github.com/protocolbuffers/protobuf/blob/670e0c2a0d0b64c994f743a73ee9b8926c47580d/java/util/src/main/java/com/google/protobuf/util/JsonFormat.java#L1455
// This is used because:
// * ProtobufJS's native verify method does not check that only defined fields are present.
// * Other protobuf libraries, such as ProtobufTS, incur significant performance hits.
// A key downside of using ProtobufJS is that it does not record the expected types of fields,
// meaning that the type of fields cannot be verified; an int can be confused with a string.
function isVerifiableObject(value: unknown): boolean {
  return (
    Array.isArray(value) ||
    isPlainObject(value) ||
    isProtoMessageInstance(value) ||
    value instanceof Uint8Array ||
    Boolean(util.Long && value instanceof util.Long)
  );
}

function isNestedContainer(value: unknown): value is object {
  if (!value || typeof value !== "object" || value instanceof google.protobuf.Struct) {
    return false;
  }
  return Array.isArray(value) || isPlainObject(value) || isProtoMessageInstance(value);
}

function getOwnPropertyValue(target: object, key: string): unknown {
  if (!Object.prototype.hasOwnProperty.call(target, key)) {
    return undefined;
  }
  return Object.getOwnPropertyDescriptor(target, key)?.value;
}

export function verifyObjectMatchesProto<Proto>(
  protoType: IProtoClass<any, Proto>,
  object: object,
  errorBehaviour: VerifyProtoErrorBehaviour = VerifyProtoErrorBehaviour.DEFAULT,
): Proto {
  if (Array.isArray(object)) {
    throw ReferenceError(`Expected a top-level object, but found an array`);
  }

  let normalizedObject: object;
  try {
    normalizedObject = Structs.normalizeForProtoVerification(protoType, object);
  } catch (err) {
    if (err instanceof UnsupportedStructValueError) {
      throw formatUnexpectedPropertyError(protoType, err.key ?? "", err.valueType, errorBehaviour);
    }
    throw err;
  }
  // The generated toObject() reads every repeated message element, so null and undefined entries
  // are rejected before the cast. This runs on every compiled action, so paths are only built
  // when needed.
  function checkNoEmptyEntries(container: object, containerPath: string) {
    if (Array.isArray(container)) {
      for (let index = 0; index < container.length; index++) {
        const element = container[index];
        if (element === null || element === undefined) {
          throw formatEmptyValueError(protoType, `${containerPath}[${index}]`, errorBehaviour);
        }
        if (isNestedContainer(element)) {
          checkNoEmptyEntries(element, `${containerPath}[${index}]`);
        }
      }
      return;
    }
    for (const key of Object.keys(container)) {
      const value = Reflect.get(container, key);
      if (value === null) {
        throw formatEmptyValueError(protoType, key, errorBehaviour);
      }
      if (isNestedContainer(value)) {
        checkNoEmptyEntries(value, key);
      }
    }
  }

  checkNoEmptyEntries(normalizedObject, "");
  const proto = protoType.create(normalizedObject);
  const protoCastObject = protoType.toObject(proto, { arrays: true, objects: true });

  function checkFields(present: { [k: string]: any }, desired: { [k: string]: any }) {
    // Iterate caller-supplied entries; `protoCastObject` includes defined empty arrays and maps so
    // valid empty containers match `desired` while unknown keys remain absent.
    Object.entries(present).forEach(([presentKey, presentValue]) => {
      if (presentValue instanceof google.protobuf.Struct) {
        return;
      }
      const desiredValue = getOwnPropertyValue(desired, presentKey);
      if (presentValue === undefined) {
        // toObject() copies map entries as they are and leaves unset message fields out, so an
        // undefined value that was copied is an empty map entry.
        const isEmptyMapEntry =
          desiredValue === undefined && Object.prototype.hasOwnProperty.call(desired, presentKey);
        if (isEmptyMapEntry) {
          throw formatEmptyValueError(protoType, presentKey, errorBehaviour);
        }
        return;
      }
      if (typeof presentValue === "object" && !isVerifiableObject(presentValue)) {
        throw formatUnexpectedPropertyError(
          protoType,
          presentKey,
          typeof presentValue,
          errorBehaviour,
        );
      }
      if (
        typeof desiredValue !== typeof presentValue ||
        Array.isArray(desiredValue) !== Array.isArray(presentValue)
      ) {
        if (!presentValue) {
          throw formatEmptyValueError(protoType, presentKey, errorBehaviour);
        }
        throw formatUnexpectedPropertyError(
          protoType,
          presentKey,
          typeof presentValue,
          errorBehaviour,
        );
      }
      if (typeof presentValue === "object") {
        checkFields(presentValue, desiredValue);
      }
    });
  }

  checkFields(normalizedObject, protoCastObject);
  return proto;
}

function formatEmptyValueError<Proto>(
  protoType: IProtoClass<any, Proto>,
  propertyPath: string,
  errorBehaviour: VerifyProtoErrorBehaviour,
): ReferenceError {
  return ReferenceError(
    `Unexpected empty value for "${propertyPath}".` +
      maybeGetDocsLinkPrefix(errorBehaviour, protoType),
  );
}

function formatUnexpectedPropertyError<Proto>(
  protoType: IProtoClass<any, Proto>,
  propertyName: string,
  propertyType: string,
  errorBehaviour: VerifyProtoErrorBehaviour,
): ReferenceError {
  if (errorBehaviour === VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM) {
    return ReferenceError(
      `Unexpected property "${propertyName}" for "${protoType
        .getTypeUrl("")
        .replace("/", "")}", please report this to the Dataform team at ` + `${REPORT_ISSUE_URL}.`,
    );
  }
  return ReferenceError(
    `Unexpected property "${propertyName}", or property value type of ` +
      `"${propertyType}" is incorrect.` +
      maybeGetDocsLinkPrefix(errorBehaviour, protoType),
  );
}

function maybeGetDocsLinkPrefix<Proto>(
  errorBehaviour: VerifyProtoErrorBehaviour,
  protoType: IProtoClass<any, Proto>,
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
  value: IProto | Proto = {} as IProto,
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
  valueB: IProto | Proto,
): boolean {
  return encode64(protoType, valueA) === encode64(protoType, valueB);
}

export function deepClone<IProto, Proto>(
  protoType: IProtoClass<IProto, Proto>,
  value: IProto | Proto,
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
          Object.entries(raw).map(([key, value]) => [key, unknownToValue(value)]),
        ),
      },
    };
  }
  throw new Error(`Unsupported value: ${raw}`);
}
