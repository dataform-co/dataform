import { util } from "protobufjs";

import { IStringifier } from "df/common/strings/stringifier";

export interface IProtoClass<IProto, Proto> {
  new (): Proto;

  create(iProto?: IProto | Proto): Proto;

  encode(proto: IProto | Proto): { finish(): Uint8Array };
  decode(bytes: Uint8Array): Proto;

  toObject(proto: Proto): { [k: string]: any };
  fromObject(obj: { [k: string]: any }): Proto;
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
  object: object
): Proto {
  if (Array.isArray(object)) {
    throw ReferenceError(`Expected a top-level object, but found an array`);
  }

  // Calling toObject on the object/JSON creates a version only contains the valid proto fields.
  const proto = protoType.create(object);
  const protoCastObject = protoType.toObject(proto);

  function checkFields(present: { [k: string]: any }, desired: { [k: string]: any }) {
    // Only the entries of `present` need to be iterated through as `desired` is guaranteed to be a
    // strict subset of `present`.
    Object.entries(present).forEach(([presentKey, presentValue]) => {
      const desiredValue = desired[presentKey];
      if (typeof desiredValue !== typeof presentValue) {
        if (Array.isArray(presentValue) && presentValue.length === 0) {
          // Empty arrays are assigned to empty proto array fields by ProtobufJS.
          return;
        }
        if (typeof presentValue === "object" && Object.keys(presentValue).length === 0) {
          // Empty objects are assigned to empty object fields by ProtobufJS.
          return;
        }
        throw ReferenceError(
          `Cannot find field: ${presentKey} in message, or value type is incorrect`
        );
      }
      if (typeof presentValue === "object") {
        checkFields(presentValue, desiredValue);
      }
    });
  }

  checkFields(object, protoCastObject);
  return proto;
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
