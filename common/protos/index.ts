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

// ProtobufJS's native verify method does not check that only defined fields are present.
// TODO(ekrekr): swap to Typescript protobuf library rather than having to do this.
export function verifyObjectMatchesProto<Proto>(
  protoType: IProtoClass<any, Proto>,
  object: object
): Proto {
  // Create a version of the object that only contains the valid proto fields.
  // ProtobufJS doesn't care about the types of the values of fields.
  const proto = protoType.create(object);
  const protoCastObject = protoType.toObject(proto);

  function checkFields(present: { [k: string]: any }, desired: { [k: string]: any }) {
    // Present is guaranteed to be a strict subset of desired.
    // Present fields cannot have empty values.
    Object.entries(present).forEach(([presentKey, presentValue]) => {
      const desiredValue = desired[presentKey];
      if (!desiredValue) {
        throw ReferenceError(`unexpected key '${presentKey}'`);
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

export class ProtoStringifier<T> implements IStringifier<T> {
  public static create<T>(protoType: IProtoClass<T, T>) {
    return new ProtoStringifier<T>(protoType);
  }

  constructor(private readonly protoType: IProtoClass<T, T>) {}

  public stringify(value: T) {
    return encode64(this.protoType, value);
  }
  public parse(value: string) {
    return decode64(this.protoType, value);
  }
}
