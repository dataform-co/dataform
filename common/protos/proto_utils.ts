import { util } from "protobufjs";

export class ProtoUtils {
  public static decode64(value: string): Uint8Array {
    const buf = new Uint8Array(util.base64.length(value));
    util.base64.decode(value, buf, 0);
    return buf;
  }

  public static encode64(value: Uint8Array): string {
    return util.base64.encode(value, 0, value.length);
  }

  public static decode<Proto>(protoType: new () => Proto, encodedValue: string): Proto {
    if (!encodedValue || encodedValue === "" || encodedValue === "_") {
      return (protoType as any).create();
    }
    return (protoType as any).decode(ProtoUtils.decode64(encodedValue)) as Proto;
  }

  public static encode<Proto>(protoType: new () => Proto, value: Proto): string {
    if (!value) {
      value = {} as Proto;
    }
    return ProtoUtils.encode64(
      (protoType as any).encode((protoType as any).create(value)).finish()
    );
  }
}
