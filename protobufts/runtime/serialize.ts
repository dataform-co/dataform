import Long from "long";
import { Writer } from "protobufjs";

export interface IMessage {
  serialize: () => Uint8Array;
  serializeInternal: (serializer: Serializer) => Serializer;
}

export class Serializer {
  constructor(private readonly writer: Writer = new Writer()) {}

  public double(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.double(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).double(val);
    }
    return this;
  }

  public float(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.float(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).float(val);
    }
    return this;
  }

  public int32(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.int32(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.VARINT).int32(val);
    }
    return this;
  }

  public fixed32(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.fixed32(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).fixed32(val);
    }
    return this;
  }

  public uint32(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.uint32(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.VARINT).uint32(val);
    }
    return this;
  }

  public sfixed32(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.sfixed32(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).sfixed32(val);
    }
    return this;
  }

  public sint32(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.sint32(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.VARINT).sint32(val);
    }
    return this;
  }

  public enum(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.int32(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.VARINT).int32(val);
    }
    return this;
  }

  public int64(fieldNumber: number, val?: Long | Long[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.int64(value));
    } else if (val && !val.isZero()) {
      this.newTag(fieldNumber, WireType.VARINT).int64(val);
    }
    return this;
  }

  public uint64(fieldNumber: number, val?: Long | Long[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.uint64(value));
    } else if (val && !val.isZero()) {
      this.newTag(fieldNumber, WireType.VARINT).uint64(val);
    }
    return this;
  }

  public fixed64(fieldNumber: number, val?: Long | Long[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.fixed64(value));
    } else if (val && !val.isZero()) {
      this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).fixed64(val);
    }
    return this;
  }

  public sfixed64(fieldNumber: number, val?: Long | Long[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.sfixed64(value));
    } else if (val && !val.isZero()) {
      this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).sfixed64(val);
    }
    return this;
  }

  public sint64(fieldNumber: number, val?: Long | Long[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.sint64(value));
    } else if (val && !val.isZero()) {
      this.newTag(fieldNumber, WireType.VARINT).sint64(val);
    }
    return this;
  }

  public bool(fieldNumber: number, val?: boolean | boolean[]): this {
    if (Array.isArray(val)) {
      this.writeRepeated(fieldNumber, val, (writer, value) => writer.bool(value));
    } else if (val) {
      this.newTag(fieldNumber, WireType.VARINT).bool(val);
    }
    return this;
  }

  public bytes(fieldNumber: number, val?: Uint8Array | Uint8Array[]): this {
    const values = Array.isArray(val) ? val : val.length > 0 ? [val] : [];
    values.forEach(value => this.newTag(fieldNumber, WireType.LENGTH_DELIMITED).bytes(value));
    return this;
  }

  public string(fieldNumber: number, val?: string | string[]): this {
    const values = Array.isArray(val) ? val : val ? [val] : [];
    values.forEach(value => this.newTag(fieldNumber, WireType.LENGTH_DELIMITED).string(value));
    return this;
  }

  public message(fieldNumber: number, val?: IMessage | IMessage[]): this {
    const values = Array.isArray(val) ? val : val ? [val] : [];
    values.forEach(value => {
      const writer = this.newTag(fieldNumber, WireType.LENGTH_DELIMITED).fork();
      value.serializeInternal(new Serializer(writer));
      writer.ldelim();
    });
    return this;
  }

  public finish(): Uint8Array {
    return this.writer.finish();
  }

  private writeRepeated<T>(
    fieldNumber: number,
    values: T[],
    writeFn: (writer: Writer, value: T) => void
  ) {
    const writer = this.newTag(fieldNumber, WireType.LENGTH_DELIMITED).fork();
    values.forEach(value => writeFn(writer, value));
    writer.ldelim();
  }

  private newTag(fieldNumber: number, wireType: WireType) {
    // See https://developers.google.com/protocol-buffers/docs/encoding#structure.
    // tslint:disable-next-line: no-bitwise
    return this.writer.uint32((fieldNumber << 3) | wireType);
  }
}

export class NewSerializer {
  private readonly output: number[] = [];

  public double(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val) {
      this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).writeNonVarInt(val, true, true);
    }
    return this;
  }

  public float(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val) {
      this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).writeNonVarInt(val, false, true);
    }
    return this;
  }

  public int32(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val) {
      this.newTag(fieldNumber, WireType.VARINT).writeVarInt(val);
    }
    return this;
  }

  public fixed32(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val) {
      this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).writeNonVarInt(val, false, false);
    }
    return this;
  }

  public uint32(fieldNumber: number, val?: number | number[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val) {
      this.newTag(fieldNumber, WireType.VARINT).writeVarInt(val);
    }
    return this;
  }

  public sfixed32(fieldNumber: number, val?: number | number[]): this {
    return this;
  }

  public sint32(fieldNumber: number, val?: number | number[]): this {
    return this;
  }

  public enum(fieldNumber: number, val?: number | number[]): this {
    return this;
  }

  public int64(fieldNumber: number, val?: Long | Long[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (!val.isZero()) {
      this.newTag(fieldNumber, WireType.VARINT).writeLongVarInt(val);
    }
    return this;
  }

  public uint64(fieldNumber: number, val?: Long | Long[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (!val.isZero()) {
      this.newTag(fieldNumber, WireType.VARINT).writeLongVarInt(val);
    }
    return this;
  }

  public fixed64(fieldNumber: number, val?: Long | Long[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (!val.isZero()) {
      this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).writeLongNonVarInt(val);
    }
    return this;
  }

  public sfixed64(fieldNumber: number, val?: Long | Long[]): this {
    return this;
  }

  public sint64(fieldNumber: number, val?: Long | Long[]): this {
    return this;
  }

  public bool(fieldNumber: number, val?: boolean | boolean[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val) {
      this.newTag(fieldNumber, WireType.VARINT).writeVarInt(1);
    }
    return this;
  }

  public bytes(fieldNumber: number, val?: Uint8Array | Uint8Array[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val && val.length > 0) {
      this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
        .writeVarInt(val.length)
        .writeBytes(val);
    }
    return this;
  }

  public string(fieldNumber: number, val?: string | string[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val) {
      const buffer = Buffer.from(val);
      this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
        .writeVarInt(buffer.byteLength)
        .writeBytes(buffer);
    }
    return this;
  }

  public message(fieldNumber: number, val?: IMessage | IMessage[]): this {
    if (Array.isArray(val)) {
      // TODO: default checks should really be moved into the protobuf Message code to allow for proto2.
    } else if (val) {
      const bytes = val.serialize();
      this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
        .writeVarInt(bytes.length)
        .writeBytes(bytes);
    }
    return this;
  }

  public finish(): Uint8Array {
    return Uint8Array.from(this.output);
  }

  private newTag(fieldNumber: number, wireType: WireType): this {
    // See https://developers.google.com/protocol-buffers/docs/encoding#structure.
    this.writeVarInt((fieldNumber << 3) | wireType);
    return this;
  }

  private writeVarInt(varint: number): this {
    if (varint === 0) {
      this.output.push(0);
    } else if (varint > 0) {
      while (varint) {
        let nextByte = varint & 0b01111111;
        varint >>>= 7;
        if (varint) {
          nextByte |= 0b10000000;
        }
        this.output.push(nextByte);
      }
    } else {
      for (let i = 0; i < 10; i++) {
        const nextByte = i === 9 ? 1 : (varint & 0b01111111) | 0b10000000;
        varint >>= 7;
        this.output.push(nextByte);
      }
    }
    return this;
  }

  private writeLongVarInt(varint: Long): this {
    if (varint.isZero()) {
      this.output.push(0);
    } else if (varint.greaterThan(0)) {
      while (!varint.isZero()) {
        let nextByte = varint.getLowBits() & 0b01111111;
        varint = varint.shiftRightUnsigned(7);
        if (!varint.isZero()) {
          nextByte |= 0b10000000;
        }
        this.output.push(nextByte);
      }
    } else {
      for (let i = 0; i < 10; i++) {
        const nextByte = i === 9 ? 1 : (varint.getLowBits() & 0b01111111) | 0b10000000;
        varint = varint.shiftRight(7);
        this.output.push(nextByte);
      }
    }
    return this;
  }

  private writeNonVarInt(num: number, sixtyFourBit: boolean, float: boolean): this {
    const bytes = new Uint8Array(sixtyFourBit ? 8 : 4);
    const dataView = new DataView(bytes.buffer);
    if (float) {
      if (sixtyFourBit) {
        dataView.setFloat64(0, num, true);
      } else {
        dataView.setFloat32(0, num, true);
      }
    } else {
      dataView.setInt32(0, num, true);
    }
    return this.writeBytes(bytes);
  }

  private writeLongNonVarInt(num: Long): this {
    const bytes = new Uint8Array(8);
    const dataView = new DataView(bytes.buffer);
    dataView.setInt32(0, num.getLowBits(), true);
    dataView.setInt32(4, num.getHighBits(), true);
    return this.writeBytes(bytes);
  }

  private writeBytes(bytes: Uint8Array): this {
    bytes.forEach(byte => this.output.push(byte));
    return this;
  }
}

enum WireType {
  VARINT = 0,
  SIXTY_FOUR_BIT = 1,
  LENGTH_DELIMITED = 2,
  THIRTY_TWO_BIT = 5
}
