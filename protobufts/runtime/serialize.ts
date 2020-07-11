import Long from "long";

export interface IMessage {
  serialize: () => Uint8Array;
  serializeInternal: (serializer: Proto3Serializer) => Proto3Serializer;
}

// Proto3Serializer serializes protobuf fields as according to proto3 rules,
// i.e. it does not write out default values.
export class Proto3Serializer {
  private readonly output: number[] = [];

  public double(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      if (packed) {
        const packedSerializer = new Proto3Serializer();
        for (const singleVal of val) {
          packedSerializer.writeNonVarInt(singleVal, true, true);
        }
        const packedBytes = Uint8Array.from(packedSerializer.output);
        this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
          .writeVarInt(packedBytes.byteLength)
          .writeBytes(packedBytes);
      } else {
        for (const singleVal of val) {
          this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).writeNonVarInt(singleVal, true, true);
        }
      }
      return this;
    }
    return this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).writeNonVarInt(val, true, true);
  }

  public float(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).writeNonVarInt(val, false, true);
  }

  public int32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.VARINT).writeVarInt(val);
  }

  public fixed32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).writeNonVarInt(val, false, false);
  }

  public uint32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.VARINT).writeVarInt(val);
  }

  public sfixed32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).writeNonVarInt(val, false, false);
  }

  public sint32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.VARINT).writeVarInt(((val << 1) ^ (val >> 31)) >>> 0);
  }

  public enum(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.VARINT).writeVarInt(val);
  }

  public int64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.VARINT).writeLongVarInt(val);
  }

  public uint64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.VARINT).writeLongVarInt(val);
  }

  public fixed64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).writeLongNonVarInt(val);
  }

  public sfixed64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).writeLongNonVarInt(val);
  }

  public sint64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.VARINT).writeLongVarInt(
      val.shiftLeft(1).xor(val.shiftRight(63))
    );
  }

  public bool(fieldNumber: number, packed: boolean, val?: boolean | boolean[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.VARINT).writeVarInt(1);
  }

  public bytes(fieldNumber: number, packed: boolean, val?: Uint8Array | Uint8Array[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    return this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
      .writeVarInt(val.length)
      .writeBytes(val);
  }

  public string(fieldNumber: number, packed: boolean, val?: string | string[]): this {
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    const buffer = Buffer.from(val);
    return this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
      .writeVarInt(buffer.byteLength)
      .writeBytes(buffer);
  }

  public message(fieldNumber: number, packed: boolean, val?: IMessage | IMessage[]): this {
    // should always return false
    if (!shouldProto3Serialize(val)) {
      return this;
    }
    if (Array.isArray(val)) {
      return this;
    }
    const bytes = val.serialize();
    return this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
      .writeVarInt(bytes.length)
      .writeBytes(bytes);
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

function shouldProto3Serialize(val?: any | any[]) {
  if (!val) {
    return false;
  }
  if (Array.isArray(val) && val.length === 0) {
    return false;
  }
  return !(
    val === 0 ||
    val === "" ||
    val === false ||
    (val instanceof Long && val.isZero()) ||
    (val instanceof Uint8Array && val.length === 0)
  );
}

enum WireType {
  VARINT = 0,
  SIXTY_FOUR_BIT = 1,
  LENGTH_DELIMITED = 2,
  THIRTY_TWO_BIT = 5
}
