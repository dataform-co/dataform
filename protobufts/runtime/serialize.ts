import Long from "long";

export interface IMessage {
  serialize: () => Uint8Array;
}

export class Serializer {
  private readonly writer: BufferedWriter = new BufferedWriter();

  public double(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.SIXTY_FOUR_BIT,
      packed,
      (writer, singleVal) => writer.writeNonVarInt(singleVal, true, true),
      val
    );
  }

  public float(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.THIRTY_TWO_BIT,
      packed,
      (writer, singleVal) => writer.writeNonVarInt(singleVal, false, true),
      val
    );
  }

  public int32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(singleVal),
      val
    );
  }

  public fixed32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.THIRTY_TWO_BIT,
      packed,
      (writer, singleVal) => writer.writeNonVarInt(singleVal, false, false),
      val
    );
  }

  public uint32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(singleVal),
      val
    );
  }

  public sfixed32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.THIRTY_TWO_BIT,
      packed,
      (writer, singleVal) => writer.writeNonVarInt(singleVal, false, false),
      val
    );
  }

  public sint32(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(((singleVal << 1) ^ (singleVal >> 31)) >>> 0),
      val
    );
  }

  public enum(fieldNumber: number, packed: boolean, val?: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(singleVal),
      val
    );
  }

  public int64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeLongVarInt(singleVal),
      val
    );
  }

  public uint64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeLongVarInt(singleVal),
      val
    );
  }

  public fixed64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.SIXTY_FOUR_BIT,
      packed,
      (writer, singleVal) => writer.writeLongNonVarInt(singleVal),
      val
    );
  }

  public sfixed64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.SIXTY_FOUR_BIT,
      packed,
      (writer, singleVal) => writer.writeLongNonVarInt(singleVal),
      val
    );
  }

  public sint64(fieldNumber: number, packed: boolean, val?: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) =>
        writer.writeLongVarInt(singleVal.shiftLeft(1).xor(singleVal.shiftRight(63))),
      val
    );
  }

  public bool(fieldNumber: number, packed: boolean, val?: boolean | boolean[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      writer => writer.writeVarInt(1),
      val
    );
  }

  public bytes(fieldNumber: number, packed: boolean, val?: Uint8Array | Uint8Array[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.LENGTH_DELIMITED,
      packed,
      (writer, singleVal) => writer.writeVarInt(singleVal.length).writeBytes(singleVal),
      val
    );
  }

  public string(fieldNumber: number, packed: boolean, val?: string | string[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.LENGTH_DELIMITED,
      packed,
      (writer, singleVal) => {
        const buffer = Buffer.from(singleVal);
        return writer.writeVarInt(buffer.byteLength).writeBytes(buffer);
      },
      val
    );
  }

  public message(fieldNumber: number, packed: boolean, val?: IMessage | IMessage[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.LENGTH_DELIMITED,
      packed,
      (writer, singleVal) => {
        const bytes = singleVal.serialize();
        return writer.writeVarInt(bytes.byteLength).writeBytes(bytes);
      },
      val
    );
  }

  public finish(): Uint8Array {
    return Uint8Array.from(this.writer.buffer);
  }

  private serializeField<T>(
    fieldNumber: number,
    wireType: WireType,
    packed: boolean,
    writeSingleVal: (writer: BufferedWriter, singleVal: T) => void,
    val: T | T[]
  ) {
    if (!Array.isArray(val)) {
      writeSingleVal(this.newTag(fieldNumber, wireType), val);
      return this;
    }
    if (packed) {
      const writer = new BufferedWriter();
      val.forEach(singleVal => writeSingleVal(writer, singleVal));
      const packedBytes = Uint8Array.from(writer.buffer);
      this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
        .writeVarInt(packedBytes.byteLength)
        .writeBytes(packedBytes);
    } else {
      val.forEach(singleVal => writeSingleVal(this.newTag(fieldNumber, wireType), singleVal));
    }
    return this;
  }

  private newTag(fieldNumber: number, wireType: WireType): BufferedWriter {
    // See https://developers.google.com/protocol-buffers/docs/encoding#structure.
    return this.writer.writeVarInt((fieldNumber << 3) | wireType);
  }
}

class BufferedWriter {
  public readonly buffer: number[] = [];

  public writeVarInt(varint: number): this {
    if (varint === 0) {
      this.buffer.push(0);
    } else if (varint > 0) {
      while (varint) {
        let nextByte = varint & 0b01111111;
        varint >>>= 7;
        if (varint) {
          nextByte |= 0b10000000;
        }
        this.buffer.push(nextByte);
      }
    } else {
      for (let i = 0; i < 10; i++) {
        const nextByte = i === 9 ? 1 : (varint & 0b01111111) | 0b10000000;
        varint >>= 7;
        this.buffer.push(nextByte);
      }
    }
    return this;
  }

  public writeLongVarInt(varint: Long): this {
    if (varint.isZero()) {
      this.buffer.push(0);
    } else if (varint.greaterThan(0)) {
      while (!varint.isZero()) {
        let nextByte = varint.getLowBits() & 0b01111111;
        varint = varint.shiftRightUnsigned(7);
        if (!varint.isZero()) {
          nextByte |= 0b10000000;
        }
        this.buffer.push(nextByte);
      }
    } else {
      for (let i = 0; i < 10; i++) {
        const nextByte = i === 9 ? 1 : (varint.getLowBits() & 0b01111111) | 0b10000000;
        varint = varint.shiftRight(7);
        this.buffer.push(nextByte);
      }
    }
    return this;
  }

  public writeNonVarInt(num: number, sixtyFourBit: boolean, float: boolean): this {
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

  public writeLongNonVarInt(num: Long): this {
    const bytes = new Uint8Array(8);
    const dataView = new DataView(bytes.buffer);
    dataView.setInt32(0, num.getLowBits(), true);
    dataView.setInt32(4, num.getHighBits(), true);
    return this.writeBytes(bytes);
  }

  public writeBytes(bytes: Uint8Array): this {
    bytes.forEach(byte => this.buffer.push(byte));
    return this;
  }
}

export class Deserializer {
  private readonly bufferedReader: BytesReader;

  constructor(bytes: Uint8Array) {
    this.bufferedReader = new BytesReader(bytes);
  }

  public *deserialize() {
    for (const { fieldNumber, wireType } of this.bufferedReader.read()) {
      const length =
        wireType === WireType.LENGTH_DELIMITED ? this.bufferedReader.readVarInt().toNumber() : 1;
      yield { fieldNumber, length };
    }
  }

  public double() {
    return this.bufferedReader.readSixtyFourBitFloat();
  }

  public float() {
    return this.bufferedReader.readThirtyTwoBitFloat();
  }

  public int32() {
    return this.bufferedReader.readVarInt().toNumber();
  }

  public fixed32() {
    return this.bufferedReader.readThirtyTwoBitInteger();
  }

  public uint32() {
    return this.bufferedReader.readVarInt().toNumber();
  }

  public sfixed32() {
    return this.bufferedReader.readThirtyTwoBitInteger();
  }

  public sint32() {
    const val = this.bufferedReader.readVarInt().toNumber();
    return (val >>> 1) ^ -(val & 1);
  }

  public enum() {
    return this.bufferedReader.readVarInt().toNumber();
  }

  public int64() {
    return this.bufferedReader.readVarInt();
  }

  public uint64() {
    return this.bufferedReader.readVarInt();
  }

  public fixed64() {
    return this.bufferedReader.readSixtyFourBitInteger();
  }

  public sfixed64() {
    return this.bufferedReader.readSixtyFourBitInteger();
  }

  public sint64() {
    const val = this.bufferedReader.readVarInt();
    return val.shiftRightUnsigned(1).xor(val.and(1).multiply(-1));
  }

  public bool() {
    return this.bufferedReader.readVarInt().greaterThan(0);
  }

  public bytes(length: number) {
    return this.bufferedReader.readBytes(length);
  }

  public string(length: number) {
    return Buffer.from(this.bytes(length)).toString("utf8");
  }
}

class BytesReader {
  private cursor = 0;

  constructor(private readonly bytes: Uint8Array) {}

  public *read() {
    while (this.cursor < this.bytes.length) {
      const tag = this.readVarInt().toNumber();
      const fieldNumber = tag >> 3;
      // TODO: this might be LENGTH_DELIMITED for a repeated packed field (only possible for primitive numeric types).
      // This might happen even if the repeated field has not been declared as packed.
      // Need to handle this case.
      const wireType: WireType = tag & 0b00000111;
      yield { fieldNumber, wireType };
    }
  }

  public readVarInt() {
    const collected = [];
    let shouldReadNextByte = true;
    while (shouldReadNextByte) {
      const nextByte = this.bytes[this.cursor];
      this.cursor++;
      shouldReadNextByte = (nextByte & 0b10000000) > 0;
      collected.push(nextByte);
    }
    let value = Long.ZERO;
    for (let i = collected.length - 1; i >= 0; i--) {
      value = value.shiftLeft(7).or(collected[i] & 0b01111111);
    }
    return value;
  }

  public readThirtyTwoBitInteger() {
    const bytes = this.readBytes(4);
    const dataView = new DataView(bytes.buffer);
    return dataView.getInt32(0, true);
  }

  public readThirtyTwoBitFloat() {
    const bytes = this.readBytes(4);
    const dataView = new DataView(bytes.buffer);
    return dataView.getFloat32(0, true);
  }

  public readSixtyFourBitInteger() {
    return Long.fromBytesLE(this.readBytesAsNumberArray(8));
  }

  public readSixtyFourBitFloat() {
    const bytes = this.readBytes(8);
    const dataView = new DataView(bytes.buffer);
    return dataView.getFloat64(0, true);
  }

  public readBytes(num: number) {
    return Uint8Array.from(this.readBytesAsNumberArray(num));
  }

  private readBytesAsNumberArray(num: number) {
    const bytes = [];
    for (let i = 0; i < num; i++) {
      bytes.push(this.bytes[this.cursor]);
      this.cursor++;
    }
    return bytes;
  }
}

enum WireType {
  VARINT = 0,
  SIXTY_FOUR_BIT = 1,
  LENGTH_DELIMITED = 2,
  THIRTY_TWO_BIT = 5
}
