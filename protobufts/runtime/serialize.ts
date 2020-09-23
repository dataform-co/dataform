import Long from "long";

interface IMessage {
  serialize: () => Uint8Array;
}

enum WireType {
  VARINT = 0,
  SIXTY_FOUR_BIT = 1,
  LENGTH_DELIMITED = 2,
  THIRTY_TWO_BIT = 5
}

export class Serializer {
  private readonly writer: BytesWriter = new BytesWriter();

  public double(fieldNumber: number, packed: boolean, val: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.SIXTY_FOUR_BIT,
      packed,
      (writer, singleVal) => writer.writeSixtyFourBitFloat(singleVal),
      val
    );
  }

  public float(fieldNumber: number, packed: boolean, val: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.THIRTY_TWO_BIT,
      packed,
      (writer, singleVal) => writer.writeThirtyTwoBitFloat(singleVal),
      val
    );
  }

  public int32(fieldNumber: number, packed: boolean, val: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(Long.fromNumber(singleVal)),
      val
    );
  }

  public fixed32(fieldNumber: number, packed: boolean, val: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.THIRTY_TWO_BIT,
      packed,
      (writer, singleVal) => writer.writeThirtyTwoBitInteger(singleVal),
      val
    );
  }

  public uint32(fieldNumber: number, packed: boolean, val: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(Long.fromNumber(singleVal)),
      val
    );
  }

  public sfixed32(fieldNumber: number, packed: boolean, val: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.THIRTY_TWO_BIT,
      packed,
      (writer, singleVal) => writer.writeThirtyTwoBitInteger(singleVal),
      val
    );
  }

  public sint32(fieldNumber: number, packed: boolean, val: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) =>
        writer.writeVarInt(Long.fromNumber(((singleVal << 1) ^ (singleVal >> 31)) >>> 0)),
      val
    );
  }

  public enum(fieldNumber: number, packed: boolean, val: number | number[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(Long.fromNumber(singleVal)),
      val
    );
  }

  public int64(fieldNumber: number, packed: boolean, val: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(singleVal),
      val
    );
  }

  public uint64(fieldNumber: number, packed: boolean, val: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(singleVal),
      val
    );
  }

  public fixed64(fieldNumber: number, packed: boolean, val: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.SIXTY_FOUR_BIT,
      packed,
      (writer, singleVal) => writer.writeSixtyFourBitInteger(singleVal),
      val
    );
  }

  public sfixed64(fieldNumber: number, packed: boolean, val: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.SIXTY_FOUR_BIT,
      packed,
      (writer, singleVal) => writer.writeSixtyFourBitInteger(singleVal),
      val
    );
  }

  public sint64(fieldNumber: number, packed: boolean, val: Long | Long[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) =>
        writer.writeVarInt(singleVal.shiftLeft(1).xor(singleVal.shiftRight(63))),
      val
    );
  }

  public bool(fieldNumber: number, packed: boolean, val: boolean | boolean[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.VARINT,
      packed,
      (writer, singleVal) => writer.writeVarInt(singleVal ? Long.ONE : Long.ZERO),
      val
    );
  }

  public bytes(fieldNumber: number, packed: boolean, val: Uint8Array | Uint8Array[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.LENGTH_DELIMITED,
      false,
      (writer, singleVal) =>
        writer.writeVarInt(Long.fromNumber(singleVal.length)).writeBytes(singleVal),
      val
    );
  }

  public string(fieldNumber: number, packed: boolean, val: string | string[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.LENGTH_DELIMITED,
      false,
      (writer, singleVal) => {
        const buffer = Buffer.from(singleVal);
        return writer.writeVarInt(Long.fromNumber(buffer.byteLength)).writeBytes(buffer);
      },
      val
    );
  }

  public message(fieldNumber: number, packed: boolean, val: IMessage | IMessage[]): this {
    return this.serializeField(
      fieldNumber,
      WireType.LENGTH_DELIMITED,
      false,
      (writer, singleVal) => {
        const bytes = singleVal.serialize();
        return writer.writeVarInt(Long.fromNumber(bytes.byteLength)).writeBytes(bytes);
      },
      val
    );
  }

  public map<K, V>(
    fieldNumber: number,
    packed: boolean,
    val: Map<K, V>,
    serializeKey: (serializer: Serializer, key: K) => void,
    serializeValue: (serializer: Serializer, value: V) => void
  ) {
    return this.serializeField(
      fieldNumber,
      WireType.LENGTH_DELIMITED,
      false,
      (writer, [key, value]) => {
        const entrySerializer = new Serializer();
        serializeKey(entrySerializer, key);
        serializeValue(entrySerializer, value);
        const bytes = entrySerializer.finish();
        return writer.writeVarInt(Long.fromNumber(bytes.byteLength)).writeBytes(bytes);
      },
      Array.from(val)
    );
  }

  public finish(): Uint8Array {
    return Uint8Array.from(this.writer.buffer);
  }

  private serializeField<T>(
    fieldNumber: number,
    wireType: WireType,
    packed: boolean,
    writeSingleVal: (writer: BytesWriter, singleVal: T) => void,
    val: T | T[]
  ) {
    if (!Array.isArray(val)) {
      writeSingleVal(this.newTag(fieldNumber, wireType), val);
      return this;
    }
    if (packed) {
      const writer = new BytesWriter();
      val.forEach(singleVal => writeSingleVal(writer, singleVal));
      const packedBytes = Uint8Array.from(writer.buffer);
      this.newTag(fieldNumber, WireType.LENGTH_DELIMITED)
        .writeVarInt(Long.fromNumber(packedBytes.byteLength))
        .writeBytes(packedBytes);
    } else {
      val.forEach(singleVal => writeSingleVal(this.newTag(fieldNumber, wireType), singleVal));
    }
    return this;
  }

  private newTag(fieldNumber: number, wireType: WireType): BytesWriter {
    // See https://developers.google.com/protocol-buffers/docs/encoding#structure.
    return this.writer.writeVarInt(Long.fromNumber((fieldNumber << 3) | wireType));
  }
}

export class Deserializer {
  public static single<T>(values: T[]) {
    if (values.length !== 1) {
      throw new Error(`Expected exactly one value in array, but got ${values}`);
    }
    return values[0];
  }

  private readonly reader: BytesReader;

  constructor(bytes: Uint8Array) {
    this.reader = new BytesReader(bytes);
  }

  public *deserialize() {
    for (const { fieldNumber, wireType } of this.reader.read()) {
      const reader = (() => {
        switch (wireType) {
          case WireType.VARINT:
            return new BytesReader(this.reader.readBytes(this.reader.nextVarIntSize()));
          case WireType.SIXTY_FOUR_BIT:
            return new BytesReader(this.reader.readBytes(8));
          case WireType.LENGTH_DELIMITED:
            return new BytesReader(this.reader.readBytes(this.reader.readVarInt().toNumber()));
          case WireType.THIRTY_TWO_BIT:
            return new BytesReader(this.reader.readBytes(4));
          default:
            throw new Error(`Unrecognized wire type: ${wireType}`);
        }
      })();
      yield { fieldNumber, reader };
    }
  }
}

interface IRepeatedFieldDecoder<T> {
  decode(reader: BytesReader, currentValue: T[]): T[];
  single(): IDecoder<T>;
}

interface IDecoder<T> {
  decode(reader: BytesReader, currentValue?: T): T;
}

abstract class NotPackedFieldDecoder<T> implements IRepeatedFieldDecoder<T> {
  public decode(reader: BytesReader, currentValue: T[]): T[] {
    currentValue.push(this.decodeSingleValue(reader));
    return currentValue;
  }

  public single(): IDecoder<T> {
    const repeatedDecoder = this;
    return new (class {
      public decode(reader: BytesReader): T {
        return repeatedDecoder.decodeSingleValue(reader);
      }
    })();
  }

  protected abstract decodeSingleValue(reader: BytesReader): T;
}

abstract class MaybePackedFieldDecoder<T> implements IRepeatedFieldDecoder<T> {
  public decode(reader: BytesReader, currentValue: T[]): T[] {
    while (!reader.done()) {
      currentValue.push(this.decodeSingleValue(reader));
    }
    return currentValue;
  }

  public single(): IDecoder<T> {
    const repeatedDecoder = this;
    return new (class {
      public decode(reader: BytesReader): T {
        const values = repeatedDecoder.decode(reader, []);
        if (values.length !== 1) {
          throw new Error(`Expected exactly one value but saw ${values.length} values.`);
        }
        return values[0];
      }
    })();
  }

  protected abstract decodeSingleValue(reader: BytesReader): T;
}

export class Decoders {
  public static double(): IRepeatedFieldDecoder<number> {
    return new (class extends MaybePackedFieldDecoder<number> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readSixtyFourBitFloat();
      }
    })();
  }

  public static float(): IRepeatedFieldDecoder<number> {
    return new (class extends MaybePackedFieldDecoder<number> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readThirtyTwoBitFloat();
      }
    })();
  }

  public static int32(): IRepeatedFieldDecoder<number> {
    return new (class extends MaybePackedFieldDecoder<number> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readVarInt().toNumber();
      }
    })();
  }

  public static fixed32(): IRepeatedFieldDecoder<number> {
    return new (class extends MaybePackedFieldDecoder<number> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readThirtyTwoBitInteger(true);
      }
    })();
  }

  public static uint32(): IRepeatedFieldDecoder<number> {
    return new (class extends MaybePackedFieldDecoder<number> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readVarInt().toNumber();
      }
    })();
  }

  public static sfixed32(): IRepeatedFieldDecoder<number> {
    return new (class extends MaybePackedFieldDecoder<number> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readThirtyTwoBitInteger();
      }
    })();
  }

  public static sint32(): IRepeatedFieldDecoder<number> {
    return new (class extends MaybePackedFieldDecoder<number> {
      public decodeSingleValue(reader: BytesReader) {
        const val = reader.readVarInt().toNumber();
        return (val >>> 1) ^ -(val & 1);
      }
    })();
  }

  public static enum(): IRepeatedFieldDecoder<number> {
    return new (class extends MaybePackedFieldDecoder<number> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readVarInt().toNumber();
      }
    })();
  }

  public static int64(): IRepeatedFieldDecoder<Long> {
    return new (class extends MaybePackedFieldDecoder<Long> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readVarInt();
      }
    })();
  }

  public static uint64(): IRepeatedFieldDecoder<Long> {
    return new (class extends MaybePackedFieldDecoder<Long> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readVarInt().toUnsigned();
      }
    })();
  }

  public static fixed64(): IRepeatedFieldDecoder<Long> {
    return new (class extends MaybePackedFieldDecoder<Long> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readSixtyFourBitInteger(true);
      }
    })();
  }

  public static sfixed64(): IRepeatedFieldDecoder<Long> {
    return new (class extends MaybePackedFieldDecoder<Long> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readSixtyFourBitInteger();
      }
    })();
  }

  public static sint64(): IRepeatedFieldDecoder<Long> {
    return new (class extends MaybePackedFieldDecoder<Long> {
      public decodeSingleValue(reader: BytesReader) {
        const val = reader.readVarInt();
        return val.shiftRightUnsigned(1).xor(val.and(1).multiply(-1));
      }
    })();
  }

  public static bool(): IRepeatedFieldDecoder<boolean> {
    return new (class extends MaybePackedFieldDecoder<boolean> {
      public decodeSingleValue(reader: BytesReader) {
        return reader.readVarInt().greaterThan(0);
      }
    })();
  }

  public static bytes(): IRepeatedFieldDecoder<Uint8Array> {
    return new (class extends NotPackedFieldDecoder<Uint8Array> {
      protected decodeSingleValue(reader: BytesReader): Uint8Array {
        return reader.readBytes();
      }
    })();
  }

  public static string(): IRepeatedFieldDecoder<string> {
    return new (class extends NotPackedFieldDecoder<string> {
      protected decodeSingleValue(reader: BytesReader): string {
        return Buffer.from(reader.readBytes()).toString("utf8");
      }
    })();
  }

  public static message<T>(
    deserialize: (bytes: Uint8Array) => IMessage & T
  ): IRepeatedFieldDecoder<T> {
    return new (class {
      public decode(reader: BytesReader, currentValue: T[]): T[] {
        currentValue.push(deserialize(reader.readBytes()));
        return currentValue;
      }

      public single(): IDecoder<T> {
        return new (class {
          public decode(reader: BytesReader, currentValue: T): T {
            const newMessage = deserialize(reader.readBytes());
            // TODO: return currentValue.mergeWith(newMessage);
            return newMessage;
          }
        })();
      }
    })();
  }

  public static oneOfEntry<Name extends string, T>(
    field: Name,
    decoder: IDecoder<T>
  ): IDecoder<{
    field: Name;
    value: T;
  }> {
    return new (class {
      public decode(
        reader: BytesReader,
        currentValue?: {
          field: Name;
          value: T;
        }
      ): {
        field: Name;
        value: T;
      } {
        return {
          field,
          value: decoder.decode(reader, currentValue?.value)
        };
      }
    })();
  }

  public static map<K, V>(keyDecoder: IDecoder<K>, valueDecoder: IDecoder<V>): IDecoder<Map<K, V>> {
    return new (class {
      public decode(reader: BytesReader, currentValue: Map<K, V>): Map<K, V> {
        let key: K;
        let value: V;
        for (const { fieldNumber, reader: fieldReader } of new Deserializer(
          reader.readBytes()
        ).deserialize()) {
          switch (fieldNumber) {
            case 1: {
              key = keyDecoder.decode(fieldReader, key);
              break;
            }
            case 2: {
              value = valueDecoder.decode(fieldReader, value);
              break;
            }
          }
        }
        if (key === undefined || value === undefined) {
          throw new Error("Did not deserialize complete Map entry.");
        }
        currentValue.set(key, value);
        return currentValue;
      }
    })();
  }
}

class BytesWriter {
  public readonly buffer: number[] = [];

  public writeVarInt(varint: Long): this {
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

  public writeThirtyTwoBitInteger(num: number): this {
    const bytes = new Uint8Array(4);
    const dataView = new DataView(bytes.buffer);
    dataView.setInt32(0, num, true);
    return this.writeBytes(bytes);
  }

  public writeThirtyTwoBitFloat(num: number): this {
    const bytes = new Uint8Array(4);
    const dataView = new DataView(bytes.buffer);
    dataView.setFloat32(0, num, true);
    return this.writeBytes(bytes);
  }

  public writeSixtyFourBitInteger(num: Long) {
    const bytes = new Uint8Array(8);
    const dataView = new DataView(bytes.buffer);
    dataView.setInt32(0, num.getLowBits(), true);
    dataView.setInt32(4, num.getHighBits(), true);
    return this.writeBytes(bytes);
  }

  public writeSixtyFourBitFloat(num: number): this {
    const bytes = new Uint8Array(8);
    const dataView = new DataView(bytes.buffer);
    dataView.setFloat64(0, num, true);
    return this.writeBytes(bytes);
  }

  public writeBytes(bytes: Uint8Array): this {
    bytes.forEach(byte => this.buffer.push(byte));
    return this;
  }
}

class BytesReader {
  private cursor = 0;

  constructor(private readonly bytes: Uint8Array) {}

  public *read() {
    while (this.cursor < this.bytes.length) {
      const tag = this.readVarInt().toNumber();
      const fieldNumber = tag >> 3;
      const wireType: WireType = tag & 0b00000111;
      yield { fieldNumber, wireType };
    }
  }

  public nextVarIntSize() {
    for (let i = this.cursor; i < this.bytes.length; i++) {
      if ((this.bytes[i] & 0b10000000) === 0) {
        return i - this.cursor + 1;
      }
    }
    throw new Error("Varint decoding did not end before buffer end.");
  }

  public readVarInt() {
    const collected = [];
    const nextVarintSize = this.nextVarIntSize();
    for (let i = 0; i < nextVarintSize; i++) {
      collected.push(this.bytes[this.cursor]);
      this.cursor++;
    }
    let value = Long.ZERO;
    for (let i = collected.length - 1; i >= 0; i--) {
      value = value.shiftLeft(7).or(collected[i] & 0b01111111);
    }
    return value;
  }

  public readThirtyTwoBitInteger(unsigned: boolean = false) {
    const bytes = this.readBytes(4);
    const dataView = new DataView(bytes.buffer);
    return unsigned ? dataView.getUint32(0, true) : dataView.getInt32(0, true);
  }

  public readThirtyTwoBitFloat() {
    const bytes = this.readBytes(4);
    const dataView = new DataView(bytes.buffer);
    return dataView.getFloat32(0, true);
  }

  public readSixtyFourBitInteger(unsigned: boolean = false) {
    return Long.fromBytesLE(this.readBytesAsNumberArray(8), unsigned);
  }

  public readSixtyFourBitFloat() {
    const bytes = this.readBytes(8);
    const dataView = new DataView(bytes.buffer);
    return dataView.getFloat64(0, true);
  }

  public readBytes(num?: number) {
    return Uint8Array.from(this.readBytesAsNumberArray(num === undefined ? this.length : num));
  }

  public get length() {
    return this.bytes.length;
  }

  public done() {
    return this.cursor >= this.bytes.length;
  }

  private readBytesAsNumberArray(num: number) {
    const bytes = [];
    for (let i = 0; i < num; i++) {
      if (this.done()) {
        throw new Error("No more data to read.");
      }
      bytes.push(this.bytes[this.cursor]);
      this.cursor++;
    }
    return bytes;
  }
}
