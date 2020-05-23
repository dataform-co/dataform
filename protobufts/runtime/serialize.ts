import Long from "long";
import { Writer } from "protobufjs";

export interface IMessage {
  serializeInternal: (serializer: Serializer) => Serializer;
}

export class Serializer {
  constructor(private readonly writer: Writer = new Writer()) {}

  public double(fieldNumber: number, val?: number): this {
    if (val) {
      this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).double(val);
    }
    return this;
  }

  public float(fieldNumber: number, val?: number): this {
    if (val) {
      this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).float(val);
    }
    return this;
  }

  public int32(fieldNumber: number, val?: number): this {
    if (val) {
      this.newTag(fieldNumber, WireType.VARINT).int32(val);
    }
    return this;
  }

  public fixed32(fieldNumber: number, val?: number): this {
    if (val) {
      this.newTag(fieldNumber, WireType.THIRTY_TWO_BIT).fixed32(val);
    }
    return this;
  }

  public uint32(fieldNumber: number, val?: number): this {
    if (val) {
    }
    return this;
  }

  public sfixed32(fieldNumber: number, val?: number): this {
    if (val) {
    }
    return this;
  }

  public sint32(fieldNumber: number, val?: number): this {
    if (val) {
    }
    return this;
  }

  public enum(fieldNumber: number, val?: number): this {
    if (val) {
    }
    return this;
  }

  public int64(fieldNumber: number, val?: Long): this {
    if (val && !val.isZero()) {
      this.newTag(fieldNumber, WireType.VARINT).int64(val);
    }
    return this;
  }

  public uint64(fieldNumber: number, val?: Long): this {
    if (val && !val.isZero()) {
      this.newTag(fieldNumber, WireType.VARINT).uint64(val);
    }
    return this;
  }

  public fixed64(fieldNumber: number, val?: Long): this {
    if (val && !val.isZero()) {
      this.newTag(fieldNumber, WireType.SIXTY_FOUR_BIT).fixed64(val);
    }
    return this;
  }

  public sfixed64(fieldNumber: number, val?: Long): this {
    if (val && !val.isZero()) {
    }
    return this;
  }

  public sint64(fieldNumber: number, val?: Long): this {
    if (val && !val.isZero()) {
    }
    return this;
  }

  public bool(fieldNumber: number, val?: boolean): this {
    if (val) {
    }
    return this;
  }

  public bytes(fieldNumber: number, val?: Uint8Array): this {
    if (val) {
    }
    return this;
  }

  public string(fieldNumber: number, val?: string): this {
    if (val) {
      this.newTag(fieldNumber, WireType.LENGTH_DELIMITED).string(val);
    }
    return this;
  }

  public message(fieldNumber: number, val?: IMessage): this {
    if (val) {
      const writer = this.newTag(fieldNumber, WireType.LENGTH_DELIMITED).fork();
      val.serializeInternal(new Serializer(writer));
      writer.ldelim();
    }
    return this;
  }

  public finish(): Uint8Array {
    return this.writer.finish();
  }

  private newTag(fieldNumber: number, wireType: WireType) {
    // See https://developers.google.com/protocol-buffers/docs/encoding#structure.
    // tslint:disable-next-line: no-bitwise
    return this.writer.uint32((fieldNumber << 3) | wireType);
  }
}

enum WireType {
  VARINT = 0,
  SIXTY_FOUR_BIT = 1,
  LENGTH_DELIMITED = 2,
  THIRTY_TWO_BIT = 5
}
