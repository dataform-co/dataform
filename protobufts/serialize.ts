import { Writer } from "protobufjs";

export interface Message {
  serializeInternal: (serializer: Serializer) => Serializer;
}

export class Serializer {
  constructor(private readonly writer: Writer = new Writer()) {}

  public string(fieldNumber: number, val?: string): this {
    if (val) {
      this.newTag(fieldNumber, WireType.LENGTH_DELIMITED).string(val);
    }
    return this;
  }

  public message(fieldNumber: number, val?: Message): this {
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
