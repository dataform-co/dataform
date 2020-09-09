import { expect } from "chai";
import Long from "long";

import { execSync } from "child_process";
import * as protobuftsProtos from "df/protobufts/tests/test1";
import { suite, test } from "df/testing";

suite(__filename, () => {
  suite("single-field serialization old", () => {
    const testCases = [
      {
        type: "packed double [35.6, 12.8, -8.9]",
        proto: protobuftsProtos.TestRepeatedMessage.create({
          doubleField: [35.6, 12.8, -8.9]
        }),
        encoded: new Uint8Array([
          10,
          24,
          205,
          204,
          204,
          204,
          204,
          204,
          65,
          64,
          154,
          153,
          153,
          153,
          153,
          153,
          41,
          64,
          205,
          204,
          204,
          204,
          204,
          204,
          33,
          192
        ])
      },
      {
        type: "unpacked double [35.6, 12.8, -8.9]",
        proto: protobuftsProtos.TestRepeatedMessage.create({
          unpackedDoubleField: [35.6, 12.8, -8.9]
        }),
        encoded: new Uint8Array([
          146,
          1,
          24,
          205,
          204,
          204,
          204,
          204,
          204,
          65,
          64,
          154,
          153,
          153,
          153,
          153,
          153,
          41,
          64,
          205,
          204,
          204,
          204,
          204,
          204,
          33,
          192
        ])
      },
      {
        type: "packed float [2.7, -9876.549]",
        proto: protobuftsProtos.TestRepeatedMessage.create({
          floatField: [2.7, -9876.549]
        }),
        encoded: new Uint8Array([18, 8, 205, 204, 44, 64, 50, 82, 26, 198])
      },
      {
        type: "unpacked float [2.7, -9876.549]",
        proto: protobuftsProtos.TestRepeatedMessage.create({
          unpackedFloatField: [2.7, -9876.549]
        }),
        encoded: new Uint8Array([154, 1, 8, 205, 204, 44, 64, 50, 82, 26, 198])
      }
    ];

    for (const testCase of testCases) {
      test(testCase.type, () => {
        expect(testCase.proto.serialize()).eql(testCase.encoded);
      });
    }
  });

  suite("single-field non-repeated reserialization", () => {
    const testCases = [
      // double_field
      protobuftsProtos.TestMessage.create({
        doubleField: 4.940656458412465441765687928682213723651e-324
      }),
      protobuftsProtos.TestMessage.create({
        doubleField: 35.6
      }),
      protobuftsProtos.TestMessage.create({
        doubleField: 1.797693134862315708145274237317043567981e308
      }),
      // float_field
      protobuftsProtos.TestMessage.create({
        floatField: 1.40129846432481707092372958328991613128e-45
      }),
      protobuftsProtos.TestMessage.create({
        floatField: 3.4028234663852885981170418348451692544e38
      }),
      // int64_field
      protobuftsProtos.TestMessage.create({
        int64Field: Long.MIN_VALUE
      }),
      protobuftsProtos.TestMessage.create({
        int64Field: Long.MAX_VALUE
      }),
      // uint64_field
      protobuftsProtos.TestMessage.create({
        uint64Field: Long.UZERO
      }),
      protobuftsProtos.TestMessage.create({
        uint64Field: Long.MAX_UNSIGNED_VALUE
      }),
      // int32_field
      protobuftsProtos.TestMessage.create({
        int32Field: -2147483648
      }),
      protobuftsProtos.TestMessage.create({
        int32Field: 2147483647
      }),
      // fixed64_field
      protobuftsProtos.TestMessage.create({
        fixed64Field: Long.UZERO
      }),
      protobuftsProtos.TestMessage.create({
        fixed64Field: Long.MAX_UNSIGNED_VALUE
      }),
      // fixed32_field
      protobuftsProtos.TestMessage.create({
        fixed32Field: 0
      }),
      // bool_field
      protobuftsProtos.TestMessage.create({
        boolField: false
      }),
      protobuftsProtos.TestMessage.create({
        boolField: true
      }),
      // string_field
      protobuftsProtos.TestMessage.create({
        stringField: ""
      }),
      protobuftsProtos.TestMessage.create({
        stringField: "hello world"
      }),
      // message_field
      protobuftsProtos.TestMessage.create({
        messageField: protobuftsProtos.TestMessage.create({})
      }),
      protobuftsProtos.TestMessage.create({
        messageField: protobuftsProtos.TestMessage.create({
          stringField: "hello world"
        })
      }),
      // bytes_field
      protobuftsProtos.TestMessage.create({
        bytesField: new Uint8Array([])
      }),
      protobuftsProtos.TestMessage.create({
        bytesField: new Uint8Array([0x5, 0xff])
      }),
      // uint32_field
      protobuftsProtos.TestMessage.create({
        uint32Field: 0
      }),
      protobuftsProtos.TestMessage.create({
        uint32Field: 4294967295
      }),
      // enum_field
      protobuftsProtos.TestMessage.create({
        enumField: protobuftsProtos.TestEnum.VAL0
      }),
      protobuftsProtos.TestMessage.create({
        enumField: protobuftsProtos.TestEnum.VAL1
      }),
      // sfixed32_field
      protobuftsProtos.TestMessage.create({
        sfixed32Field: -2147483648
      }),
      protobuftsProtos.TestMessage.create({
        sfixed32Field: 2147483647
      }),
      // sfixed64_field
      protobuftsProtos.TestMessage.create({
        sfixed64Field: Long.MIN_VALUE
      }),
      protobuftsProtos.TestMessage.create({
        sfixed64Field: Long.MAX_VALUE
      }),
      // sint32_field
      protobuftsProtos.TestMessage.create({
        sint32Field: -2147483648
      }),
      protobuftsProtos.TestMessage.create({
        sint32Field: 2147483647
      }),
      protobuftsProtos.TestMessage.create({
        fixed32Field: 4294967295
      }),
      // sint64_field
      protobuftsProtos.TestMessage.create({
        sint64Field: Long.MIN_VALUE
      }),
      protobuftsProtos.TestMessage.create({
        sint64Field: Long.MAX_VALUE
      }),
      // oneof_int32_field
      protobuftsProtos.TestMessage.create({
        oneof: { field: "oneof_int32_field", value: 0 }
      }),
      protobuftsProtos.TestMessage.create({
        oneof: { field: "oneof_int32_field", value: 1234 }
      }),
      // oneof_string_field
      protobuftsProtos.TestMessage.create({
        oneof: { field: "oneof_string_field", value: "" }
      }),
      protobuftsProtos.TestMessage.create({
        oneof: { field: "oneof_string_field", value: "hello world" }
      })
    ];

    for (const input of testCases) {
      test("reserialized", () => {
        const output = protobuftsProtos.TestMessage.deserialize(
          reserialize("TestMessage", input.serialize())
        );
        expect(output).eql(input);
      });
    }
  });

  suite("single-field repeated reserialization", () => {
    const testCases = [
      // double_field
      protobuftsProtos.TestRepeatedMessage.create({
        // doubleField: [4.940656458412465441765687928682213723651e-324, 35.6]
      })
      // float_field
      // int32_field
      // uint32_field
      // sint32_field
      // fixed32_field
      // sfixed32_field
      // int64_field
      // uint64_field
      // sint64_field
      // fixed64_field
      // sfixed64_field
      // bool_field
      // enum_field
      // string_field
      // message_field
      // bytes_field
    ];

    for (const input of testCases) {
      test("reserialized", () => {
        const output = protobuftsProtos.TestRepeatedMessage.deserialize(
          reserialize("TestRepeatedMessage", input.serialize())
        );
        expect(output).eql(input);
      });
    }
  });
});

const RESERIALIZER_BINARY = "protobufts/tests/reserializer/darwin_amd64_stripped/reserializer";

function reserialize(
  messageType: "TestMessage" | "TestRepeatedMessage",
  bytes: Uint8Array
): Uint8Array {
  const base64EncodedBytes = Buffer.from(bytes).toString("base64");
  const returnedBase64EncodedBytes = execSync(
    `${RESERIALIZER_BINARY} --proto_type=${messageType} --base64_proto_value=${base64EncodedBytes}`
  ).toString();
  return Buffer.from(returnedBase64EncodedBytes, "base64");
}
