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

  suite("single-field serialization old", () => {
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
      // int32_field
      protobuftsProtos.TestMessage.create({
        int32Field: -2147483648
      }),
      protobuftsProtos.TestMessage.create({
        int32Field: 2147483647
      }),
      // uint32_field
      protobuftsProtos.TestMessage.create({
        uint32Field: 0
      }),
      protobuftsProtos.TestMessage.create({
        uint32Field: 4294967295
      }),
      // sint32_field
      protobuftsProtos.TestMessage.create({
        sint32Field: -2147483648
      }),
      protobuftsProtos.TestMessage.create({
        sint32Field: 2147483647
      }),
      // fixed32_field
      protobuftsProtos.TestMessage.create({
        fixed32Field: 0
      }),
      protobuftsProtos.TestMessage.create({
        fixed32Field: 4294967295
      }),
      // sfixed32_field
      protobuftsProtos.TestMessage.create({
        sfixed32Field: -2147483648
      }),
      protobuftsProtos.TestMessage.create({
        sfixed32Field: 2147483647
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
      // sint64_field
      protobuftsProtos.TestMessage.create({
        sint64Field: Long.MIN_VALUE
      }),
      protobuftsProtos.TestMessage.create({
        sint64Field: Long.MAX_VALUE
      }),
      // fixed64_field
      protobuftsProtos.TestMessage.create({
        fixed64Field: Long.UZERO
      }),
      protobuftsProtos.TestMessage.create({
        fixed64Field: Long.MAX_UNSIGNED_VALUE
      }),
      // sfixed64_field
      protobuftsProtos.TestMessage.create({
        sfixed64Field: Long.MIN_VALUE
      }),
      protobuftsProtos.TestMessage.create({
        sfixed64Field: Long.MAX_VALUE
      }),
      // bool_field
      protobuftsProtos.TestMessage.create({
        boolField: false
      }),
      protobuftsProtos.TestMessage.create({
        boolField: true
      }),
      // enum_field
      protobuftsProtos.TestMessage.create({
        enumField: protobuftsProtos.TestEnum.VAL0
      }),
      protobuftsProtos.TestMessage.create({
        enumField: protobuftsProtos.TestEnum.VAL1
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

      protobuftsProtos.TestMessage.create({
        int32Field: -67
      }),
      protobuftsProtos.TestMessage.create({
        uint32Field: 132
      }),
      protobuftsProtos.TestMessage.create({
        sint32Field: -132
      }),
      protobuftsProtos.TestMessage.create({
        int64Field: Long.fromNumber(-112129164178641)
      }),
      protobuftsProtos.TestMessage.create({
        uint64Field: Long.fromNumber(112129164178641).toUnsigned()
      }),
      protobuftsProtos.TestMessage.create({
        sint64Field: Long.fromNumber(-112129164178641)
      }),
      protobuftsProtos.TestMessage.create({
        enumField: protobuftsProtos.TestEnum.VAL2
      }),
      protobuftsProtos.TestMessage.create({
        boolField: true
      }),
      protobuftsProtos.TestMessage.create({
        // TODO: add decimals after doing some correction upon reserialization
        floatField: 13.0
      }),
      protobuftsProtos.TestMessage.create({
        fixed32Field: 123141
      }),
      protobuftsProtos.TestMessage.create({
        sfixed32Field: -123141
      }),
      protobuftsProtos.TestMessage.create({
        // TODO: add decimals after doing some correction upon reserialization
        doubleField: 4.940656458412465441765687928682213723651e-324
      }),
      protobuftsProtos.TestMessage.create({
        fixed64Field: Long.fromNumber(1278319).toUnsigned()
      }),
      protobuftsProtos.TestMessage.create({
        sfixed64Field: Long.fromNumber(-1278319)
      }),
      protobuftsProtos.TestMessage.create({
        stringField: "hello"
      }),
      protobuftsProtos.TestMessage.create({
        bytesField: Uint8Array.from([3, 5, 6, 9])
      }),
      protobuftsProtos.TestMessage.create({
        messageField: protobuftsProtos.TestMessage.create({
          stringField: "hello"
        })
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
