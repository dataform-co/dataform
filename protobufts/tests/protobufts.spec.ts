import { expect } from "chai";
import Long from "long";

import { execSync } from "child_process";
import * as protobuftsProtos from "df/protobufts/tests/test1";
import { suite, test } from "df/testing";

suite(__filename, { parallel: true }, () => {
  suite("single-field non-repeated reserialization", { parallel: true }, () => {
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

  suite("single-field repeated reserialization", { parallel: true }, () => {
    const testCases = [
      // double_field
      protobuftsProtos.TestRepeatedMessage.create({
        doubleField: [4.940656458412465441765687928682213723651e-324, 35.6]
      }),
      // float_field
      protobuftsProtos.TestRepeatedMessage.create({
        floatField: [1.40129846432481707092372958328991613128e-45, 35.5]
      }),
      // int32_field
      protobuftsProtos.TestRepeatedMessage.create({
        int32Field: [-100, 99, 0, 76, 10231862]
      }),
      // uint32_field
      protobuftsProtos.TestRepeatedMessage.create({
        uint32Field: [89, 3, 67, 0, 213131]
      }),
      // sint32_field
      protobuftsProtos.TestRepeatedMessage.create({
        sint32Field: [-21332, 323, 555, 0, -23123]
      }),
      // fixed32_field
      protobuftsProtos.TestRepeatedMessage.create({
        fixed32Field: [1232, 0, 51232, 222]
      }),
      // sfixed32_field
      protobuftsProtos.TestRepeatedMessage.create({
        sfixed32Field: [-13279, 3232, 0, -231]
      }),
      // int64_field
      protobuftsProtos.TestRepeatedMessage.create({
        int64Field: [Long.fromNumber(12323), Long.ZERO, Long.fromNumber(-121927)]
      }),
      // uint64_field
      protobuftsProtos.TestRepeatedMessage.create({
        uint64Field: [Long.fromNumber(12323, true), Long.UZERO, Long.fromNumber(172168261, true)]
      }),
      // sint64_field
      protobuftsProtos.TestRepeatedMessage.create({
        sint64Field: [Long.fromNumber(1212), Long.ZERO, Long.fromNumber(-1271333)]
      }),
      // fixed64_field
      protobuftsProtos.TestRepeatedMessage.create({
        fixed64Field: [Long.fromNumber(1323, true), Long.fromNumber(0, true)]
      }),
      // sfixed64_field
      protobuftsProtos.TestRepeatedMessage.create({
        sfixed64Field: [
          Long.fromNumber(-1821921),
          Long.fromNumber(-1),
          Long.fromNumber(12121982172)
        ]
      }),
      // bool_field
      protobuftsProtos.TestRepeatedMessage.create({
        boolField: [true, false, false, true, true]
      }),
      // enum_field
      protobuftsProtos.TestRepeatedMessage.create({
        enumField: [
          protobuftsProtos.TestEnum.VAL1,
          protobuftsProtos.TestEnum.VAL0,
          protobuftsProtos.TestEnum.VAL2
        ]
      }),
      // string_field
      protobuftsProtos.TestRepeatedMessage.create({
        stringField: ["", "foo", "bar"]
      }),
      // message_field
      protobuftsProtos.TestRepeatedMessage.create({
        messageField: [
          protobuftsProtos.TestMessage.create({
            stringField: "one"
          }),
          protobuftsProtos.TestMessage.create({
            stringField: "two"
          })
        ]
      }),
      // bytes_field
      protobuftsProtos.TestRepeatedMessage.create({
        bytesField: [
          Uint8Array.from([5, 8, 19, 33]),
          Uint8Array.from([50]),
          Uint8Array.from([89, 0])
        ]
      })
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
