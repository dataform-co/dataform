import { expect } from "chai";
import Long from "long";

import { execSync } from "child_process";
import { Flags } from "df/common/flags";
import * as testProtos from "df/protoc-gen-ts/tests/test1";
import { suite, test } from "df/testing";

const flags = {
  reserializerLocation: Flags.string("reserializer-location")
};

suite(__filename, { parallel: true }, () => {
  suite("single-field non-repeated reserialization", { parallel: true }, () => {
    const testCases = [
      // double_field
      testProtos.TestMessage.create({
        doubleField: Number.NaN
      }),
      testProtos.TestMessage.create({
        doubleField: Number.NEGATIVE_INFINITY
      }),
      testProtos.TestMessage.create({
        doubleField: 4.940656458412465441765687928682213723651e-324
      }),
      testProtos.TestMessage.create({
        doubleField: 35.6
      }),
      testProtos.TestMessage.create({
        doubleField: 1.797693134862315708145274237317043567981e308
      }),
      testProtos.TestMessage.create({
        doubleField: Number.POSITIVE_INFINITY
      }),
      // float_field
      testProtos.TestMessage.create({
        floatField: Number.NaN
      }),
      testProtos.TestMessage.create({
        floatField: Number.NEGATIVE_INFINITY
      }),
      testProtos.TestMessage.create({
        floatField: 1.40129846432481707092372958328991613128e-45
      }),
      testProtos.TestMessage.create({
        floatField: 3.4028234663852885981170418348451692544e38
      }),
      testProtos.TestMessage.create({
        floatField: Number.POSITIVE_INFINITY
      }),
      // int64_field
      testProtos.TestMessage.create({
        int64Field: Long.MIN_VALUE
      }),
      testProtos.TestMessage.create({
        int64Field: Long.MAX_VALUE
      }),
      // uint64_field
      testProtos.TestMessage.create({
        uint64Field: Long.UZERO
      }),
      testProtos.TestMessage.create({
        uint64Field: Long.MAX_UNSIGNED_VALUE
      }),
      // int32_field
      testProtos.TestMessage.create({
        int32Field: -2147483648
      }),
      testProtos.TestMessage.create({
        int32Field: 2147483647
      }),
      // fixed64_field
      testProtos.TestMessage.create({
        fixed64Field: Long.UZERO
      }),
      testProtos.TestMessage.create({
        fixed64Field: Long.MAX_UNSIGNED_VALUE
      }),
      // fixed32_field
      testProtos.TestMessage.create({
        fixed32Field: 0
      }),
      // bool_field
      testProtos.TestMessage.create({
        boolField: false
      }),
      testProtos.TestMessage.create({
        boolField: true
      }),
      // string_field
      testProtos.TestMessage.create({
        stringField: ""
      }),
      testProtos.TestMessage.create({
        stringField: "hello world"
      }),
      // message_field
      testProtos.TestMessage.create({
        messageField: testProtos.TestMessage.create({})
      }),
      testProtos.TestMessage.create({
        messageField: testProtos.TestMessage.create({
          stringField: "hello world"
        })
      }),
      // bytes_field
      testProtos.TestMessage.create({
        bytesField: new Uint8Array([])
      }),
      testProtos.TestMessage.create({
        bytesField: new Uint8Array([0x5, 0xff])
      }),
      // uint32_field
      testProtos.TestMessage.create({
        uint32Field: 0
      }),
      testProtos.TestMessage.create({
        uint32Field: 4294967295
      }),
      // enum_field
      testProtos.TestMessage.create({
        enumField: testProtos.TestEnum.VAL0
      }),
      testProtos.TestMessage.create({
        enumField: testProtos.TestEnum.VAL1
      }),
      // sfixed32_field
      testProtos.TestMessage.create({
        sfixed32Field: -2147483648
      }),
      testProtos.TestMessage.create({
        sfixed32Field: 2147483647
      }),
      // sfixed64_field
      testProtos.TestMessage.create({
        sfixed64Field: Long.MIN_VALUE
      }),
      testProtos.TestMessage.create({
        sfixed64Field: Long.MAX_VALUE
      }),
      // sint32_field
      testProtos.TestMessage.create({
        sint32Field: -2147483648
      }),
      testProtos.TestMessage.create({
        sint32Field: 2147483647
      }),
      testProtos.TestMessage.create({
        fixed32Field: 4294967295
      }),
      // sint64_field
      testProtos.TestMessage.create({
        sint64Field: Long.MIN_VALUE
      }),
      testProtos.TestMessage.create({
        sint64Field: Long.MAX_VALUE
      }),
      // oneof_int32_field
      testProtos.TestMessage.create({
        oneof: { field: "oneofInt32Field", value: 0 }
      }),
      testProtos.TestMessage.create({
        oneof: { field: "oneofInt32Field", value: 1234 }
      }),
      // oneof_string_field
      testProtos.TestMessage.create({
        oneof: { field: "oneofStringField", value: "" }
      }),
      testProtos.TestMessage.create({
        oneof: { field: "oneofStringField", value: "hello world" }
      })
    ];

    for (const input of testCases) {
      test(`reserialized ${JSON.stringify(input.toJson())}`, () => {
        expect(input.serialize()).eql(reserialize("TestMessage", input.serialize()));
        expect(testProtos.TestMessage.deserialize(input.serialize())).eql(input);
      });
    }
  });

  suite("single-field repeated packed reserialization", { parallel: true }, () => {
    const testCases = [
      // double_field
      testProtos.TestRepeatedMessage.create({
        doubleField: [
          Number.NaN,
          Number.NEGATIVE_INFINITY,
          4.940656458412465441765687928682213723651e-324,
          35.6,
          Number.POSITIVE_INFINITY
        ]
      }),
      // float_field
      testProtos.TestRepeatedMessage.create({
        floatField: [
          Number.NaN,
          Number.NEGATIVE_INFINITY,
          1.40129846432481707092372958328991613128e-45,
          35.5,
          Number.POSITIVE_INFINITY
        ]
      }),
      // int32_field
      testProtos.TestRepeatedMessage.create({
        int32Field: [-100, 99, 0, 76, 10231862]
      }),
      // uint32_field
      testProtos.TestRepeatedMessage.create({
        uint32Field: [89, 3, 67, 0, 213131]
      }),
      // sint32_field
      testProtos.TestRepeatedMessage.create({
        sint32Field: [-21332, 323, 555, 0, -23123]
      }),
      // fixed32_field
      testProtos.TestRepeatedMessage.create({
        fixed32Field: [1232, 0, 51232, 222]
      }),
      // sfixed32_field
      testProtos.TestRepeatedMessage.create({
        sfixed32Field: [-13279, 3232, 0, -231]
      }),
      // int64_field
      testProtos.TestRepeatedMessage.create({
        int64Field: [Long.fromNumber(12323), Long.ZERO, Long.fromNumber(-121927)]
      }),
      // uint64_field
      testProtos.TestRepeatedMessage.create({
        uint64Field: [Long.fromNumber(12323, true), Long.UZERO, Long.fromNumber(172168261, true)]
      }),
      // sint64_field
      testProtos.TestRepeatedMessage.create({
        sint64Field: [Long.fromNumber(1212), Long.ZERO, Long.fromNumber(-1271333)]
      }),
      // fixed64_field
      testProtos.TestRepeatedMessage.create({
        fixed64Field: [Long.fromNumber(1323, true), Long.fromNumber(0, true)]
      }),
      // sfixed64_field
      testProtos.TestRepeatedMessage.create({
        sfixed64Field: [
          Long.fromNumber(-1821921),
          Long.fromNumber(-1),
          Long.fromNumber(12121982172)
        ]
      }),
      // bool_field
      testProtos.TestRepeatedMessage.create({
        boolField: [true, false, false, true, true]
      }),
      // enum_field
      testProtos.TestRepeatedMessage.create({
        enumField: [testProtos.TestEnum.VAL1, testProtos.TestEnum.VAL0, testProtos.TestEnum.VAL2]
      }),
      // string_field
      testProtos.TestRepeatedMessage.create({
        stringField: ["", "foo", "bar"]
      }),
      // message_field
      testProtos.TestRepeatedMessage.create({
        messageField: [
          testProtos.TestMessage.create({
            stringField: "one"
          }),
          testProtos.TestMessage.create({
            stringField: "two"
          })
        ]
      }),
      // bytes_field
      testProtos.TestRepeatedMessage.create({
        bytesField: [
          Uint8Array.from([5, 8, 19, 33]),
          Uint8Array.from([50]),
          Uint8Array.from([89, 0])
        ]
      })
    ];

    for (const input of testCases) {
      test(`reserialized ${JSON.stringify(input.toJson())}`, () => {
        expect(input.serialize()).eql(reserialize("TestRepeatedMessage", input.serialize()));
        expect(testProtos.TestRepeatedMessage.deserialize(input.serialize())).eql(input);
      });
    }
  });

  suite("single-field repeated unpacked reserialization", { parallel: true }, () => {
    const testCases = [
      // double_field
      testProtos.TestUnpackedRepeatedMessage.create({
        doubleField: [
          Number.NaN,
          Number.NEGATIVE_INFINITY,
          4.940656458412465441765687928682213723651e-324,
          35.6,
          Number.POSITIVE_INFINITY
        ]
      }),
      // float_field
      testProtos.TestUnpackedRepeatedMessage.create({
        floatField: [
          Number.NaN,
          Number.NEGATIVE_INFINITY,
          1.40129846432481707092372958328991613128e-45,
          35.5,
          Number.POSITIVE_INFINITY
        ]
      }),
      // int32_field
      testProtos.TestUnpackedRepeatedMessage.create({
        int32Field: [-100, 99, 0, 76, 10231862]
      }),
      // uint32_field
      testProtos.TestUnpackedRepeatedMessage.create({
        uint32Field: [89, 3, 67, 0, 213131]
      }),
      // sint32_field
      testProtos.TestUnpackedRepeatedMessage.create({
        sint32Field: [-21332, 323, 555, 0, -23123]
      }),
      // fixed32_field
      testProtos.TestUnpackedRepeatedMessage.create({
        fixed32Field: [1232, 0, 51232, 222]
      }),
      // sfixed32_field
      testProtos.TestUnpackedRepeatedMessage.create({
        sfixed32Field: [-13279, 3232, 0, -231]
      }),
      // int64_field
      testProtos.TestUnpackedRepeatedMessage.create({
        int64Field: [Long.fromNumber(12323), Long.ZERO, Long.fromNumber(-121927)]
      }),
      // uint64_field
      testProtos.TestUnpackedRepeatedMessage.create({
        uint64Field: [Long.fromNumber(12323, true), Long.UZERO, Long.fromNumber(172168261, true)]
      }),
      // sint64_field
      testProtos.TestUnpackedRepeatedMessage.create({
        sint64Field: [Long.fromNumber(1212), Long.ZERO, Long.fromNumber(-1271333)]
      }),
      // fixed64_field
      testProtos.TestUnpackedRepeatedMessage.create({
        fixed64Field: [Long.fromNumber(1323, true), Long.fromNumber(0, true)]
      }),
      // sfixed64_field
      testProtos.TestUnpackedRepeatedMessage.create({
        sfixed64Field: [
          Long.fromNumber(-1821921),
          Long.fromNumber(-1),
          Long.fromNumber(12121982172)
        ]
      }),
      // bool_field
      testProtos.TestUnpackedRepeatedMessage.create({
        boolField: [true, false, false, true, true]
      }),
      // enum_field
      testProtos.TestUnpackedRepeatedMessage.create({
        enumField: [testProtos.TestEnum.VAL1, testProtos.TestEnum.VAL0, testProtos.TestEnum.VAL2]
      }),
      // string_field
      testProtos.TestUnpackedRepeatedMessage.create({
        stringField: ["", "foo", "bar"]
      }),
      // message_field
      testProtos.TestUnpackedRepeatedMessage.create({
        messageField: [
          testProtos.TestMessage.create({
            stringField: "one"
          }),
          testProtos.TestMessage.create({
            stringField: "two"
          })
        ]
      }),
      // bytes_field
      testProtos.TestUnpackedRepeatedMessage.create({
        bytesField: [
          Uint8Array.from([5, 8, 19, 33]),
          Uint8Array.from([50]),
          Uint8Array.from([89, 0])
        ]
      })
    ];

    for (const input of testCases) {
      test(`reserialized ${JSON.stringify(input.toJson())}`, () => {
        expect(input.serialize()).eql(
          reserialize("TestUnpackedRepeatedMessage", input.serialize())
        );
        expect(testProtos.TestUnpackedRepeatedMessage.deserialize(input.serialize())).eql(input);
      });
    }
  });

  suite("field type compatibility", { parallel: true }, () => {
    test("repeated Messages are concatenated when interpreted as singular", () => {
      expect(
        testProtos.SingleConcatenatedMessageWrapper.deserialize(
          testProtos.RepeatedConcatenatedMessageWrapper.create({
            concatenatedMessage: [
              testProtos.ConcatenatedMessage.create({
                int32Field: 45,
                stringField: ["foo", "bar"],
                uint32Field: [78, 0]
              }),
              testProtos.ConcatenatedMessage.create({
                int32Field: -89,
                stringField: ["baz"],
                uint32Field: [1, 100]
              })
            ]
          }).serialize()
        )
      ).eql(
        testProtos.SingleConcatenatedMessageWrapper.create({
          concatenatedMessage: testProtos.ConcatenatedMessage.create({
            int32Field: -89,
            stringField: ["foo", "bar", "baz"],
            uint32Field: [78, 0, 1, 100]
          })
        })
      );
    });
  });

  suite("json support", { parallel: true }, () => {
    test("singular fields", () => {
      expect(
        testProtos.TestMessage.create({
          doubleField: 45.8,
          floatField: Number.NaN,
          int64Field: Long.MAX_VALUE,
          uint64Field: Long.UONE,
          int32Field: 190323,
          fixed64Field: Long.fromNumber(12988, true),
          fixed32Field: 4173723,
          boolField: true,
          stringField: "hello world",
          messageField: testProtos.TestMessage.create({
            stringField: "byeeee"
          }),
          bytesField: Uint8Array.from([5, 78, 93, 101]),
          uint32Field: 12455,
          enumField: testProtos.TestEnum.VAL1,
          sfixed32Field: -135131,
          sfixed64Field: Long.fromValue(-9102713712),
          mapField: new Map([
            ["hello", 5],
            ["goodbye", 0]
          ])
        }).toJson()
      ).eql({
        doubleField: 45.8,
        floatField: "NaN",
        int64Field: "9223372036854775807",
        uint64Field: "1",
        int32Field: 190323,
        fixed64Field: "12988",
        fixed32Field: 4173723,
        boolField: true,
        stringField: "hello world",
        messageField: {
          stringField: "byeeee"
        },
        bytesField: "BU5dZQ==",
        uint32Field: 12455,
        enumField: "VAL1",
        sfixed32Field: -135131,
        sfixed64Field: "-9102713712",
        mapField: {
          hello: 5,
          goodbye: 0
        }
      });
    });

    test("repeated fields", () => {
      expect(
        testProtos.TestRepeatedMessage.create({
          doubleField: [45.8, 78.1],
          floatField: [Number.NaN, Number.NEGATIVE_INFINITY],
          int64Field: [Long.MAX_VALUE],
          uint64Field: [Long.UONE],
          int32Field: [190323, -18278],
          fixed64Field: [Long.fromNumber(12988, true)],
          fixed32Field: [4173723],
          boolField: [true, false, false, true],
          stringField: ["hello", "world"],
          messageField: [
            testProtos.TestMessage.create({
              stringField: "byeeee"
            }),
            testProtos.TestMessage.create({
              stringField: "wow"
            })
          ],
          bytesField: [Uint8Array.from([5, 78, 93, 101]), Uint8Array.from([7, 121, 1])],
          uint32Field: [12455],
          enumField: [testProtos.TestEnum.VAL0, testProtos.TestEnum.VAL1],
          sfixed32Field: [-135131],
          sfixed64Field: [Long.fromValue(-9102713712)]
        }).toJson()
      ).eql({
        doubleField: [45.8, 78.1],
        floatField: ["NaN", "-Infinity"],
        int64Field: ["9223372036854775807"],
        uint64Field: ["1"],
        int32Field: [190323, -18278],
        fixed64Field: ["12988"],
        fixed32Field: [4173723],
        boolField: [true, false, false, true],
        stringField: ["hello", "world"],
        messageField: [
          {
            stringField: "byeeee"
          },
          {
            stringField: "wow"
          }
        ],
        bytesField: ["BU5dZQ==", "B3kB"],
        uint32Field: [12455],
        enumField: ["VAL0", "VAL1"],
        sfixed32Field: [-135131],
        sfixed64Field: ["-9102713712"]
      });
    });
  });
});

function reserialize(
  messageType: "TestMessage" | "TestRepeatedMessage" | "TestUnpackedRepeatedMessage",
  bytes: Uint8Array
): Uint8Array {
  const base64EncodedBytes = Buffer.from(bytes).toString("base64");
  const returnedBase64EncodedBytes = execSync(
    `../${flags.reserializerLocation.get()} --proto_type=${messageType} --base64_proto_value=${base64EncodedBytes}`
  ).toString();
  return Buffer.from(returnedBase64EncodedBytes, "base64");
}
