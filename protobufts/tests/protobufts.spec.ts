import { expect } from "chai";
import Long from "long";

import * as protobufjsProtos from "df/protobufts/tests/protobufjs_testprotos_ts_proto";
import * as protobuftsProtos from "df/protobufts/tests/test1";
import { suite, test } from "df/testing";

suite(__filename, () => {
  // suite("protobufjs serialization compatibility", () => {
  //   const testCases = [
  //     {
  //       name: "simple",
  //       in: protobuftsProtos.TestMessage.create({
  //         doubleField: -32.67,
  //         floatField: 5.677999973297119,
  //         int64Field: Long.fromNumber(9729136168),
  //         uint64Field: Long.fromNumber(13315566123232),
  //         fixed64Field: Long.fromNumber(78576569),
  //         fixed32Field: 96989,
  //         boolField: true,
  //         int32Field: 45,
  //         stringField: "foo",
  //         messageField: protobuftsProtos.TestMessage.create({
  //           doubleField: 101.789,
  //           floatField: 92222.1015625,
  //           int64Field: Long.fromNumber(21312313141515),
  //           uint64Field: Long.fromNumber(21323),
  //           fixed64Field: Long.fromNumber(12381608099),
  //           fixed32Field: 7875,
  //           boolField: true,
  //           int32Field: 12,
  //           stringField: "bar",
  //           bytesField: new Uint8Array([2, 3, 8, 9]),
  //           uint32Field: 907,
  //           enumField: protobuftsProtos.TestEnum.VAL1,
  //           sfixed32Field: 13219,
  //           sfixed6Field: Long.fromNumber(-151512323),
  //           sint32Field: 1937291863,
  //           sint64Field: Long.fromNumber(-918747183)
  //         }),
  //         bytesField: new Uint8Array([1, 3, 5, 6]),
  //         uint32Field: 1223,
  //         enumField: protobuftsProtos.TestEnum.VAL2,
  //         sfixed32Field: -7896,
  //         sfixed6Field: Long.fromNumber(92796),
  //         sint32Field: -1315123,
  //         sint64Field: Long.fromNumber(-140977666)
  //       }),
  //       deserialize: protobufjsProtos.testprotos.TestMessage.decode,
  //       out: protobufjsProtos.testprotos.TestMessage.create({
  //         doubleField: -32.67,
  //         floatField: 5.677999973297119,
  //         int64Field: Long.fromNumber(9729136168),
  //         uint64Field: Long.fromNumber(13315566123232),
  //         fixed64Field: Long.fromNumber(78576569),
  //         fixed32Field: 96989,
  //         boolField: true,
  //         int32Field: 45,
  //         stringField: "foo",
  //         messageField: protobufjsProtos.testprotos.TestMessage.create({
  //           doubleField: 101.789,
  //           floatField: 92222.1015625,
  //           int64Field: Long.fromNumber(21312313141515),
  //           uint64Field: Long.fromNumber(21323),
  //           fixed64Field: Long.fromNumber(12381608099),
  //           fixed32Field: 7875,
  //           boolField: true,
  //           int32Field: 12,
  //           stringField: "bar",
  //           bytesField: new Uint8Array([2, 3, 8, 9]),
  //           uint32Field: 907,
  //           enumField: protobufjsProtos.testprotos.TestEnum.VAL1,
  //           sfixed32Field: 13219,
  //           sfixed6Field: Long.fromNumber(-151512323),
  //           sint32Field: 1937291863,
  //           sint64Field: Long.fromNumber(-918747183)
  //         }),
  //         bytesField: new Uint8Array([1, 3, 5, 6]),
  //         uint32Field: 1223,
  //         enumField: protobufjsProtos.testprotos.TestEnum.VAL2,
  //         sfixed32Field: -7896,
  //         sfixed6Field: Long.fromNumber(92796),
  //         sint32Field: -1315123,
  //         sint64Field: Long.fromNumber(-140977666)
  //       })
  //     },
  //     {
  //       name: "repeated",
  //       in: protobuftsProtos.TestRepeatedMessage.create({
  //         doubleField: [-32.67, 1872.3]
  //       }),
  //       deserialize: protobufjsProtos.testprotos.TestRepeatedMessage.decode,
  //       out: protobufjsProtos.testprotos.TestRepeatedMessage.create({
  //         doubleField: [-32.67, 1872.3]
  //       })
  //     }
  //   ];

  //   for (const testCase of testCases) {
  //     test(testCase.name, () => {
  //       // This JSON.stringify nonsense is here because protobufjs doesn't consistently use
  //       // Long types for all "Long-ish" fields.
  //       expect(JSON.stringify(testCase.deserialize(testCase.in.serialize()), null, 4)).eql(
  //         JSON.stringify(testCase.out, null, 4)
  //       );
  //     });
  //   }
  // });

  suite("single-field serialization", () => {
    const testCases = [
      {
        type: "double 4.940656458412465441765687928682213723651e-324",
        proto: protobuftsProtos.TestMessage.create({
          doubleField: 4.940656458412465441765687928682213723651e-324
        }),
        encoded: new Uint8Array([9, 1, 0, 0, 0, 0, 0, 0, 0])
      },
      {
        type: "double 35.6",
        proto: protobuftsProtos.TestMessage.create({
          doubleField: 35.6
        }),
        encoded: new Uint8Array([9, 205, 204, 204, 204, 204, 204, 65, 64])
      },
      {
        type: "double 1.797693134862315708145274237317043567981e+308",
        proto: protobuftsProtos.TestMessage.create({
          doubleField: 1.797693134862315708145274237317043567981e308
        }),
        encoded: new Uint8Array([9, 255, 255, 255, 255, 255, 255, 239, 127])
      },
      {
        type: "float 1.401298464324817070923729583289916131280e-45",
        proto: protobuftsProtos.TestMessage.create({
          floatField: 1.40129846432481707092372958328991613128e-45
        }),
        encoded: new Uint8Array([21, 1, 0, 0, 0])
      },
      {
        type: "float 3.40282346638528859811704183484516925440e+38",
        proto: protobuftsProtos.TestMessage.create({
          floatField: 3.4028234663852885981170418348451692544e38
        }),
        encoded: new Uint8Array([21, 255, 255, 127, 127])
      },
      {
        type: "int64 -9223372036854775808",
        proto: protobuftsProtos.TestMessage.create({
          int64Field: Long.MIN_VALUE
        }),
        encoded: new Uint8Array([24, 128, 128, 128, 128, 128, 128, 128, 128, 128, 1])
      },
      {
        type: "int64 9223372036854775807",
        proto: protobuftsProtos.TestMessage.create({
          int64Field: Long.MAX_VALUE
        }),
        encoded: new Uint8Array([24, 255, 255, 255, 255, 255, 255, 255, 255, 127])
      },
      {
        type: "uint64 0",
        proto: protobuftsProtos.TestMessage.create({
          uint64Field: Long.UZERO
        }),
        encoded: new Uint8Array([])
      },
      {
        type: "uint64 18446744073709551615",
        proto: protobuftsProtos.TestMessage.create({
          uint64Field: Long.MAX_UNSIGNED_VALUE
        }),
        encoded: new Uint8Array([32, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1])
      },
      {
        type: "int32 -2147483648",
        proto: protobuftsProtos.TestMessage.create({
          int32Field: -2147483648
        }),
        encoded: new Uint8Array([40, 128, 128, 128, 128, 248, 255, 255, 255, 255, 1])
      },
      {
        type: "int32 2147483647",
        proto: protobuftsProtos.TestMessage.create({
          int32Field: 2147483647
        }),
        encoded: new Uint8Array([40, 255, 255, 255, 255, 7])
      },
      {
        type: "fixed64 0",
        proto: protobuftsProtos.TestMessage.create({
          fixed64Field: Long.UZERO
        }),
        encoded: new Uint8Array([])
      },
      {
        type: "fixed64 18446744073709551615",
        proto: protobuftsProtos.TestMessage.create({
          fixed64Field: Long.MAX_UNSIGNED_VALUE
        }),
        encoded: new Uint8Array([49, 255, 255, 255, 255, 255, 255, 255, 255])
      },
      {
        type: "fixed32 0",
        proto: protobuftsProtos.TestMessage.create({
          fixed32Field: 0
        }),
        encoded: new Uint8Array([])
      },
      {
        type: "fixed32 4294967295",
        proto: protobuftsProtos.TestMessage.create({
          fixed32Field: 4294967295
        }),
        encoded: new Uint8Array([61, 255, 255, 255, 255])
      },
      {
        type: "bool false",
        proto: protobuftsProtos.TestMessage.create({
          boolField: false
        }),
        encoded: new Uint8Array([])
      },
      {
        type: "bool true",
        proto: protobuftsProtos.TestMessage.create({
          boolField: true
        }),
        encoded: new Uint8Array([64, 1])
      },
      {
        type: "string ''",
        proto: protobuftsProtos.TestMessage.create({
          stringField: ""
        }),
        encoded: new Uint8Array([])
      },
      {
        type: "string 'hello world'",
        proto: protobuftsProtos.TestMessage.create({
          stringField: "hello world"
        }),
        encoded: new Uint8Array([74, 11, 104, 101, 108, 108, 111, 32, 119, 111, 114, 108, 100])
      },
      {
        type: "embedded message ''",
        proto: protobuftsProtos.TestMessage.create({
          messageField: protobuftsProtos.TestMessage.create({})
        }),
        encoded: new Uint8Array([82, 0])
      },
      {
        type: "bytes []",
        proto: protobuftsProtos.TestMessage.create({
          bytesField: new Uint8Array([])
        }),
        encoded: new Uint8Array([])
      },
      {
        type: "bytes [0x5, 0xFF]",
        proto: protobuftsProtos.TestMessage.create({
          bytesField: new Uint8Array([0x5, 0xff])
        }),
        encoded: new Uint8Array([90, 2, 5, 255])
      },
      {
        type: "uint32 0",
        proto: protobuftsProtos.TestMessage.create({
          uint32Field: 0
        }),
        encoded: new Uint8Array([])
      },
      {
        type: "uint32 4294967295",
        proto: protobuftsProtos.TestMessage.create({
          uint32Field: 4294967295
        }),
        encoded: new Uint8Array([96, 255, 255, 255, 255, 15])
      },
      {
        type: "enum VAL0",
        proto: protobuftsProtos.TestMessage.create({
          enumField: protobuftsProtos.TestEnum.VAL0
        }),
        encoded: new Uint8Array([])
      },
      {
        type: "enum VAL1",
        proto: protobuftsProtos.TestMessage.create({
          enumField: protobuftsProtos.TestEnum.VAL1
        }),
        encoded: new Uint8Array([104, 1])
      },
      {
        type: "sfixed32 -2147483648",
        proto: protobuftsProtos.TestMessage.create({
          sfixed32Field: -2147483648
        }),
        encoded: new Uint8Array([117, 0, 0, 0, 128])
      },
      {
        type: "sfixed32 2147483647",
        proto: protobuftsProtos.TestMessage.create({
          sfixed32Field: 2147483647
        }),
        encoded: new Uint8Array([117, 255, 255, 255, 127])
      },
      {
        type: "sfixed64 -9223372036854775808",
        proto: protobuftsProtos.TestMessage.create({
          sfixed64Field: Long.MIN_VALUE
        }),
        encoded: new Uint8Array([121, 0, 0, 0, 0, 0, 0, 0, 128])
      },
      {
        type: "int64 9223372036854775807",
        proto: protobuftsProtos.TestMessage.create({
          sfixed64Field: Long.MAX_VALUE
        }),
        encoded: new Uint8Array([121, 255, 255, 255, 255, 255, 255, 255, 127])
      },
      {
        type: "sint32 -2147483648",
        proto: protobuftsProtos.TestMessage.create({
          sint32Field: -2147483648
        }),
        encoded: new Uint8Array([128, 1, 255, 255, 255, 255, 15])
      },
      {
        type: "sint32 2147483647",
        proto: protobuftsProtos.TestMessage.create({
          sint32Field: 2147483647
        }),
        encoded: new Uint8Array([128, 1, 254, 255, 255, 255, 15])
      },
      {
        type: "sint64 -9223372036854775808",
        proto: protobuftsProtos.TestMessage.create({
          sint64Field: Long.MIN_VALUE
        }),
        encoded: new Uint8Array([136, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1])
      },
      {
        type: "sint64 9223372036854775807",
        proto: protobuftsProtos.TestMessage.create({
          sint64Field: Long.MAX_VALUE
        }),
        encoded: new Uint8Array([136, 1, 254, 255, 255, 255, 255, 255, 255, 255, 255, 1])
      },
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
      }
    ];

    for (const testCase of testCases) {
      test(testCase.type, () => {
        expect(testCase.proto.serialize()).eql(testCase.encoded);
      });
    }
  });
});
