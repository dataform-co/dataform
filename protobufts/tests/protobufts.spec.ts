import { expect } from "chai";
import * as protobufjsProtos from "df/protobufts/tests/protobufjs_testprotos_ts_proto";
import * as protobuftsProtos from "df/protobufts/tests/test1";
import { suite, test } from "df/testing";
import Long from "long";

suite(__filename, () => {
  suite("protobufjs serialization compatibility", () => {
    const testCases = [
      {
        name: "simple",
        in: protobuftsProtos.TestMessage.create({
          doubleField: -32.67,
          floatField: 5.677999973297119,
          int64Field: Long.fromNumber(9729136168),
          uint64Field: Long.fromNumber(13315566123232),
          fixed64Field: Long.fromNumber(78576569),
          fixed32Field: 96989,
          boolField: true,
          int32Field: 45,
          stringField: "foo",
          messageField: protobuftsProtos.TestMessage.create({
            doubleField: 101.789,
            floatField: 92222.1015625,
            int64Field: Long.fromNumber(21312313141515),
            uint64Field: Long.fromNumber(21323),
            fixed64Field: Long.fromNumber(12381608099),
            fixed32Field: 7875,
            boolField: true,
            int32Field: 12,
            stringField: "bar",
            bytesField: new Uint8Array([2, 3, 8, 9]),
            uint32Field: 907,
            enumField: protobuftsProtos.TestEnum.VAL1,
            sfixed32Field: 13219,
            sfixed6Field: Long.fromNumber(-151512323),
            sint32Field: 1937291863,
            sint64Field: Long.fromNumber(-918747183)
          }),
          bytesField: new Uint8Array([1, 3, 5, 6]),
          uint32Field: 1223,
          enumField: protobuftsProtos.TestEnum.VAL2,
          sfixed32Field: -7896,
          sfixed6Field: Long.fromNumber(92796),
          sint32Field: -1315123,
          sint64Field: Long.fromNumber(-140977666)
        }),
        deserialize: protobufjsProtos.testprotos.TestMessage.decode,
        out: protobufjsProtos.testprotos.TestMessage.create({
          doubleField: -32.67,
          floatField: 5.677999973297119,
          int64Field: Long.fromNumber(9729136168),
          uint64Field: Long.fromNumber(13315566123232),
          fixed64Field: Long.fromNumber(78576569),
          fixed32Field: 96989,
          boolField: true,
          int32Field: 45,
          stringField: "foo",
          messageField: protobufjsProtos.testprotos.TestMessage.create({
            doubleField: 101.789,
            floatField: 92222.1015625,
            int64Field: Long.fromNumber(21312313141515),
            uint64Field: Long.fromNumber(21323),
            fixed64Field: Long.fromNumber(12381608099),
            fixed32Field: 7875,
            boolField: true,
            int32Field: 12,
            stringField: "bar",
            bytesField: new Uint8Array([2, 3, 8, 9]),
            uint32Field: 907,
            enumField: protobufjsProtos.testprotos.TestEnum.VAL1,
            sfixed32Field: 13219,
            sfixed6Field: Long.fromNumber(-151512323),
            sint32Field: 1937291863,
            sint64Field: Long.fromNumber(-918747183)
          }),
          bytesField: new Uint8Array([1, 3, 5, 6]),
          uint32Field: 1223,
          enumField: protobufjsProtos.testprotos.TestEnum.VAL2,
          sfixed32Field: -7896,
          sfixed6Field: Long.fromNumber(92796),
          sint32Field: -1315123,
          sint64Field: Long.fromNumber(-140977666)
        })
      }
    ];

    for (const testCase of testCases) {
      test(testCase.name, () => {
        // expect(
        //   protobufjsProtos.testprotos.TestMessage.decode(
        //     protobufjsProtos.testprotos.TestMessage.encode({
        //       doubleField: -32.67,
        //       floatField: 5.677999973297119,
        //       int64Field: Long.fromNumber(9729136168),
        //       uint64Field: Long.fromNumber(13315566123232),
        //       int32Field: 45,
        //       stringField: "foo",
        //       messageField: protobufjsProtos.testprotos.TestMessage.create({
        //         doubleField: 101.789,
        //         floatField: 92222.1015625,
        //         int64Field: Long.fromNumber(21312313141515),
        //         // uint64Field: Long.fromNumber(21323),
        //         int32Field: 12,
        //         stringField: "bar"
        //       })
        //     }).finish()
        //   )
        // ).eql(
        //   protobufjsProtos.testprotos.TestMessage.create({
        //     doubleField: -32.67,
        //     floatField: 5.677999973297119,
        //     int64Field: Long.fromNumber(9729136168),
        //     uint64Field: Long.fromNumber(13315566123232),
        //     int32Field: 45,
        //     stringField: "foo",
        //     messageField: protobufjsProtos.testprotos.TestMessage.create({
        //       doubleField: 101.789,
        //       floatField: 92222.1015625,
        //       int64Field: Long.fromNumber(21312313141515),
        //       // uint64Field: Long.fromNumber(21323),
        //       int32Field: 12,
        //       stringField: "bar"
        //     })
        //   })
        // );

        // This JSON.stringify nonsense is here because protobufjs doesn't consistently use
        // Long types for all "Long-ish" fields.
        expect(JSON.stringify(testCase.deserialize(testCase.in.serialize()), null, 4)).eql(
          JSON.stringify(testCase.out, null, 4)
        );
      });
    }
  });
});
