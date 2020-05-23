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
          doubleField: 32.67,
          floatField: 5.677999973297119,
          int64Field: Long.fromNumber(9729136168),
          int32Field: 45,
          stringField: "foo",
          messageField: protobuftsProtos.TestMessage.create({
            doubleField: 101.789,
            floatField: 92222.1015625,
            int64Field: Long.fromNumber(21312313141515),
            int32Field: 12,
            stringField: "bar"
          })
        }),
        deserialize: protobufjsProtos.testprotos.TestMessage.decode,
        out: protobufjsProtos.testprotos.TestMessage.create({
          doubleField: 32.67,
          floatField: 5.677999973297119,
          int64Field: Long.fromNumber(9729136168),
          int32Field: 45,
          stringField: "foo",
          messageField: protobufjsProtos.testprotos.TestMessage.create({
            doubleField: 101.789,
            floatField: 92222.1015625,
            int64Field: Long.fromNumber(21312313141515),
            int32Field: 12,
            stringField: "bar"
          })
        })
      }
    ];

    for (const testCase of testCases) {
      test(testCase.name, () => {
        expect(testCase.deserialize(testCase.in.serialize())).eql(testCase.out);
      });
    }
  });
});
