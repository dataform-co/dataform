import { expect } from "chai";
import * as protobufjsProtos from "df/protobufts/tests/protobufjs_testprotos_ts_proto";
import * as protobuftsProtos from "df/protobufts/tests/test1";
import { suite, test } from "df/testing";

suite(__filename, () => {
  suite("protobufjs serialization compatibility", () => {
    const testCases = [
      {
        name: "simple",
        in: protobuftsProtos.TestMessage.create({
          stringField: "foo",
          int32Field: 45,
          messageField: protobuftsProtos.TestMessage.create({
            stringField: "foo",
            int32Field: 45
          })
        }),
        deserialize: protobufjsProtos.testprotos.TestMessage.decode,
        out: protobufjsProtos.testprotos.TestMessage.create({
          stringField: "foo",
          int32Field: 45,
          messageField: protobufjsProtos.testprotos.TestMessage.create({
            stringField: "foo",
            int32Field: 45
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
