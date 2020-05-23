import { expect } from "chai";
import * as newProtos from "df/protos/core";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite(__filename, () => {
  suite("protobufjs serialization compatibility", () => {
    const testCases = [
      {
        name: "simple",
        in: newProtos.ProjectConfig.create({
          schemaSuffix: "foo",
          idempotentActionRetries: 5
        }),
        deserialize: dataform.ProjectConfig.decode,
        out: dataform.ProjectConfig.create({
          schemaSuffix: "foo",
          idempotentActionRetries: 5
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
