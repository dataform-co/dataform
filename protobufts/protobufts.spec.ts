import { expect } from "chai";
import * as newProtos from "df/protos/core";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite(__filename, () => {
  test("serialize", () => {
    expect(
      newProtos.Environments.Environment.create({
        name: "environment",
        configOverride: newProtos.ProjectConfig.create({
          schemaSuffix: "foo"
        })
      }).serialize()
    ).eql(
      dataform.Environments.Environment.encode({
        name: "environment",
        configOverride: {
          schemaSuffix: "foo"
        }
      }).finish()
    );
  });
});
