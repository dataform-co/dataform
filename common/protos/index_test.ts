import { expect } from "chai";

import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "./index";

import { AnyValue, Structs } from "df/common/protos/structs";
import { dataform, google } from "df/protos/ts";
import { asPlainObject, suite, test } from "df/testing";

suite("verifyObjectMatchesProto", () => {
  test("throws error when top-level object is an array", () => {
    expect(() => {
      verifyObjectMatchesProto(dataform.Target, [] as any);
    }).to.throw(ReferenceError, "Expected a top-level object, but found an array");
  });

  test("throws error when null value provided for array field and SHOW_DOCS_LINK", () => {
    expect(() => {
      verifyObjectMatchesProto(
        dataform.Table,
        { dependencyTargets: null } as any,
        VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
      );
    }).to.throw(ReferenceError, /Unexpected empty value for "dependencyTargets"/);
  });

  test("throws error on type mismatch with SUGGEST_REPORTING_TO_DATAFORM_TEAM", () => {
    expect(() => {
      verifyObjectMatchesProto(
        dataform.Table,
        { actionDescriptor: 123 } as any,
        VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM,
      );
    }).to.throw(
      ReferenceError,
      /Unexpected property "actionDescriptor" for ".*Table".*please report this to the Dataform team/,
    );
  });

  test("throws error on default type mismatch", () => {
    expect(() => {
      verifyObjectMatchesProto(dataform.Table, { actionDescriptor: 123 } as any);
    }).to.throw(
      ReferenceError,
      /Unexpected property "actionDescriptor", or property value type of "number" is incorrect/,
    );
  });

  suite("non-mutating google.protobuf.Struct handling", () => {
    test("converts plain JSON Struct fields without mutating frozen caller input", () => {
      const extraProperties = Object.freeze({
        owner: "analytics",
        tier: 1,
        certified: true,
        nullableNote: null,
        tags: Object.freeze(["pii", "finance"]),
        nested: Object.freeze({ region: "EU" }),
        fields: Object.freeze(["col_a", "col_b"]),
      });
      const metadata = Object.freeze({
        overview: "Orders table",
        extraProperties,
      });
      const inputConfig = Object.freeze({
        name: "orders",
        metadata,
      });

      const verified = verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, inputConfig);

      const expectedExtraProperties: { [key: string]: AnyValue } = {
        owner: "analytics",
        tier: 1,
        certified: true,
        nullableNote: null,
        tags: ["pii", "finance"],
        nested: { region: "EU" },
        fields: ["col_a", "col_b"],
      };
      expect(inputConfig.metadata.extraProperties).to.deep.equal(expectedExtraProperties);
      expect(verified.metadata?.extraProperties).to.be.instanceOf(google.protobuf.Struct);
      expect(Structs.toObject(verified.metadata?.extraProperties)).to.deep.equal(
        expectedExtraProperties,
      );
    });

    test("accepts already-wrapped IStruct objects without re-wrapping or mutating", () => {
      const wrappedExtraProperties = Object.freeze({
        fields: Object.freeze({
          priority: Object.freeze({ stringValue: "high" }),
        }),
      });
      const inputConfig = Object.freeze({
        metadata: Object.freeze({
          extraProperties: wrappedExtraProperties,
        }),
      });

      const verified = verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, inputConfig);

      expect(inputConfig.metadata.extraProperties).to.equal(wrappedExtraProperties);
      expect(asPlainObject(verified)).to.deep.equal({
        metadata: {
          extraProperties: {
            fields: {
              priority: { stringValue: "high" },
            },
          },
        },
      });
    });

    test("google.protobuf.Struct.fromObject and Struct.verify do not mutate caller objects", () => {
      const callerObj = Object.freeze({ env: "prod", count: 3, fields: Object.freeze(["x"]) });

      expect(google.protobuf.Struct.verify(callerObj)).to.equal(null);
      const structProto = google.protobuf.Struct.fromObject(callerObj);

      expect(callerObj).to.deep.equal({ env: "prod", count: 3, fields: ["x"] });
      expect(asPlainObject(structProto)).to.deep.equal({
        fields: {
          env: { stringValue: "prod" },
          count: { numberValue: 3 },
          fields: {
            listValue: {
              values: [{ stringValue: "x" }],
            },
          },
        },
      });
    });
  });
});
