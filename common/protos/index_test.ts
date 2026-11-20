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

    test("installNonMutatingProtoHandlers is idempotent across repeated calls", () => {
      const verifyBefore = google.protobuf.Struct.verify;
      const fromObjectBefore = google.protobuf.Struct.fromObject;

      Structs.installNonMutatingProtoHandlers();
      Structs.installNonMutatingProtoHandlers();

      expect(google.protobuf.Struct.verify).to.equal(verifyBefore);
      expect(google.protobuf.Struct.fromObject).to.equal(fromObjectBefore);
    });

    test("rejects non-object Struct field values such as strings and arrays", () => {
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: "str" } } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "extraProperties", or property value type of "string" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: [1] } } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "extraProperties", or property value type of "object" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );
    });

    test("rejects non-JSON values inside Struct fields with formatted ReferenceError", () => {
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: { created: new Date() } } } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "created", or property value type of "object" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: { fn: () => 1 } } } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "fn", or property value type of "function" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: { sym: Symbol("x") } } } as any,
          VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "sym" for "dataform\.ActionConfig\.TableConfig", please report this to the Dataform team/,
      );
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: new Date() } } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "extraProperties", or property value type of "object" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: new Map([["a", 1]]) } } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "extraProperties", or property value type of "object" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { iceberg: new Date() } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "iceberg", or property value type of "object" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: { validKey: 1, "": new Date() } } } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "", or property value type of "object" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );
      expect(() => google.protobuf.Struct.fromObject({ fn: () => 1 } as any)).to.throw(
        TypeError,
        /Unsupported Struct value/,
      );
      expect(google.protobuf.Struct.verify([] as any)).to.equal("object expected");
      expect(google.protobuf.Struct.verify(new Date() as any)).to.match(/Unsupported Struct value/);
      expect(google.protobuf.Struct.verify({ fn: () => 1 } as any)).to.match(
        /Unsupported Struct value/,
      );
    });

    test("preserves plain JSON Struct payloads with empty nested objects under fields key", () => {
      const nullProtoSection = Object.create(null);
      nullProtoSection.region = "EU";
      const verified = verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, {
        metadata: { extraProperties: { fields: { user_id: {} }, config: nullProtoSection } },
      } as any);

      expect(Structs.toObject(verified.metadata?.extraProperties)).to.deep.equal({
        fields: { user_id: {} },
        config: { region: "EU" },
      });
    });

    test("rejects custom class instances on Struct fields", () => {
      class CustomConfig {}
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          { metadata: { extraProperties: new CustomConfig() } } as any,
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "extraProperties", or property value type of "object" is incorrect/,
      );
    });
  });
});
