import { expect } from "chai";

import { decode64, encode64, verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "./index";

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
          { metadata: { extraProperties: new CustomConfig() } },
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "extraProperties", or property value type of "object" is incorrect/,
      );
    });
  });

  suite("Structs __proto__ own-property handling", () => {
    test("Structs.toObject defines __proto__ as an own property without mutating prototype", () => {
      const rawStruct: google.protobuf.IStruct = JSON.parse(
        '{"fields":{"__proto__":{"structValue":{"fields":{"marker":{"boolValue":true}}}},"normal":{"stringValue":"ok"}}}',
      );

      const converted = Structs.toObject(rawStruct);

      expect(Object.getPrototypeOf(converted)).to.equal(Object.prototype);
      expect(Reflect.get({}, "marker")).to.equal(undefined);
      expect(Reflect.get(converted, "marker")).to.equal(undefined);
      expect(Object.keys(converted)).to.deep.equal(["__proto__", "normal"]);
      expect(Object.getOwnPropertyDescriptor(converted, "__proto__")?.value).to.deep.equal({
        marker: true,
      });
      expect(converted?.normal).to.equal("ok");
    });

    test("Structs.fromObject preserves own __proto__ keys and round-trips through binary encoding", () => {
      const rawObj: { [key: string]: AnyValue } = JSON.parse(
        '{"__proto__":{"nested":"v"},"normal":42}',
      );

      const struct = Structs.fromObject(rawObj);

      expect(Object.getPrototypeOf(struct.fields)).to.equal(Object.prototype);
      expect(Object.keys(struct.fields)).to.deep.equal(["__proto__", "normal"]);

      const decoded = decode64(google.protobuf.Struct, encode64(google.protobuf.Struct, struct));
      const roundTripped = Structs.toObject(decoded);

      expect(Object.getPrototypeOf(roundTripped)).to.equal(Object.prototype);
      expect(Object.keys(roundTripped)).to.deep.equal(["__proto__", "normal"]);
      expect(Object.getOwnPropertyDescriptor(roundTripped, "__proto__")?.value).to.deep.equal({
        nested: "v",
      });
      expect(roundTripped?.normal).to.equal(42);
    });

    test("verifyObjectMatchesProto preserves own __proto__ keys inside plain and wrapped Struct fields", () => {
      const rawExtraProperties = JSON.parse('{"__proto__":{"tier":1},"owner":"analytics"}');
      Object.freeze(Object.getOwnPropertyDescriptor(rawExtraProperties, "__proto__")?.value);
      Object.freeze(rawExtraProperties);
      const inputConfig = Object.freeze({
        name: "orders",
        metadata: Object.freeze({
          extraProperties: rawExtraProperties,
        }),
      });

      const verified = verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, inputConfig);
      const convertedExtra = Structs.toObject(verified.metadata?.extraProperties);

      expect(Object.getPrototypeOf(convertedExtra)).to.equal(Object.prototype);
      expect(Object.keys(convertedExtra)).to.deep.equal(["__proto__", "owner"]);
      expect(Object.getOwnPropertyDescriptor(convertedExtra, "__proto__")?.value).to.deep.equal({
        tier: 1,
      });
      expect(convertedExtra?.owner).to.equal("analytics");

      const wrappedExtraProperties = Structs.fromObject(JSON.parse('{"__proto__":{"tier":2}}'));
      const verifiedWrapped = verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, {
        metadata: { extraProperties: wrappedExtraProperties },
      });
      const convertedWrapped = Structs.toObject(verifiedWrapped.metadata?.extraProperties);

      expect(Object.getPrototypeOf(convertedWrapped)).to.equal(Object.prototype);
      expect(Object.keys(convertedWrapped)).to.deep.equal(["__proto__"]);
      expect(Object.getOwnPropertyDescriptor(convertedWrapped, "__proto__")?.value).to.deep.equal({
        tier: 2,
      });
    });
  });

  suite("null and empty container validation", () => {
    test("throws ReferenceError with field name on null map values and null array elements", () => {
      expect(() =>
        verifyObjectMatchesProto(
          dataform.WorkflowSettings,
          JSON.parse('{"vars":{"emptyVar":null}}'),
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected empty value for "emptyVar"\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-WorkflowSettings/,
      );

      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          JSON.parse('{"labels":{"env":null}}'),
        ),
      ).to.throw(ReferenceError, /Unexpected empty value for "env"/);

      expect(() =>
        verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, JSON.parse('{"tags":[null]}')),
      ).to.throw(ReferenceError, /Unexpected empty value for "tags\[0\]"/);
    });

    test("rejects null and undefined elements of repeated message fields", () => {
      expect(() =>
        verifyObjectMatchesProto(
          dataform.ActionConfig.TableConfig,
          JSON.parse('{"columns":[null]}'),
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected empty value for "columns\[0\]"\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-ActionConfig-TableConfig/,
      );

      expect(() =>
        verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, {
          dependencyTargets: [{ name: "first" }, undefined],
        }),
      ).to.throw(ReferenceError, /Unexpected empty value for "dependencyTargets\[1\]"/);

      expect(() =>
        verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, {
          columns: [{ path: ["col_a", null] }],
        }),
      ).to.throw(ReferenceError, /Unexpected empty value for "path\[1\]"/);
    });

    test("rejects undefined scalar array elements and map values", () => {
      expect(() =>
        verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, { tags: [undefined] }),
      ).to.throw(ReferenceError, /Unexpected empty value for "tags\[0\]"/);

      expect(() =>
        verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, { tags: new Array(1) }),
      ).to.throw(ReferenceError, /Unexpected empty value for "tags\[0\]"/);

      expect(() =>
        verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, {
          labels: { env: undefined },
        }),
      ).to.throw(ReferenceError, /Unexpected empty value for "env"/);

      expect(() =>
        verifyObjectMatchesProto(
          dataform.WorkflowSettings,
          { vars: { missingVar: undefined } },
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected empty value for "missingVar"\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-WorkflowSettings/,
      );
    });

    test("rejects unknown empty object and array properties at root and nested message levels", () => {
      expect(() =>
        verifyObjectMatchesProto(
          dataform.WorkflowSettings,
          { defaultProject: "p", typoEmptyObj: {} },
          VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "typoEmptyObj", or property value type of "object" is incorrect\. See https:\/\/dataform-co\.github\.io\/dataform\/docs\/reference\/configs#dataform-WorkflowSettings/,
      );

      expect(() =>
        verifyObjectMatchesProto(dataform.WorkflowSettings, {
          defaultProject: "p",
          typoEmptyArr: [],
        }),
      ).to.throw(
        ReferenceError,
        /Unexpected property "typoEmptyArr", or property value type of "object" is incorrect/,
      );

      expect(() =>
        verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, {
          metadata: { typoNestedObj: {} },
        }),
      ).to.throw(
        ReferenceError,
        /Unexpected property "typoNestedObj", or property value type of "object" is incorrect/,
      );

      expect(() =>
        verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, {
          columns: [{ path: ["col_a"], typoColArr: [] }],
        }),
      ).to.throw(
        ReferenceError,
        /Unexpected property "typoColArr", or property value type of "object" is incorrect/,
      );
    });

    test("rejects own __proto__ properties on non-Struct proto messages", () => {
      expect(() =>
        verifyObjectMatchesProto(
          dataform.WorkflowSettings,
          JSON.parse('{"defaultProject":"p","__proto__":{}}'),
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "__proto__", or property value type of "object" is incorrect/,
      );

      expect(() =>
        verifyObjectMatchesProto(
          dataform.WorkflowSettings,
          JSON.parse('{"defaultProject":"p","__proto__":[]}'),
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "__proto__", or property value type of "object" is incorrect/,
      );

      expect(() =>
        verifyObjectMatchesProto(
          dataform.WorkflowSettings,
          JSON.parse('{"defaultProject":"p","__proto__":{"nested":1}}'),
        ),
      ).to.throw(
        ReferenceError,
        /Unexpected property "__proto__", or property value type of "object" is incorrect/,
      );

      expect(() =>
        verifyObjectMatchesProto(
          dataform.WorkflowSettings,
          JSON.parse('{"defaultProject":"p","__proto__":null}'),
        ),
      ).to.throw(ReferenceError, /Unexpected empty value for "__proto__"/);
    });

    test("accepts valid empty containers, undefined optional properties and __proto__ map keys", () => {
      const verifiedTable = verifyObjectMatchesProto(dataform.ActionConfig.TableConfig, {
        name: "orders",
        dataset: undefined,
        project: undefined,
        tags: [],
        columns: [],
        labels: {},
        additionalOptions: {},
        assertions: {},
        iceberg: {},
        metadata: { extraProperties: {} },
      });

      expect(asPlainObject(verifiedTable)).to.deep.equal({
        name: "orders",
        assertions: {},
        iceberg: {},
        metadata: { extraProperties: {} },
      });

      const verifiedOmittedContainers = verifyObjectMatchesProto(
        dataform.ActionConfig.TableConfig,
        {
          name: "orders",
          tags: undefined,
          labels: undefined,
          metadata: { extraProperties: { fields: {} } },
        },
      );

      expect(asPlainObject(verifiedOmittedContainers)).to.deep.equal({
        name: "orders",
        metadata: { extraProperties: {} },
      });

      const verifiedSettings = verifyObjectMatchesProto(
        dataform.WorkflowSettings,
        JSON.parse('{"defaultProject":"p","vars":{"__proto__":"v","env":"prod"}}'),
      );

      expect(Object.getPrototypeOf(verifiedSettings.vars)).to.equal(Object.prototype);
      expect(Object.keys(verifiedSettings.vars)).to.deep.equal(["__proto__", "env"]);
      expect(Object.getOwnPropertyDescriptor(verifiedSettings.vars, "__proto__")?.value).to.equal(
        "v",
      );
      expect(verifiedSettings.vars.env).to.equal("prod");
    });
  });
});
