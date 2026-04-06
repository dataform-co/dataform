import { expect } from "chai";
import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "./index";

import { suite, test } from "df/testing";

suite("verifyObjectMatchesProto", () => {
  test("throws error when top-level object is an array", () => {
    const MockProto = {} as any;
    expect(() => {
      verifyObjectMatchesProto(MockProto, [] as any);
    }).to.throw(ReferenceError, "Expected a top-level object, but found an array");
  });

  test("throws error when null value provided for array field and SHOW_DOCS_LINK", () => {
    const MockProto = {
      create: (obj: any) => obj,
      toObject: (proto: any, opts: any) => ({ repeatedField: [] as any[] }),
      getTypeUrl: () => "MockProto"
    } as any;

    expect(() => {
      verifyObjectMatchesProto(
        MockProto,
        { repeatedField: null } as any,
        VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
      );
    }).to.throw(ReferenceError, /Unexpected empty value for "repeatedField"/);
  });

  test("throws error on type mismatch with SUGGEST_REPORTING_TO_DATAFORM_TEAM", () => {
    const MockProto = {
      create: (obj: any) => obj,
      toObject: (proto: any, opts: any) => ({ stringField: "default" }),
      getTypeUrl: () => "MockProto"
    } as any;

    expect(() => {
      verifyObjectMatchesProto(
        MockProto,
        { stringField: 123 } as any,
        VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
      );
    }).to.throw(ReferenceError, /Unexpected property "stringField" for "MockProto".*please report this to the Dataform team/);
  });

  test("throws error on default type mismatch", () => {
    const MockProto = {
      create: (obj: any) => obj,
      toObject: (proto: any, opts: any) => ({ stringField: "default" }),
      getTypeUrl: () => "MockProto"
    } as any;

    expect(() => {
      verifyObjectMatchesProto(MockProto, { stringField: 123 } as any);
    }).to.throw(ReferenceError, /Unexpected property "stringField", or property value type of "number" is incorrect/);
  });
});
