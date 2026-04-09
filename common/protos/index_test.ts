import { expect } from "chai";
import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "./index";

import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

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
        VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
      );
    }).to.throw(ReferenceError, /Unexpected empty value for "dependencyTargets"/);
  });

  test("throws error on type mismatch with SUGGEST_REPORTING_TO_DATAFORM_TEAM", () => {
    expect(() => {
      verifyObjectMatchesProto(
        dataform.Table,
        { actionDescriptor: 123 } as any,
        VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
      );
    }).to.throw(
      ReferenceError,
      /Unexpected property "actionDescriptor" for ".*Table".*please report this to the Dataform team/
    );
  });

  test("throws error on default type mismatch", () => {
    expect(() => {
      verifyObjectMatchesProto(dataform.Table, { actionDescriptor: 123 } as any);
    }).to.throw(
      ReferenceError,
      /Unexpected property "actionDescriptor", or property value type of "number" is incorrect/
    );
  });
});
