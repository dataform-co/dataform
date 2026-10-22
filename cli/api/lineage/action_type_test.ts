import { expect } from "chai";

import { isLineageEligibleAction, toLineageActionType } from "df/cli/api/lineage/action_type";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

function actionOfType(type: string): dataform.IExecutionAction {
  return dataform.ExecutionAction.create({
    target: { database: "proj", schema: "schema", name: "name" },
    type,
  });
}

suite("lineage action_type", () => {
  test("actions that run a BigQuery job are eligible and map to Backend enum names", () => {
    const mapped = ["table", "operation", "assertion", "propertyGraph"].map((type) => ({
      type,
      eligible: isLineageEligibleAction(actionOfType(type)),
      actionType: toLineageActionType(actionOfType(type)),
    }));
    expect(mapped).to.deep.equal([
      { type: "table", eligible: true, actionType: "TABLE" },
      { type: "operation", eligible: true, actionType: "OPERATIONS" },
      { type: "assertion", eligible: true, actionType: "ASSERTION" },
      { type: "propertyGraph", eligible: true, actionType: "PROPERTY_GRAPH" },
    ]);
  });

  test("declarations, unknown and missing types are not eligible", () => {
    for (const type of ["declaration", "notebook", "unknown", "", "toString"]) {
      expect(isLineageEligibleAction(actionOfType(type)), type).to.equal(false);
      expect(toLineageActionType(actionOfType(type)), type).to.equal(undefined);
    }
  });
});
