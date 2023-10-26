import { expect } from "chai";

import { checkDataformJsonValidity } from "df/cli/api/commands/compile";
import { suite, test } from "df/testing";

suite("@dataform/api/validate", () => {
  suite("dataform.json validation", async () => {
    test("fails on invalid warehouse", async () => {
      expect(() =>
        checkDataformJsonValidity({
          warehouse: "dataform",
          defaultDatabase: "tada-analytics",
          defaultSchema: "df_integration_test",
          assertionSchema: "df_integration_test_assertions"
        })
      ).to.throw(/Invalid value on property warehouse: dataform/);
    });

    test("fails on missing warehouse", async () => {
      expect(() =>
        checkDataformJsonValidity({
          aint_no_warehouse: "bigquery",
          defaultSchema: "df_integration_test",
          assertionSchema: "df_integration_test_assertions"
        })
      ).to.throw(/Missing mandatory property: warehouse/);
    });

    test("fails on invalid default schema", async () => {
      expect(() =>
        checkDataformJsonValidity({
          warehouse: "bigquery",
          defaultDatabase: "tada-analytics",
          defaultSchema: "rock&roll",
          assertionSchema: "df_integration_test_assertions"
        })
      ).to.throw(
        /Invalid value on property defaultSchema: rock&roll. Should only contain alphanumeric characters, underscores and\/or hyphens./
      );
    });
  });

  test("passes for valid config", async () => {
    expect(() =>
      checkDataformJsonValidity({
        warehouse: "bigquery",
        defaultSchema: "df_integration_test-",
        assertionSchema: ""
      })
    ).to.not.throw();
  });
});
