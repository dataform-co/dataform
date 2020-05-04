import { expect } from "chai";
import * as dfapi from "df/api";
import { validateSchedules } from "df/api";
import { checkDataformJsonValidity } from "df/api/commands/compile";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";
import { TmpDirFixture } from "df/tests/utils/fixtures";
import * as fs from "fs";
import * as path from "path";

const SCHEDULES_JSON_PATH = "schedules.json";

suite("@dataform/api/validate", () => {
  suite("validateSchedules", () => {
    test("returns no errors for valid schedules object", () => {
      const compiledGraph = dataform.CompiledGraph.create({
        tables: [
          {
            name: "action1",
            target: {
              schema: "schema",
              name: "action1"
            }
          },
          {
            name: "action2",
            target: {
              schema: "schema",
              name: "action2"
            }
          },
          {
            name: "schema.action3",
            target: {
              schema: "schema",
              name: "action3"
            }
          }
        ]
      });
      const validSchedule = dataform.schedules.SchedulesJSON.create({
        schedules: [
          {
            name: "name1",
            cron: "*/2 * * * *",
            disabled: false,
            options: {
              actions: ["action2", "action3"]
            }
          },
          {
            name: "name2",
            cron: "*/2 * * * *",
            notification: {
              emails: ["tada@test.com"],
              onSuccess: true,
              onFailure: false
            }
          }
        ]
      });

      const errors = validateSchedules(validSchedule, compiledGraph);
      expect(errors).to.eql([]);
    });

    test("test all errors", () => {
      const compiledGraph = dataform.CompiledGraph.create({
        tables: [
          {
            name: "action1",
            target: {
              schema: "schema",
              name: "action1"
            }
          },
          {
            name: "action2",
            target: {
              schema: "schema",
              name: "action2"
            }
          }
        ]
      });

      const invalidSchedule = dataform.schedules.SchedulesJSON.create({
        schedules: [
          {
            name: "name1",
            cron: "asdas",
            notification: {
              emails: ["test2.com"]
            },
            options: {
              actions: ["action3"]
            }
          },
          {
            name: "name1",
            cron: "*/2 * * * *"
          },
          {
            name: "nam2",
            cron: "*/2 * * * *",
            notification: {
              onFailure: true
            }
          }
        ]
      });

      const errors = validateSchedules(invalidSchedule, compiledGraph);
      const expectedErrors = [
        'Schedule "name1" contains an invalid cron expression "asdas".',
        'Action "action3" included in schedule name1 doesn\'t exist in the project.',
        'Schedule "name1" contains an invalid email address "test2.com".',
        'Schedule name "name1" is not unique. All schedule names must be unique.'
      ];
      expect(errors).to.eql(expectedErrors);
    });

    suite("validate schedules.json file", ({ afterEach }) => {
      const tmpDirFixture = new TmpDirFixture(afterEach);
      const projectsRootDir = tmpDirFixture.createNewTmpDir();

      test("test all errors", async () => {
        const projectName = "schedules-test";
        const projectDir = path.resolve(path.join(projectsRootDir, projectName));
        const filePath = path.resolve(path.join(projectDir, SCHEDULES_JSON_PATH));

        const compiledGraph = dataform.CompiledGraph.create({
          tables: [
            {
              name: "action1",
              target: {
                schema: "schema",
                name: "action1"
              }
            },
            {
              name: "action2",
              target: {
                schema: "schema",
                name: "action2"
              }
            }
          ]
        });
        const invalidJson = {
          schedules: [
            {
              name: "name1",
              cron: "asdas",
              notification: {
                emails: ["test2.com"]
              },
              options: {
                actions: ["action3"]
              }
            },
            {
              name: "name1",
              cron: "*/2 * * * *"
            },
            {
              name: "nam2",
              cron: "*/2 * * * *",
              notification: {
                onFailure: true
              }
            }
          ]
        };

        const project = await dfapi.init(
          projectDir,
          { warehouse: "redshift" },
          { includeSchedules: true, skipInstall: true, includeEnvironments: false }
        );

        fs.writeFileSync(filePath, JSON.stringify(invalidJson));
        const expectedErrors = [
          'Schedule "name1" contains an invalid cron expression "asdas".',
          'Action "action3" included in schedule name1 doesn\'t exist in the project.',
          'Schedule "name1" contains an invalid email address "test2.com".',
          'Schedule name "name1" is not unique. All schedule names must be unique.'
        ];
        const errors = dfapi.validateSchedulesFileIfExists(compiledGraph, projectDir);
        expect(errors).to.be.eql(expectedErrors);
      });
    });
  });

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
          aint_no_warehouse: "redshift",
          defaultSchema: "df_integration_test",
          assertionSchema: "df_integration_test_assertions"
        })
      ).to.throw(/Missing mandatory property: warehouse/);
    });

    test("fails on invalid default schema", async () => {
      expect(() =>
        checkDataformJsonValidity({
          warehouse: "redshift",
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
        warehouse: "redshift",
        defaultSchema: "df_integration_test-",
        assertionSchema: ""
      })
    ).to.not.throw();
  });
});
