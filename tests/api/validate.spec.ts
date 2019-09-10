import { validateSchedules } from "@dataform/api";
import * as dfapi from "@dataform/api";
import { dataform } from "@dataform/protos";
import { assert, config, expect } from "chai";
import { TmpDirFixture } from "df/tests/utils/fixtures";
import * as fs from "fs";
import * as path from "path";

const SCHEDULES_JSON_PATH = "schedules.json";

describe("@dataform/api/validate", () => {
  describe("validateSchedules", () => {
    it("returns no errors for valid schedules object", () => {
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
        defaultNotification: {
          emails: ["team@dataform.co", "abc@test.com"],
          onSuccess: true,
          onFailure: true
        },
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

    it("test all errors", () => {
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
        defaultNotification: {
          emails: ["test.com"]
        },
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
        '"test.com" is not a valid email address.',
        'Schedule "name1" contains an invalid cron expression "asdas".',
        'Action "action3" included on schedule name1 doesn\'t exist in the project.',
        'Schedule "name1" contains an invalid email address "test2.com".',
        'Schedule name "name1" is not unique. All schedule names must be unique.'
      ];
      expect(errors).to.eql(expectedErrors);
    });

    describe("validate schedules.json file", () => {
      const tmpDirFixture = new TmpDirFixture(afterEach);
      const projectsRootDir = tmpDirFixture.createNewTmpDir();

      it("test all errors", async () => {
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
          defaultNotification: {
            emails: ["test.com"]
          },
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
          { includeSchedules: true, skipInstall: true }
        );

        fs.writeFileSync(filePath, JSON.stringify(invalidJson));
        const expectedErrors = [
          '"test.com" is not a valid email address.',
          'Schedule "name1" contains an invalid cron expression "asdas".',
          'Action "action3" included on schedule name1 doesn\'t exist in the project.',
          'Schedule "name1" contains an invalid email address "test2.com".',
          'Schedule name "name1" is not unique. All schedule names must be unique.'
        ];
        const errors = dfapi.validateSchedulesFileIfExists(compiledGraph, projectDir);
        expect(errors).to.be.eql(expectedErrors);
      });
    });
  });
});
