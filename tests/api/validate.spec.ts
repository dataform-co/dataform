import { validateSchedules } from "@dataform/api";
import { dataform } from "@dataform/protos";
import { assert, config, expect } from "chai";

describe("@dataform/api/validate", () => {
  describe("validateSchedules", () => {

    it("returns no errors for valid schedules object", () => {
      const compiledGraph = dataform.CompiledGraph.create({
        tables: [
          {
            name: "action1"
          },
          {
            name: "action2"
          }
        ]
      });
      const valid_schedule = dataform.schedules.SchedulesJSON.create({
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
              actions: ["action2"]
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
      })

      const errors = validateSchedules(valid_schedule, compiledGraph);
      expect(errors).to.eql([]);
    });

    it("test all errors", () => {
      const compiledGraph = dataform.CompiledGraph.create({
        tables: [
          {
            name: "action1"
          },
          {
            name: "action2"
          }
        ]
      });

      const invalid_schedule = dataform.schedules.SchedulesJSON.create({
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
          }
        ]
      });
      const errors = validateSchedules(invalid_schedule, compiledGraph);
      const expectedErrors = [
        '"asdas" is not a valid cron expression.',
        '"name1" is not unique. All schedule names must be unique.',
        '"test.com" is not a valid email address.',
        '"action3" doesn\'t exist in the project.',
        '"test2.com" is not a valid email address.'
      ];
      expect(errors).to.have.members(expectedErrors);
    });
  });
});
