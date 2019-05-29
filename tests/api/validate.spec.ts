import { validateSchedules } from "@dataform/api";
import { dataform } from "@dataform/protos";
import { assert, config, expect } from "chai";

describe("@dataform/api/validate", () => {
  describe("validateSchedules", () => {

    it("test_valid_schedule", () => {
      const valid_schedule = dataform.schedules.SchedulesJSON.create({
        defaultNotification: {
          emails: ["team@dataform.co", "abc@test.com"],
          success: true,
          failure: true
        },
        schedules: [
          {
            name: "name1",
            cron: "*/2 * * * *",
            enabled: false
          },
          {
            name: "name2",
            cron: "*/2 * * * *",
            notification: {
              emails: ['tada@test.com'],
              success: true,
              failure: false
            }
          }
        ]
      })

      const errors = validateSchedules(valid_schedule);
      expect(errors).to.have.members([]);
    });

    it("test all errors", () => {
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
          },
          {
            name: "name1",
            cron: "*/2 * * * *"
          }
        ]
      });
      const errors = validateSchedules(invalid_schedule);
      const expectedErrors = [
        'asdas is not a valid cron expression.',
        'name1 is not unique. All the schedules name should be unique.',
        'test.com is not a valid email address.',
        'test2.com is not a valid email address.'
      ];
      expect(errors).to.have.members(expectedErrors);
    });
  });
});
