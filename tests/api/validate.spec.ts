import { validateSchedules } from "@dataform/api";
import { dataform } from "@dataform/protos";
import { assert, config, expect } from "chai";

describe("@dataform/api/validate", () => {
  describe("validateSchedules", () => {

    it("test_valid_schedule", () => {
      const valid_schedule = dataform.schedules.Schedules.create({
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

      assert.isOk(validateSchedules(valid_schedule));

    });

    it("test_unique_name", () => {
      const invalid_schedule = dataform.schedules.Schedules.create({
        schedules: [
          {
            name: "name1",
            cron: "*/2 * * * *"
          },
          {
            name: "name1",
            cron: "*/2 * * * *"
          }
        ]
      });
      assert.throws(() => validateSchedules(invalid_schedule), Error, "name1 is not unique. All the schedules name should be unique.");
    });

    it("test_invalid_cron", () => {
      const invalid_schedule = dataform.schedules.Schedules.create({
        schedules: [
          {
            name: "name1",
            cron: "sdsdf"
          },
          {
            name: "name2",
            cron: "*/2 * * * *"
          }
        ]
      });
      assert.throws(() => validateSchedules(invalid_schedule), Error, "sdsdf is not a valid cron expression");
    });

    it("test_invalid_emails", () => {
      const invalid_schedule_1 = dataform.schedules.Schedules.create({
        defaultNotification: {
          emails: ["test.com"]
        },
        schedules: [
          {
            name: "name1",
            cron: "sdsdf"
          },
          {
            name: "name2",
            cron: "*/2 * * * *"
          }
        ]
      });
      assert.throws(() => validateSchedules(invalid_schedule_1), Error, "test.com is not a valid email address");

      const invalid_schedule_2 = dataform.schedules.Schedules.create({
        schedules: [
          {
            name: "name1",
            cron: "*/2 * * * *",
            notification: {
              emails: ['test.com']
            }
          },
          {
            name: "name2",
            cron: "*/2 * * * *"
          }
        ]
      });
      assert.throws(() => validateSchedules(invalid_schedule_2), Error, "test.com is not a valid email address");
    });
  });
});
