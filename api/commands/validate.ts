import * as cronParser from "cron-parser";
import { dataform } from "@dataform/protos";


function validateEmail(email: string) {
  const email_regex = /^(([^<>()\[\]\.,;:\s@\"]+(\.[^<>()\[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$/i;
  return email_regex.test(email);
}

export function validateSchedules(schedules: dataform.schedules.Schedules) {

  if (schedules.defaultNotification && schedules.defaultNotification.emails) {
    schedules.defaultNotification.emails.forEach(email => {
      if (!validateEmail(email)) {
        throw new Error(`${email} is not a valid email address`);
      }
    });
  }

  const uniqueNameMap = new Map();
  schedules.schedules.forEach(schedule => {
    if (!schedule.name) {
      throw new Error("schedule name is required");
    }
    if (uniqueNameMap.has(schedule.name)) {
      throw new Error(`${schedule.name} is not unique. All the schedules name should be unique.`);
    }
    uniqueNameMap.set(schedule.name, true);

    if (!schedule.cron) {
      throw new Error("cron expression is required");
    }

    try {
      const _ = cronParser.parseExpression(schedule.cron);
    } catch (e) {
      throw new Error(`${schedule.cron} is not a valid cron expression`);
    }

    if (schedule.notification && schedule.notification.emails) {
      schedule.notification.emails.forEach(email => {
        if (!validateEmail(email)) {
          throw new Error(`${email} is not a valid email address`);
        }
      });
    }
  })

  return true;
}
