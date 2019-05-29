import * as cronParser from "cron-parser";
import { dataform } from "@dataform/protos";

const EMAIL_REGEX = /^(([^<>()\[\]\.,;:\s@\"]+(\.[^<>()\[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$/i;

function validateEmail(email: string) {
  return EMAIL_REGEX.test(email);
}

export function validateSchedules(schedules: dataform.schedules.SchedulesJSON) {

  const errors = [];

  if (schedules.defaultNotification && schedules.defaultNotification.emails) {
    schedules.defaultNotification.emails.forEach(email => {
      if (!validateEmail(email)) {
        errors.push(`${email} is not a valid email address.`);
      }
    });
  }

  const uniqueNames = new Set<string>();
  schedules.schedules.forEach(schedule => {
    if (!schedule.name) {
      errors.push("schedule name is required.");
    }
    if (uniqueNames.has(schedule.name)) {
      errors.push(`${schedule.name} is not unique. All the schedules name should be unique.`);
    }
    uniqueNames.add(schedule.name);

    if (!schedule.cron) {
      errors.push("cron expression is required.");
    }

    try {
      const _ = cronParser.parseExpression(schedule.cron);
    } catch (e) {
      errors.push(`${schedule.cron} is not a valid cron expression.`);
    }

    if (schedule.notification && schedule.notification.emails) {
      schedule.notification.emails.forEach(email => {
        if (!validateEmail(email)) {
          errors.push(`${email} is not a valid email address.`);
        }
      });
    }
  })

  return errors;
}
