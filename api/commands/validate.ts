import * as cronParser from "cron-parser";
import { dataform } from "@dataform/protos";


function validateEmail(email: string) {
  const emailRegex = /^(([^<>()\[\]\.,;:\s@\"]+(\.[^<>()\[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$/i;
  return emailRegex.test(email);
}

export function validateSchedules(schedules: dataform.schedules.SchedulesJSON) {

  const errors = [];

  if (schedules.defaultNotification && schedules.defaultNotification.emails) {
    schedules.defaultNotification.emails.forEach(email => {
      if (!validateEmail(email)) {
        errors.push(`${email} is not a valid email address`);
      }
    });
  }

  const uniqueNameMap = new Map();
  schedules.schedules.forEach(schedule => {
    if (!schedule.name) {
      errors.push("schedule name is required");
    }
    if (uniqueNameMap.has(schedule.name)) {
      errors.push(`${schedule.name} is not unique. All the schedules name should be unique.`);
    }
    uniqueNameMap.set(schedule.name, true);

    if (!schedule.cron) {
      errors.push("cron expression is required");
    }

    try {
      const _ = cronParser.parseExpression(schedule.cron);
    } catch (e) {
      errors.push(`${schedule.cron} is not a valid cron expression`);
    }

    if (schedule.notification && schedule.notification.emails) {
      schedule.notification.emails.forEach(email => {
        if (!validateEmail(email)) {
          errors.push(`${email} is not a valid email address`);
        }
      });
    }
  })

  return errors;
}
