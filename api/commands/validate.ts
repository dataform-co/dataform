import * as cronParser from "cron-parser";
import { dataform } from "@dataform/protos";

const EMAIL_REGEX = /^(([^<>()\[\]\.,;:\s@\"]+(\.[^<>()\[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$/i;

function validateEmail(email: string) {
  return EMAIL_REGEX.test(email);
}

export function validateSchedules(schedules: dataform.schedules.SchedulesJSON, compiledGraph: dataform.ICompiledGraph) {

  const errors = [];
  const allNodes = [].concat(
    compiledGraph.tables,
    compiledGraph.assertions,
    compiledGraph.operations
  );
  const allNodeNames = allNodes.map(node => node.name);

  if (schedules.defaultNotification) {
    schedules.defaultNotification.emails.forEach(email => {
      if (!validateEmail(email)) {
        errors.push(`"${email}" is not a valid email address.`);
      }
    });
  }

  const uniqueNames = new Set<string>();
  schedules.schedules.forEach(schedule => {
    if (!schedule.name) {
      errors.push("Schedule name is required.");
    }
    if (uniqueNames.has(schedule.name)) {
      errors.push(`"${schedule.name}" is not unique. All schedule names must be unique.`);
    }
    uniqueNames.add(schedule.name);

    if (!schedule.cron) {
      errors.push("Cron expression is required.");
    }

    try {
      const _ = cronParser.parseExpression(schedule.cron);
    } catch (e) {
      errors.push(`"${schedule.cron}" is not a valid cron expression.`);
    }

    if (schedule.options) {
      schedule.options.actions.forEach(action => {
        if (allNodeNames.indexOf(action) < 0) {
          errors.push(`"${action}" doesn't exist in the project.`);
        }
      })
    }

    if (schedule.notification && schedule.notification.emails) {
      schedule.notification.emails.forEach(email => {
        if (!validateEmail(email)) {
          errors.push(`"${email}" is not a valid email address.`);
        }
      });
    }
  })

  return errors;
}
