import * as cronParser from "cron-parser";
import { dataform } from "@dataform/protos";
import * as fs from "fs";
import * as path from "path";

const SCHEDULES_JSON_PATH = "schedules.json";
const EMAIL_REGEX = /^(([^<>()\[\]\.,;:\s@\"]+(\.[^<>()\[\]\.,;:\s@\"]+)*)|(\".+\"))@(([^<>()[\]\.,;:\s@\"]+\.)+[^<>()[\]\.,;:\s@\"]{2,})$/i;

interface IOptions {
  projectDir?: string;
}

function validateEmail(email: string) {
  return EMAIL_REGEX.test(email);
}

export function validateSchedulesFile(
  compiledGraph: dataform.ICompiledGraph,
  options: IOptions
): string[] {
  if (!options || !options.projectDir) {
    return [];
  }
  const filePath = path.resolve(path.join(options.projectDir, SCHEDULES_JSON_PATH));
  if (!fs.existsSync(filePath)) {
    return [];
  }
  if (!compiledGraph) {
    return ["Compiled graph not provided."];
  }
  const content = fs.readFileSync(filePath, "utf8");
  try {
    const scheduleJsonObj = JSON.parse(content);
    return validateSchedules(
      dataform.schedules.SchedulesJSON.create(scheduleJsonObj),
      compiledGraph
    );
  } catch (err) {
    return [`${SCHEDULES_JSON_PATH} does not contain valid json.`];
  }
}

export function validateSchedules(
  schedules: dataform.schedules.ISchedulesJSON,
  compiledGraph: dataform.ICompiledGraph
): string[] {
  const errors = [];
  const allActions = new Array<{ name?: string }>().concat(
    compiledGraph.tables,
    compiledGraph.assertions,
    compiledGraph.operations
  );

  const allActionNames = allActions.map(action => action.name);

  if (schedules.defaultNotification && schedules.defaultNotification.emails) {
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
    if (schedule.name && schedule.name.trim().length === 0) {
      errors.push("Schedule name can not be blank.");
    }
    if (uniqueNames.has(schedule.name)) {
      errors.push(
        `Schedule name: "${schedule.name}" is not unique. All schedule names must be unique.`
      );
    }
    uniqueNames.add(schedule.name);

    if (!schedule.cron) {
      errors.push(`Cron expression is required on ${schedule.name}.`);
    }

    try {
      const _ = cronParser.parseExpression(schedule.cron);
    } catch (e) {
      errors.push(
        `Cron expression: "${schedule.cron}" is not a valid on schedule: ${schedule.name}.`
      );
    }

    if (schedule.options && schedule.options.actions) {
      schedule.options.actions.forEach(action => {
        if (allActionNames.indexOf(action) < 0) {
          errors.push(
            `Action: ${action}" included on schedule: ${
              schedule.name
            } doesn't exist in the project.`
          );
        }
      });
    }

    if (schedule.notification && schedule.notification.emails) {
      schedule.notification.emails.forEach(email => {
        if (!validateEmail(email)) {
          errors.push(
            `Email address:"${email}" included on schedule: ${
              schedule.name
            } is not a valid email address.`
          );
        }
      });
    }
  });

  return errors;
}
