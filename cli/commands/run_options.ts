import yargs from "yargs";

import {
  actionsOption,
  coerceTimeout,
  credentialsOption,
  IActionsArgs,
  ICredentialsArgs,
  IJsonOutputArgs,
  IProjectDirArgs,
  ITimeoutArgs,
  jsonOutputOption,
  requiresSelection,
  splitCommas,
  timeoutOption
} from "df/cli/common_options";
import { IProjectConfigArgs, ProjectConfigOptions } from "df/cli/project_config_options";
import { INamedOption } from "df/cli/yargswrapper";

export interface IRunArgs
  extends IProjectDirArgs,
    IProjectConfigArgs,
    ICredentialsArgs,
    IActionsArgs,
    IJsonOutputArgs,
    ITimeoutArgs {
  dryRun?: boolean;
  runTests?: boolean;
  actionRetryLimit: number;
  emitLineage?: boolean;
  fullRefresh: boolean;
  includeDeps?: boolean;
  includeDependents?: boolean;
  executionTimeout?: number | null;
  jitTimeout?: number | null;
  jobPrefix?: string | null;
  tags?: string[];
  jobLabels?: { [key: string]: string };
}

export const dryRunOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "dry-run",
  option: {
    describe:
      "If set, BigQuery will validate the run SQL without applying changes to the warehouse.",
    type: "boolean"
  }
};

export const runTestsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "run-tests",
  option: {
    describe:
      "If set, the project's unit tests are required to pass before running the project.",
    type: "boolean"
  }
};

export const actionRetryLimitOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "action-retry-limit",
  option: {
    describe: "If set, idempotent actions will be retried up to the limit.",
    type: "number",
    default: 0
  }
};

export const fullRefreshOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "full-refresh",
  option: {
    describe: "Forces incremental tables to be rebuilt from scratch.",
    type: "boolean",
    default: false
  }
};

export const tagsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "tags",
  option: {
    describe: "A list of tags to filter the actions to run.",
    type: "array",
    coerce: splitCommas
  }
};

export const includeDepsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "include-deps",
  option: {
    describe: "If set, dependencies for selected actions will also be run.",
    type: "boolean"
  },
  check: requiresSelection("include-deps", actionsOption, tagsOption)
};

export const includeDependentsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "include-dependents",
  option: {
    describe: "If set, dependents (downstream) for selected actions will also be run.",
    type: "boolean"
  },
  check: requiresSelection("include-dependents", actionsOption, tagsOption)
};

export const emitLineageOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "emit-lineage",
  option: {
    describe:
      "If set, emit OpenLineage RunEvents to Knowledge Catalog Lineage for each executed action. " +
      "Overrides workflow_settings.yaml lineage.enabled when specified.",
    type: "boolean"
  }
};

export const executionTimeoutOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "execution-timeout",
  option: {
    describe:
      "Wall-clock deadline for the entire run (compile + all actions). When it fires, " +
      "in-flight actions are cancelled and pending actions are skipped. Off by default. " +
      "Examples: '10m', '2h'.",
    type: "string",
    default: null,
    coerce: coerceTimeout
  }
};

export const jitTimeoutOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "jit-timeout",
  option: {
    describe:
      "Per-model JiT compilation worker timeout. Each action with jitCode gets " +
      "its own fresh deadline; independent of --execution-timeout. When unset, no " +
      "per-model cap is applied and only --execution-timeout bounds JiT work. " +
      "Examples: '30s', '2m'.",
    type: "string",
    default: null,
    coerce: coerceTimeout
  }
};

export const jobPrefixOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "job-prefix",
  option: {
    describe: "Adds an additional prefix in the form of `dataform-${jobPrefix}-`.",
    type: "string",
    default: null
  }
};

export const bigqueryJobLabelsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "job-labels",
  option: {
    describe:
      "Comma-separated list of labels to add to BigQuery jobs, e.g. 'key1=val1,key2=val2'.",
    type: "string",
    coerce: (raw: string | null) => {
      const labels: { [key: string]: string } = {};
      raw?.split(",").forEach(kv => {
        if (!kv) {
          return;
        }
        const [key, ...rest] = kv.split("=");
        labels[key] = rest.join("=") || "";
      });
      return labels;
    }
  }
};

export const runOptions: Array<INamedOption<yargs.Options, IRunArgs>> = [
  dryRunOption,
  runTestsOption,
  actionRetryLimitOption,
  actionsOption,
  credentialsOption,
  emitLineageOption,
  fullRefreshOption,
  includeDepsOption,
  includeDependentsOption,
  jsonOutputOption,
  timeoutOption,
  executionTimeoutOption,
  jitTimeoutOption,
  jobPrefixOption,
  tagsOption,
  bigqueryJobLabelsOption,
  ...ProjectConfigOptions.allYargsOptions
];
