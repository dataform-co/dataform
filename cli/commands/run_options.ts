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
  timeoutOption,
} from "df/cli/common_options";
import { IProjectConfigArgs, ProjectConfigOptions } from "df/cli/project_config_options";
import { INamedOption } from "df/cli/yargswrapper";

export interface IRunArgs
  extends
    IProjectDirArgs,
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
  buildOnly: boolean;
}

export const dryRunOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "dry-run",
  option: {
    describe: "Validates the run SQL with BigQuery without applying any changes to the warehouse.",
    type: "boolean",
  },
};

export const runTestsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "run-tests",
  option: {
    describe:
      "Requires the project's unit tests to pass before executing; aborts the run if any test " +
      "fails.",
    type: "boolean",
  },
};

export const actionRetryLimitOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "action-retry-limit",
  option: {
    describe: "Retries idempotent actions up to this many times on failure.",
    type: "number",
    default: 0,
  },
};

export const fullRefreshOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "full-refresh",
  option: {
    describe: "Forces incremental tables (unless marked protected) to be rebuilt from scratch.",
    type: "boolean",
    default: false,
  },
};

export const tagsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "tags",
  option: {
    describe: "Tags to filter the actions to run (comma-separated or repeatable).",
    type: "array",
    coerce: splitCommas,
  },
};

export const includeDepsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "include-deps",
  option: {
    describe:
      "Also runs upstream dependencies of the selected actions. Requires --actions or --tags.",
    type: "boolean",
  },
  check: requiresSelection("include-deps", actionsOption, tagsOption),
};

export const includeDependentsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "include-dependents",
  option: {
    describe:
      "Also runs downstream dependents of the selected actions. Requires --actions or --tags.",
    type: "boolean",
  },
  check: requiresSelection("include-dependents", actionsOption, tagsOption),
};

export const emitLineageOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "emit-lineage",
  option: {
    describe:
      "If set, emits OpenLineage RunEvents to Knowledge Catalog Lineage for each executed " +
      "action. Overrides workflow_settings.yaml lineage.enabled when specified. See " +
      "https://github.com/dataform-co/dataform/blob/main/docs/lineage.md.",
    type: "boolean",
  },
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
    coerce: coerceTimeout,
  },
};

export const jitTimeoutOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "jit-timeout",
  option: {
    describe:
      "Per-model JiT compilation worker timeout. Each JiT action gets its own fresh " +
      "deadline, independent of --execution-timeout. When unset, no " +
      "per-model cap is applied and only --execution-timeout bounds JiT work. " +
      "Examples: '30s', '2m'.",
    type: "string",
    default: null,
    coerce: coerceTimeout,
  },
};

export const jobPrefixOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "job-prefix",
  option: {
    describe: "Adds an additional prefix to BigQuery job IDs, in the form dataform-<job-prefix>-.",
    type: "string",
    default: null,
  },
};

export const bigqueryJobLabelsOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "job-labels",
  option: {
    describe:
      "Comma-separated list of labels to add to BigQuery jobs, e.g. 'key1=val1,key2=val2'. " +
      "Also applied to unit tests with --run-tests. " +
      "Overrides job labels and table labels that use the same key.",
    type: "string",
    coerce: (raw: string | null) => {
      const labels: { [key: string]: string } = {};
      raw?.split(",").forEach((kv) => {
        if (!kv) {
          return;
        }
        const [key, ...rest] = kv.split("=");
        labels[key] = rest.join("=") || "";
      });
      return labels;
    },
  },
};

export const buildOnlyOption: INamedOption<yargs.Options, IRunArgs> = {
  name: "build-only",
  option: {
    describe:
      "Only builds the execution graph without submitting any queries to BigQuery. JiT actions are not compiled. " +
      "Note that --dry-run and --run-tests will be ignored if combined with --build-only.",
    type: "boolean",
    default: false,
  },
};

export const runOptions: Array<INamedOption<yargs.Options, IRunArgs>> = [
  dryRunOption,
  runTestsOption,
  actionRetryLimitOption,
  actionsOption,
  tagsOption,
  includeDepsOption,
  includeDependentsOption,
  fullRefreshOption,
  credentialsOption,
  jsonOutputOption,
  timeoutOption,
  executionTimeoutOption,
  jitTimeoutOption,
  jobPrefixOption,
  bigqueryJobLabelsOption,
  emitLineageOption,
  buildOnlyOption,
  ...ProjectConfigOptions.allYargsOptions,
];
