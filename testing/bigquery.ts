import { randomBytes } from "crypto";

/** The minimal interface needed to run queries, satisfied by `BigQueryDbAdapter`. */
export interface IQueryExecutor {
  execute(statement: string): Promise<{ rows: any[] }>;
}

const UNIQUE_SUFFIX_BYTES = 4;
const UNIQUE_SUFFIX_PATTERN = `[0-9a-f]{${UNIQUE_SUFFIX_BYTES * 2}}`;
const DATASET_PREFIX_REGEX = /^[A-Za-z0-9_]+$/;

const DEFAULT_STALE_DATASET_MIN_AGE_HOURS = 6;

export function makeUniqueSuffix(): string {
  return randomBytes(UNIQUE_SUFFIX_BYTES).toString("hex");
}

/** Drops the given datasets, including their contents. Best effort: failures are only logged. */
export async function dropDatasets(
  executor: IQueryExecutor,
  project: string,
  datasets: string[],
): Promise<void> {
  await Promise.all(
    datasets.map(async (dataset) => {
      try {
        await executor.execute(`drop schema if exists \`${project}.${dataset}\` cascade`);
      } catch (e) {
        console.warn(`Failed to drop dataset ${dataset}: ${e?.message || e}`);
      }
    }),
  );
}

export interface IDropStaleDatasetsOptions {
  project: string;
  location: string;
  prefixes: string[];
  minAgeHours?: number;
}

export async function dropStaleDatasets(
  executor: IQueryExecutor,
  {
    project,
    location,
    prefixes,
    minAgeHours = DEFAULT_STALE_DATASET_MIN_AGE_HOURS,
  }: IDropStaleDatasetsOptions,
): Promise<void> {
  if (prefixes.length === 0) {
    return;
  }
  for (const prefix of prefixes) {
    // Guards against SQL injection, and against an empty prefix matching every test's datasets.
    if (!DATASET_PREFIX_REGEX.test(prefix)) {
      throw new Error(`Invalid dataset name prefix: "${prefix}"`);
    }
  }
  const namePattern = `^(${prefixes.join("|")})_${UNIQUE_SUFFIX_PATTERN}$`;

  let staleDatasets: string[];
  try {
    const { rows } = await executor.execute(
      `select schema_name
       from \`${project}\`.\`region-${location.toLowerCase()}\`.INFORMATION_SCHEMA.SCHEMATA
       where regexp_contains(schema_name, r'${namePattern}')
         and creation_time < timestamp_sub(current_timestamp(), interval ${minAgeHours} hour)`,
    );
    staleDatasets = rows.map((row: { schema_name: string }) => row.schema_name);
  } catch (e) {
    console.warn(`Failed to list stale datasets: ${e?.message || e}`);
    return;
  }
  // Another run may be dropping the same datasets concurrently, which is fine.
  await dropDatasets(executor, project, staleDatasets);
}
