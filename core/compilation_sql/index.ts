import { dataform } from "df/protos/ts";

export class CompilationSql {
  constructor(
    private readonly project: dataform.IProjectConfig,
    private readonly dataformCoreVersion: string
  ) {}

  public resolveTarget(target: dataform.ITarget) {
    const database = target.database || this.project.defaultDatabase;
    if (!database) {
      return `\`${target.schema || this.project.defaultSchema}.${target.name}\``;
    }
    return `\`${database}.${target.schema || this.project.defaultSchema}.${target.name}\``;
  }

  public sqlString(stringContents: string) {
    // Escape escape characters, then escape single quotes, then wrap the string in single quotes.
    return `'${stringContents.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
  }

  public indexAssertion(dataset: string, indexCols: string[]) {
    const commaSeparatedColumns = indexCols.join(", ");
    return `
SELECT
  *
FROM (
  SELECT
    ${commaSeparatedColumns},
    COUNT(1) AS index_row_count
  FROM ${dataset}
  GROUP BY ${commaSeparatedColumns}
  ) AS data
WHERE index_row_count > 1
`;
  }

  public rowConditionsAssertion(dataset: string, rowConditions: string[]) {
    return rowConditions
      .map(
        (rowCondition: string) => `
SELECT
  ${this.sqlString(rowCondition)} AS failing_row_condition,
  *
FROM ${dataset}
WHERE NOT (${rowCondition})
`
      )
      .join(`UNION ALL`);
  }
}
