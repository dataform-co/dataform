import { dataform } from "df/protos/ts";

export class CompilationSql {
  constructor(
    private readonly project: dataform.IProjectConfig,
    private readonly dataformCoreVersion: string,
  ) {}

  public resolveTarget(target: dataform.ITarget) {
    const database = target.database || this.project.defaultDatabase;
    if (!database) {
      return `\`${target.schema || this.project.defaultSchema}.${target.name}\``;
    }
    return `\`${database}.${target.schema || this.project.defaultSchema}.${target.name}\``;
  }

  public sqlString(stringContents: string) {
    // Escape backslashes first (so the escapes added below are not doubled), then single quotes,
    // then line breaks, and wrap the result in single quotes. BigQuery single-quoted literals
    // cannot span lines; "\n" / "\r" escapes parse back to the original characters.
    return `'${stringContents
      .replace(/\\/g, "\\\\")
      .replace(/'/g, "\\'")
      .replace(/\n/g, "\\n")
      .replace(/\r/g, "\\r")}'`;
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
      .map((rowCondition: string) => {
        // A condition ending in a "--" or "#" line comment would comment out a closing parenthesis
        // on the same line, so close on a new line when the condition has a comment or spans lines.
        const closingParenthesis = /\n|--|#/.test(rowCondition) ? "\n)" : ")";
        return `
SELECT
  ${this.sqlString(rowCondition)} AS failing_row_condition,
  *
FROM ${dataset}
WHERE NOT (${rowCondition}${closingParenthesis}
`;
      })
      .join(`UNION ALL`);
  }
}
