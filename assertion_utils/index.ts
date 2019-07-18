export function keyUniqueness(dataset: string, keys: string[]): string {
  // Given a dataset name and an array of keys for which the table
  // should be unique, constructs a query to find duplicate rows for
  // that combination of keys
  return `
  WITH base AS (
  
  SELECT
    ${keys.map((field, i) => `${field} as c_${i}`).join(", ")},
    SUM(1) as row_count
  FROM ${dataset}
  GROUP BY 
    ${keys.map((field, i) => `${i+1}`).join(", ")}
  
  )

  SELECT
    *
  FROM
    base
  WHERE
    row_count > 1
  `
}