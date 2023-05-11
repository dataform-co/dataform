from typing import List


def sql_string(string_contents: str):
    # TODO: This was originally:
    # Escape escape characters, then escape single quotes, then wrap the string in single quotes.
    # return `'${stringContents.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
    return f"'{string_contents}'"


def index_assertion(dataset: str, index_cols: List[str]):
    comma_separated_columns = ", ".join(index_cols)
    return f"""
SELECT
  *
FROM (
  SELECT
    {comma_separated_columns},
    COUNT(1) AS index_row_count
  FROM `{dataset}`
  GROUP BY {comma_separated_columns}
  ) AS data
WHERE index_row_count > 1
`;
"""


def row_conditions_assertion(dataset: str, row_conditions: List[str]):
    return "UNION ALL".join(
        [
            f"""
SELECT
  {sql_string(row_condition)} AS failing_row_condition,
  *
FROM `{dataset}`
WHERE NOT ({row_condition})
"""
            for row_condition in row_conditions
        ]
    )
