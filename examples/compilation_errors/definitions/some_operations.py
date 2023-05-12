# Columns cannot be defined at the same time as has_output.
operations(
    {"has_output": False, "columns": [{"description": "c", "path": ["p"]}]}
).queries(["SELECT 1"])
