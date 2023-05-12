table(
    {
        "name": "a_table",
        "tags": ["daily"],
        "description": "A table",
        "assertions": {"unique_key": ["post_id"]},
        "partition_by": "date(created_at)",
    }
).load_sql_file("definitions/a_table.sql")

table(
    {
        "name": "another_table",
        "tags": ["daily"],
        "description": "A table",
        "assertions": {"unique_key": ["post_id"]},
        "partition_by": "date(created_at)",
    }
).load_sql_file("definitions/another_table.sql")
