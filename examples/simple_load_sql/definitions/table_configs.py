table(
    {
        "name": "a_table",
        "tags": ["daily"],
        "description": "A table",
        "assertions": {"unique_key": ["post_id"]},
        "partition_by": "date(created_at)",
    }
).load_sql("definitions/a_table.sql")

table(
    {
        "name": "another_table",
        "tags": ["daily"],
        "description": "A table",
        # TODO: Make unique key table names unique.
        "partition_by": "date(created_at)",
    }
).load_sql("definitions/another_table.sql")
