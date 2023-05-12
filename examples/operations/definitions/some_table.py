table(
    {
        "columns": [
            {"description": "columnA", "path": ["pathA"]},
            {"description": "columnB", "path": ["pathB"]},
        ],
    }
).pre_operations(["SELECT 1"]).sql(
    f"""
select * from {foo("test")}
"""
).post_operations(
    ["SELECT 2"]
)
