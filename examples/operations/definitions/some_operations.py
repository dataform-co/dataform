operations({"has_output": True, "disabled": False}).queries(
    [
        "SELECT 1",
        f"CREATE OR REPLACE MODEL {this()} as SELECT 2",
    ]
)
