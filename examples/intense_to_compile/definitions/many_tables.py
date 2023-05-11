for i in range(200):
    table({"name": f"name{i}"}).sql(
        # A string of 500kb.
        ("a" * 500 * 1024)
    )
