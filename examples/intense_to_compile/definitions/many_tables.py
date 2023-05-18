[table({"name": f"name{i}"}).sql("a" * 500 * 1024) for i in range(250)]
