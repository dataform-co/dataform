# A string of 100kb.
str = "a" * 100 * 1024

for i in range(40):
    table(f"name${i}").query(f"query{i}")
