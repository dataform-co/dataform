print("Creating example table")

table({"tags": ["tagA"]}).sql(
    f"""

select 1

"""
)
