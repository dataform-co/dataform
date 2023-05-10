table({"tags": ["tagA"]}).sql(
    f"""

select * from {foo("test")}

"""
)
