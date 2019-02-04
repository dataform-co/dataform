${type("view")}
-- Make sure it's possible to drop tables which have dependent views.
select * from ${ref("example_table")}
