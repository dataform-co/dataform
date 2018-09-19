${type("table")}
select sample, "${macros.foo("bar")}" as foobar from ${ref("sample_data")} where true
