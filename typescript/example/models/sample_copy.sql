${type("table")}
${config({
  type: "table",
  schema: {
    sample: "Sample field.",
    foobar: "Foobar field"
  }
})}
select sample, "${macros.foo("bar")}" as foobar from ${ref("sample_data")} where true
