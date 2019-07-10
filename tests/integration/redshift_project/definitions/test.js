test("test case")
  .dataset("example_table")
  .input(
    "sample_data",
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col4 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col4 union all
        select 'sup?' as col3, 3 as col2, 9.5 as col3, true as col4
    `
  )
  .expect(
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col4 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col4 union all
        select 'sup?' as col3, 3 as col2, 9.5 as col3, true as col4
    `
  );
