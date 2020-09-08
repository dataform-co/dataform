test("successful")
  .dataset("example_table")
  .input(
    "sample_data",
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col4, cast('20200723' as timestamp) as col5 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col4, cast('20200724' as timestamp) as col5 union all
        select 'sup?' as col3, 3 as col2, 9.5 as col3, true as col4, cast('20200725' as timestamp) as col5
    `
  )
  .expect(
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col4, cast('20200723' as timestamp) as col5 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col4, cast('20200724' as timestamp) as col5 union all
        select 'sup?' as col3, 3 as col2, 9.5 as col3, true as col4, cast('20200725' as timestamp) as col5
    `
  );

test("expected more rows than got")
  .dataset("example_table")
  .input(
    "sample_data",
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col4 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col4
    `
  )
  .expect(
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col4 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col4 union all
        select 'sup?' as col3, 3 as col2, 9.5 as col3, true as col4
    `
  );

test("expected fewer columns than got")
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
          select 'hi' as col1, 1 as col2, 3.5 as col3 union all
          select 'ben' as col2, 2 as col2, 6.5 as col3 union all
          select 'sup?' as col3, 3 as col2, 9.5 as col3
    `
  );

test("wrong columns")
  .dataset("example_table")
  .input(
    "sample_data",
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col5 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col5 union all
        select 'sup?' as col3, 3 as col2, 9.5 as col3, true as col5
    `
  )
  .expect(
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col4 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col4 union all
        select 'sup?' as col3, 3 as col2, 9.5 as col3, true as col4
    `
  );

test("wrong row contents")
  .dataset("example_table")
  .input(
    "sample_data",
    `
        select 'hi' as col1, 5 as col2, 3.5 as col3, true as col4 union all
        select 'ben' as col2, 2 as col2, 12 as col3, false as col4 union all
        select 'WRONG' as col3, 3 as col2, 9.5 as col3, true as col4
    `
  )
  .expect(
    `
        select 'hi' as col1, 1 as col2, 3.5 as col3, true as col4 union all
        select 'ben' as col2, 2 as col2, 6.5 as col3, false as col4 union all
        select 'sup?' as col3, 3 as col2, 9.5 as col3, true as col4
    `
  );
