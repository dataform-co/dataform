test("this is my cool test")
  .dataset("example_table")
  .input(
    "sample_data",
    `
        select 'hi' as faked union all
        select 'ben' as faked union all
        select 'sup?' as faked
    `
  )
  .expect(
    `
        select 'hi' as faked union all
        select 'ben' as faked union all
        select 'sup?' as faked
    `
  );
