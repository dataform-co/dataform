import { expect } from "chai";
import * as path from "path";

import { format, formatFile } from "df/sqlx/format";
import { suite, test } from "df/testing";

suite("@dataform/sqlx", () => {
  suite("formatter", () => {
    test("correctly formats simple.sqlx", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/simple.sqlx")))
        .eql(`config {
  type: "view",
  tags: ["tag1", "tag2"]
}

js {
  const foo =
    jsFunction("table");
}

select
  1
from
  \${
    ref({
      schema: "df_integration_test",
      name: "sample_data"
    })
  }
`);
    });

    test("correctly formats multiple_queries.sqlx", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/multiple_queries.sqlx")))
        .eql(`js {
  var tempTable = "yay"
  const colname = "column";

  let finalTableName = 'dkaodihwada';
}

drop something

---

alter table \${tempTable}
rename to \${finalTableName}

---

SELECT
  SUM(IF(session_start_event, 1, 0)) AS session_index
`);
    });

    test("correctly formats bigquery_regexps.sqlx", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/bigquery_regexps.sqlx")))
        .eql(`config {
  type: "operation",
  tags: ["tag1", "tag2"]
}

select
  CAST(
    REGEXP_EXTRACT("", r'^/([0-9]+)\\'\\"/.*') AS INT64
  ) AS id,
  CAST(
    REGEXP_EXTRACT("", r"^/([0-9]+)\\"\\'/.*") AS INT64
  ) AS id2,
  IFNULL(
    regexp_extract('', r'\\a?query=([^&]+)&*'),
    regexp_extract('', r'\\a?q=([^&]+)&*')
  ) AS id3,
  regexp_extract('bar', r'bar') as ID4
from
  \${ref("dab")}
where
  sample = 100
`);
    });

    test("correctly formats comments.sqlx", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/comments.sqlx")))
        .eql(`config {
  type: "test",
}

SELECT
  MAX(
    (
      SELECT
        SUM(IF(track.event = "event_viewed_project_with_connection", 1, 0))
      FROM
        UNNEST (records)
    )
  ) > 0 as created_project,
  /* multi line
  comment      */
  2 as foo

input "something" {
  select
    1 as test
    /* something */
    /* something
    else      */
    -- and another thing
}
`);
    });
    test("Backslashes within regex don't cause 'r' prefix to separate.", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/regex.sqlx")))
        .equal(`select
  regexp_extract("", r'abc\\de\\'fg select * from self()'),
  'bar'
`);
    });
    test("doesn't care about special string replacement characters", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/dollar_regex.sqlx")))
        .equal(`'^.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PhantomJS|a_archiver|facebookexternalhit|BingPreview|360User-agent|semalt).*$'
`);
    });

    test("formats named arguments", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/named_arguments.sqlx")))
      .equal(`config {
  type: "operations"
}

SELECT
  MAKE_INTERVAL(1, day => 2, minute => 3)
`);
    });

    test("formats QUALIFY clause", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/qualify.sqlx")))
      .equal(`config {
  type: "operations"
}

SELECT
  *
FROM
  UNNEST ([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]) AS n
WHERE
  n < 8
QUALIFY
  MOD(ROW_NUMBER() OVER (), 2) = 0
`);
    });

    test("format triple quoted string", async () => {
      expect(await formatFile(path.resolve("examples/formatter/definitions/triple_quoted.sqlx")))
      .equal(`config {
  type: "table"
}

SELECT
  '''1''' AS single_line,
  """multi
  line
    string
      with indent""" AS multi_line,
  REGEXP_CONTAINS("\\n  abc\\n  ", r'''
abc
''') AS multi_line_regex,
  """
This project is ...
  "\${database()}"!!
""" AS with_js

post_operations {
  select
    """1""" as inner_sql
}
`);
    });
  });

  suite("formatter todos", () => {
    test("TODO format template string in a string", async () => {
      const input = `
        config {
          type: "view"
        }
        SELECT
          "ok" AS \${  "here"+  "works"  },
          "1 + 2 = \${ 1+2  }" AS TODO_in_string,
          '''\${1  +2  }''' AS TODO_in_triple_quoted_string
      `;
      expect(format(input, 'sqlx')).eql(`config {
  type: "view"
}

SELECT
  "ok" AS \${"here" + "works"},
  "1 + 2 = \${ 1+2  }" AS TODO_in_string,
  '''\${1  +2  }''' AS TODO_in_triple_quoted_string
`)});
  })
});
