import { expect } from "chai";
import * as path from "path";

import { formatFile } from "df/sqlx/format";
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

alter table
  \${tempTable} rename to \${finalTableName}

---

SELECT
  SUM(IF (session_start_event, 1, 0)) AS session_index
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
  IFNULL (
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
        UNNEST(records)
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
  });
});
