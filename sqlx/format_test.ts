import { expect } from "chai";
import * as fs from "fs";
import * as path from "path";

import { format, formatFile } from "df/sqlx/format";
import { suite, test } from "df/testing";

suite("@dataform/sqlx", () => {
  suite("formatter", () => {
    test("correctly formats a simple SQLX file", async () => {
      const filePath = path.join(process.env.TEST_TMPDIR, "simple.sqlx");
      const fileContents = `config { type: "view",
      tags: ["tag1", "tag2"]
}

js {
const foo = 
  jsFunction("table");
}

select 
1 from \${ref({ schema: "df_integration_test", name: "sample_data" })}
`;
      fs.writeFileSync(filePath, fileContents);
      expect(await formatFile(filePath)).eql(`config {
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

    test("correctly formats multiple_queries", async () => {
      const fileContents = `js {
    var tempTable = "yay"
           const colname =         "column";



           let finalTableName = 'dkaodihwada';
}

drop something


     ---


     alter table \${tempTable} rename to \${finalTableName}

---

SELECT
  SUM(
  IF
    ( session_start_event,
      1,
      0 ) ) AS session_index
`;
      expect(format(fileContents, "sqlx")).eql(`js {
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

    test("correctly formats bigquery regexps", async () => {
      const fileContents = `config { type: "operation",
                      tags: ["tag1", "tag2"]
}

select CAST(REGEXP_EXTRACT("", r'^/([0-9]+)\\'\\"/.*') AS INT64) AS id,
       CAST(REGEXP_EXTRACT("", r"^/([0-9]+)\\"\\'/.*") AS INT64) AS id2,
       IFNULL (
         regexp_extract('', r'\\a?query=([^&]+)&*'),
         regexp_extract('', r'\\a?q=([^&]+)&*')
       ) AS id3,
       regexp_extract('bar', r'bar') as ID4 from \${ref("dab")}

where sample = 100`;
      expect(format(fileContents, "sqlx")).eql(`config {
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

    test("correctly formats comments", async () => {
      const fileContents = `

SELECT
    MAX((SELECT SUM(IF(track.event="event_viewed_project_with_connection",1,0)) FROM UNNEST(records)))>0 as created_project,

/* multi line
     comment      */ 2 as
     foo


input "something" {





    select 1
as test
   /* something */
/* something
     else      */
          -- and another thing







          
}

       config { type: "test",
}
`;
      expect(format(fileContents, "sqlx")).eql(`config {
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
      const fileContents = `select regexp_extract("", r'abc\\de\\'fg select * from self()'), 'bar'`;
      expect(format(fileContents, "sqlx")).equal(`select
  regexp_extract("", r'abc\\de\\'fg select * from self()'),
  'bar'
`);
    });
    test("doesn't care about special string replacement characters", async () => {
      const fileContents = `
      '^.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PhantomJS|a_archiver|facebookexternalhit|BingPreview|360User-agent|semalt).*$'
`;
      expect(format(fileContents, "sqlx"))
        .equal(`'^.*(bot|crawl|slurp|spider|archiv|spinn|sniff|seo|audit|survey|pingdom|worm|capture|(browser|screen)shots|analyz|index|thumb|check|facebook|PhantomJS|a_archiver|facebookexternalhit|BingPreview|360User-agent|semalt).*$'
`);
    });

    test("formats named arguments", async () => {
      const fileContents = `config { type: "operations" }

SELECT MAKE_INTERVAL(1,  day=>2, minute => 3)`;
      expect(format(fileContents, "sqlx")).equal(`config {
  type: "operations"
}

SELECT
  MAKE_INTERVAL(1, day => 2, minute => 3)
`);
    });

    test("formats QUALIFY clause", async () => {
      const fileContents = `
config { type: "operations" }

SELECT * FROM UNNEST([0,1,2,3,4,5,6,7,8,9]) AS n
WHERE n < 8
QUALIFY MOD(ROW_NUMBER() OVER (), 2) = 0`;
      expect(format(fileContents, "sqlx")).equal(`config {
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
      const fileContents = `config { type: "table" }

SELECT

'''1''' AS single_line,

"""multi
  line
    string
      with indent"""
AS multi_line,

REGEXP_CONTAINS(
  "\\n  abc\\n  ",
  r'''
abc
''') AS multi_line_regex,

"""
This project is ...
  "\${database()}"!!
""" AS with_js

post_operations { select """1""" as inner_sql }`;
      expect(format(fileContents, "sqlx")).equal(`config {
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
    test("formats template strings in a string", async () => {
      const fileContents = `
config {
  type: "view"
}
SELECT
  "ok" AS \${  "here"+  "works"  },
  "1 + 2 = \${ 1+2  }" AS TODO_in_string,
  '''\${1  +2  }''' AS TODO_in_triple_quoted_string
`;
      expect(format(fileContents, "sqlx")).eql(`config {
  type: "view"
}

SELECT
  "ok" AS \${"here" + "works"},
  "1 + 2 = \${ 1+2  }" AS TODO_in_string,
  '''\${1  +2  }''' AS TODO_in_triple_quoted_string
`);
    });
  });
});
