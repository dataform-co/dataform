import { expect } from "chai";

import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import { Sql } from "df/sql";
import { build, ISelectOrBuilder, ISelectSchema } from "df/sql/builders/select";
import { suite, test } from "df/testing";

suite("builders", { parallel: true }, ({ before, after }) => {
  let bigquery: dbadapters.IDbAdapter;
  let snowflake: dbadapters.IDbAdapter;
  let redshift: dbadapters.IDbAdapter;

  before("create bigquery", async () => {
    bigquery = await dbadapters.create(
      { ...dfapi.credentials.read("bigquery", "test_credentials/bigquery.json"), location: "EU" },
      "bigquery"
    );
  });

  after("close bigquery", () => bigquery.close());

  before("create snowflake", async () => {
    snowflake = await dbadapters.create(
      dfapi.credentials.read("snowflake", "test_credentials/snowflake.json"),
      "snowflake"
    );
  });

  after("close snowflake", () => snowflake.close());

  before("create redshift", async () => {
    redshift = await dbadapters.create(
      dfapi.credentials.read("redshift", "test_credentials/redshift.json"),
      "redshift"
    );
  });

  after("close redshift", () => redshift.close());

  for (const { name, dbadapter, sql } of [
    { name: "bigquery", dbadapter: () => bigquery, sql: new Sql("standard") },
    { name: "snowflake", dbadapter: () => snowflake, sql: new Sql("snowflake") }
    // Redshift tests disabled temporarily.
  ]) {
    suite(name, { parallel: true }, () => {
      const execute = async <S extends ISelectSchema>(select: ISelectOrBuilder<S>) => {
        const query = build(select).query;
        try {
          const rows = (await dbadapter().execute(query)).rows as S[];
          // Snowflake upper cases column names. Turn them back to lowercase for easier testing.
          return rows.map(row =>
            Object.keys(row).reduce(
              (acc, key) => ({ ...acc, [String(key).toLowerCase()]: row[key] }),
              {}
            )
          );
        } catch (e) {
          throw new Error(`Error during query: ${e}\n${query}`);
        }
      };

      test("timestamps", async () => {
        const rows = [
          {
            millis: 1591786375000,
            truncated_millis: 1591747200000
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          millis: sql.timestamps.toMillis(sql.asTimestamp(sql.timestamps.fromMillis("millis"))),
          truncated_millis: sql.timestamps.toMillis(
            sql.timestamps.truncate(sql.asTimestamp(sql.timestamps.fromMillis("millis")), "day")
          )
        });

        const result = await execute(query);
        expect(result).deep.equals(rows);
      });

      test("conditionals", async () => {
        const rows = [
          {
            v: "a"
          },
          {
            v: "b"
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          ifac: sql.conditional(sql.in("v", [sql.literal("a")]), sql.literal("c"), "v"),
          ifbd: sql.conditional(sql.equals("v", sql.literal("b")), sql.literal("d"), "v")
        });

        const result = await execute(query);
        expect(result).deep.equals([
          {
            ifac: "c",
            ifbd: "a"
          },
          {
            ifac: "b",
            ifbd: "d"
          }
        ]);
      });

      test("safe divide", async () => {
        const rows = [
          {
            a: 4,
            b: 2
          },
          {
            a: 4,
            b: 0
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          v: sql.safeDivide("a", "b")
        });

        const result = await execute(query);
        expect(result).deep.equals([
          {
            v: 2
          },
          {
            v: null
          }
        ]);
      });

      test("safe divide", async () => {
        const rows = [
          {
            a: 4,
            b: 2
          },
          {
            a: 4,
            b: 0
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          v: sql.safeDivide("a", "b")
        });

        const result = await execute(query);
        expect(result).deep.equals([
          {
            v: 2
          },
          {
            v: null
          }
        ]);
      });

      test("as string", async () => {
        const rows = [
          {
            a: 1,
            b: "b"
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          a: sql.asString("a"),
          b: sql.asString("b")
        });

        const result = await execute(query);
        expect(result).deep.equals([
          {
            a: "1",
            b: "b"
          }
        ]);
      });

      test("surrogate key", async () => {
        const rows = [
          {
            a: 1,
            b: "b"
          },
          {
            a: 2,
            b: "c"
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          key: sql.surrogateKey(["a", "b"])
        });

        const result: any = await execute(query);
        expect(result.length).equals(2);
        expect(result[0].key).not.equals(result[1].key);
      });

      test("window function", async () => {
        const rows = [
          {
            key: 1,
            sort: 1,
            value: 1,
            partition_field: "a"
          },
          {
            key: 2,
            sort: 2,
            value: 1,
            partition_field: "a"
          },
          {
            key: 3,
            sort: 3,
            value: null,
            partition_field: "b"
          },
          {
            key: 4,
            sort: 4,
            value: 1,
            partition_field: "b"
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          lag: sql.windowFunction("lag", "value", false, { orderFields: ["key"] }),
          max: sql.windowFunction("max", "value", false, { partitionFields: ["partition_field"] }),
          first_value: sql.windowFunction("first_value", "value", true, {
            partitionFields: ["partition_field"],
            orderFields: ["sort"]
          })
        });

        const result: any = await execute(query);
        expect(result.length).equals(4);
        expect(result[1].lag).equals(1);
        expect(result[0].max).equals(1);
        expect(result[0].first_value).equals(1);
      });
      // skipping this test for redshift as the function is only supported on user-defined-tables
      // i.e. not when just querying a select statement with no table
      if (name !== "redshift") {
        test("string agg", async () => {
          const rows = [
            {
              a: "foo"
            },
            {
              a: "bar"
            }
          ];
          const query = sql.from(sql.json(rows)).select({
            agg: sql.stringAgg("a"),
            agg_hyphen: sql.stringAgg("a", "-")
          });

          const result: any = await execute(query);
          expect(result.length).equals(1);
          expect(result[0]).deep.equals({
            agg: "foo,bar",
            agg_hyphen: "foo-bar"
          });
        });
      }

      test("timestamp diff", async () => {
        const rows = [
          {
            millis: 1604575623426,
            previous_day: 1604489223426,
            previous_hour: 1604572023426,
            previous_minute: 1604575563426,
            previous_second: 1604575622426,
            previous_millisecond: 1604575623425
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          day: sql.timestamps.diff(
            "day",
            sql.timestamps.fromMillis("previous_day"),
            sql.timestamps.fromMillis("millis")
          ),
          hour: sql.timestamps.diff(
            "hour",
            sql.timestamps.fromMillis("previous_hour"),
            sql.timestamps.fromMillis("millis")
          ),
          minute: sql.timestamps.diff(
            "minute",
            sql.timestamps.fromMillis("previous_minute"),
            sql.timestamps.fromMillis("millis")
          ),
          second: sql.timestamps.diff(
            "second",
            sql.timestamps.fromMillis("previous_second"),
            sql.timestamps.fromMillis("millis")
          ),
          millisecond: sql.timestamps.diff(
            "millisecond",
            sql.timestamps.fromMillis("previous_millisecond"),
            sql.timestamps.fromMillis("millis")
          )
        });

        const result: any = await execute(query);

        expect(result[0]).deep.equals({
          day: 1,
          hour: 1,
          minute: 1,
          second: 1,
          millisecond: 1
        });
      });

      test("timestamp add", async () => {
        const rows = [
          {
            previous_day: 1604489223000,
            previous_hour: 1604572023000,
            previous_minute: 1604575563000,
            previous_second: 1604575622000
          }
        ];
        const query = sql.from(sql.json(rows)).select({
          day: sql.timestamps.toMillis(
            sql.timestamps.add(sql.timestamps.fromMillis("previous_day"), 1, "day")
          ),
          hour: sql.timestamps.toMillis(
            sql.timestamps.add(sql.timestamps.fromMillis("previous_hour"), 1, "hour")
          ),
          minute: sql.timestamps.toMillis(
            sql.timestamps.add(sql.timestamps.fromMillis("previous_minute"), 1, "minute")
          ),
          second: sql.timestamps.toMillis(
            sql.timestamps.add(sql.timestamps.fromMillis("previous_second"), 1, "second")
          )
        });

        const result: any = await execute(query);

        expect(result[0]).deep.equals({
          day: 1604575623000,
          hour: 1604575623000,
          minute: 1604575623000,
          second: 1604575623000
        });
      });

      test("json", async () => {
        const rows = [
          {
            d: "1",
            m: 1
          },
          {
            d: "2",
            m: 2
          }
        ];
        const selectJson = sql.json(rows);
        const result = await execute(selectJson);
        expect(result).deep.equals(rows);
      });

      test("aggregate", async () => {
        const source = sql.json([
          {
            d: "1",
            m: 1
          },
          {
            d: "2",
            m: 2
          },
          {
            d: "2",
            m: 2
          }
        ]);

        const select = sql
          .aggregate(source)
          .dimensions({
            d: "d"
          })
          .metrics({
            m: sql.sum("m")
          })
          .ordering({
            expression: "m",
            descending: true
          });
        const result = await execute(select);
        expect(result).deep.equals([
          {
            d: "2",
            m: 4
          },
          {
            d: "1",
            m: 1
          }
        ]);
      });

      test("union", async () => {
        const union = sql.union(
          sql.json([
            {
              v: "a"
            }
          ]),
          sql.json([
            {
              v: "b"
            }
          ])
        );
        const result = await execute(union);
        expect(result).deep.equals([
          {
            v: "a"
          },
          {
            v: "b"
          }
        ]);
      });

      test("join", async () => {
        const sourceA = sql.json([
          {
            v1: "a"
          }
        ]);

        const sourceB = sql.json([
          {
            v2: "a"
          }
        ]);

        const join = sql.from(
          sql.join({
            a: {
              select: sourceA,
              type: "base"
            },
            b: {
              select: sourceB,
              type: "left",
              on: ["v1", "v2"]
            }
          })
        );

        const result = await execute(join);
        expect(result).deep.equals([
          {
            v1: "a",
            v2: "a"
          }
        ]);
      });

      test("with", async () => {
        const source = sql.json([
          {
            v: "a"
          }
        ]);

        const select = sql
          .with({
            renamed: source
          })
          .select(sql.from("renamed").select({ v: "v" }));
        const result = await execute(select);
        expect(result).deep.equals([
          {
            v: "a"
          }
        ]);
      });

      test("combination", async () => {
        const source = sql.json([
          {
            d1: "a",
            d2: "c",
            m1: 1,
            m2: 2
          },
          {
            d1: "b",
            d2: "c",
            m1: 2,
            m2: 2
          }
        ]);

        const aggregate = sql
          .with({
            filtered: sql.from(source).where(sql.equals("d2", sql.literal("c"))),
            top_values: sql
              .aggregate("filtered")
              .dimensions({
                d1t: "d1"
              })
              .metrics({
                m1t: sql.sum("m1")
              })
              .ordering({
                expression: "m1t",
                descending: true
              })
              .limit(1)
          })
          .select(
            sql.union(
              sql
                .aggregate(
                  sql.join({
                    top_values: {
                      select: "top_values",
                      type: "base"
                    },
                    vals: {
                      select: "filtered",
                      type: "right",
                      on: ["d1t", "d1"]
                    }
                  })
                )
                .dimensions({
                  d1: "d1t"
                })
                .metrics({
                  m1: sql.sum("m1")
                }),
              sql
                .aggregate("filtered")
                .dimensions({
                  d1: sql.literal("d")
                })
                .metrics({
                  m1: sql.sum("m1")
                })
            )
          );
        const result = await execute(aggregate);
        expect(result).deep.members([
          {
            d1: "b",
            m1: 2
          },
          {
            d1: null,
            m1: 1
          },
          {
            d1: "d",
            m1: 3
          }
        ]);
      });
    });
  }
});
