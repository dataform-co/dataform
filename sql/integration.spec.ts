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
      dfapi.credentials.read("bigquery", "test_credentials/bigquery.json"),
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
    { name: "snowflake", dbadapter: () => snowflake, sql: new Sql("snowflake") },
    { name: "redshift", dbadapter: () => redshift, sql: new Sql("postgres") }
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
          millis: sql.timestampToMillis(sql.asTimestamp(sql.millisToTimestamp("millis"))),
          truncated_millis: sql.timestampToMillis(
            sql.timestampTruncate(sql.asTimestamp(sql.millisToTimestamp("millis")), "day")
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
