import { expect } from "chai";
import * as dfapi from "df/api";
import * as dbadapters from "df/api/dbadapters";
import { BigQueryDbAdapter } from "df/api/dbadapters/bigquery";
import { Sql } from "df/sql";
import { build, ISelectOrBuilder, ISelectSchema } from "df/sql/builders/select";
import { suite, test } from "df/testing";

const sql = new Sql();

suite("sql", { parallel: true }, ({ before, after }) => {
  let dbadapter: BigQueryDbAdapter;

  before("create adapter", async () => {
    dbadapter = (await dbadapters.create(
      dfapi.credentials.read("bigquery", "test_credentials/bigquery.json"),
      "bigquery"
    )) as BigQueryDbAdapter;
  });

  after("close adapter", () => dbadapter.close());

  const execute = async <S extends ISelectSchema>(select: ISelectOrBuilder<S>) => {
    const query = build(select).query;
    console.log(query);
    try {
      return (await dbadapter.execute(query)).rows as S[];
    } catch (e) {
      throw new Error(`Error during query: ${e}\n${query}`);
    }
  };

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
    const source = sql.json([
      {
        v: "a"
      }
    ]);

    const join = sql.join({
      a: {
        select: source,
        type: "base"
      },
      b: {
        select: source,
        type: "left",
        using: "v"
      }
    });

    const result = await execute(join);
    expect(result).deep.equals([
      {
        a: { v: "a" },
        b: { v: "a" }
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

  test("cube", async () => {
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
        top: sql
          .aggregate("filtered")
          .dimensions({
            d1: "d1"
          })
          .metrics({
            m1: sql.sum("m1")
          })
          .ordering({
            expression: "m1",
            descending: true
          })
          .limit(1)
      })
      .select(
        sql.union(
          sql
            .aggregate("filtered")
            .dimensions({
              d1: sql.conditional({
                condition: `d1 in (${sql
                  .from("top")
                  .select({ d1: "d1" })
                  .build()})`,
                then: "d1",
                else: sql.literal("<other>")
              })
            })
            .metrics({
              m1: sql.sum("m1")
            }),
          sql
            .aggregate("filtered")
            .dimensions({
              d1: sql.literal("<total>")
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
        d1: "<other>",
        m1: 1
      },
      {
        d1: "<total>",
        m1: 3
      }
    ]);
  });
});
