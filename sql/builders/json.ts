import { Sql } from "df/sql";
import { ISelectBuilder, ISelectSchema, Select, indent } from "df/sql/builders/select";

export class JSONBuilder<S extends ISelectSchema> implements ISelectBuilder<S> {
  constructor(private sql: Sql, private data: S[]) {}

  public build() {
    return Select.create<S>(`(
${indent(
  this.data
    .map(
      row =>
        `select ${Object.keys(row)
          .map(alias => `${this.sql.literal(row[alias] as string | number)} as ${alias}`)
          .join(", ")}`
    )
    .join(`\nunion all\n`)
)}
)`);
  }
}
