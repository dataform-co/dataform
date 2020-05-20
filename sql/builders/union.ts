import {
  build,
  indent,
  ISelectBuilder,
  ISelectOrBuilder,
  ISelectSchema,
  Select
} from "df/sql/builders/select";

export class UnionBuilder<S extends ISelectSchema> implements ISelectBuilder<S> {
  constructor(private selects: Array<ISelectOrBuilder<S>>) {}

  public build() {
    // TODO: Check all columns have the same values for a union.
    return Select.create<S>(
      `(
${indent(
  this.selects.map(select => build(select).query).join(`
union all
`)
)}
)`
    );
  }
}
