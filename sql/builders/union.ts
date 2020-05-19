import {
  build,
  ISelectBuilder,
  ISelectOrBuilder,
  ISelectSchema,
  Select
} from "df/sql/builders/select";

export class UnionBuilder<S extends ISelectSchema> implements ISelectBuilder<S> {
  constructor(private selects: Array<ISelectOrBuilder<S>>) {}

  public build() {
    return Select.create<S>(
      `${this.selects.map(select => build(select).query).join(`
union all
`)}`
    );
  }
}
