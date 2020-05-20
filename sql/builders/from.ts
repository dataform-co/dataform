import { IOrdering } from "df/sql/builders";
import {
  build,
  indent,
  ISelectBuilder,
  ISelectOrBuilder,
  ISelectSchema,
  Select
} from "df/sql/builders/select";

export class FromBuilder<S extends ISelectSchema> implements ISelectBuilder<S> {
  private columns: ISelectSchema = {};
  private whereClauses: string[] = [];
  private selectedOrdering: IOrdering;
  private selectedLimit: number;

  constructor(private readonly from: ISelectOrBuilder<any>) {}

  public select<MS extends ISelectSchema>(select: MS): FromBuilder<S & MS> {
    if (select instanceof Array) {
      select.forEach(value => (this.columns[value] = value));
    } else {
      this.columns = { ...this.columns, ...select };
    }
    return this;
  }

  public ordering(ordering: IOrdering) {
    this.selectedOrdering = ordering;
    return this;
  }

  public where(...wheres: string[]) {
    wheres.forEach(where => this.whereClauses.push(where));
    return this;
  }

  public limit(limit: number) {
    this.selectedLimit = limit;
    return this;
  }

  public build() {
    const hasColumns = Object.keys(this.columns).length > 0;
    const whereExpression =
      this.whereClauses.length > 0 ? `\nwhere\n${indent(this.whereClauses.join(" and\n  "))}` : "";
    const orderingExpression = this.selectedOrdering
      ? `\norder by\n  ${this.selectedOrdering.expression} ${
          this.selectedOrdering.descending ? "desc" : "asc"
        }`
      : "";
    const limitExpression = this.selectedLimit ? `\nlimit ${this.selectedLimit}` : "";
    return Select.create<S>(
      `select\n` +
        (hasColumns
          ? `${Object.keys(this.columns)
              .map(alias => `  ${this.columns[alias]} as ${alias}`)
              .join(",\n")}`
          : indent("*")) +
        `\nfrom\n` +
        `${indent(build(this.from))}` +
        `${whereExpression}` +
        `${orderingExpression}` +
        `${limitExpression}`
    );
  }
}
