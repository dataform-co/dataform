import { IOrdering } from "df/sql/builders";
import {
  build,
  indent,
  ISelectBuilder,
  ISelectOrBuilder,
  ISelectSchema,
  Select
} from "df/sql/builders/select";

export class AggregateBuilder<S extends ISelectSchema> implements ISelectBuilder<S> {
  private selectedDimensions: ISelectSchema = {};
  private selectedMetrics: ISelectSchema = {};
  private whereClauses: string[] = [];
  private selectedOrdering: IOrdering;
  private selectedLimit: number;

  constructor(private readonly from: ISelectOrBuilder<any>) {}

  public metrics<MS extends ISelectSchema>(select: MS): AggregateBuilder<S & MS> {
    this.selectedMetrics = { ...this.selectedMetrics, ...select };
    return this;
  }

  public dimensions<DS extends ISelectSchema>(select: DS): AggregateBuilder<S & DS> {
    this.selectedDimensions = { ...this.selectedDimensions, ...select };
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
    const hasDimensions = Object.keys(this.selectedDimensions).length > 0;
    const hasMetrics = Object.keys(this.selectedMetrics).length > 0;
    const whereExpression =
      this.whereClauses.length > 0 ? `\nwhere\n${indent(this.whereClauses.join(" and\n"))}` : "";
    const orderingExpression = this.selectedOrdering
      ? `\norder by\n  ${this.selectedOrdering.expression} ${
          this.selectedOrdering.descending ? "desc" : "asc"
        }`
      : "";
    const limitExpression = this.selectedLimit ? `\nlimit ${this.selectedLimit}` : "";
    return Select.create<S>(
      `select\n` +
        (!hasDimensions && !hasMetrics ? indent("*") : "") +
        `${Object.keys(this.selectedDimensions)
          .map(alias => indent(`${this.selectedDimensions[alias]} as ${alias}`))
          .join(",\n")}${hasDimensions ? ",\n" : ""}` +
        `${Object.keys(this.selectedMetrics)
          .map(alias => indent(`${this.selectedMetrics[alias]} as ${alias}`))
          .join(",\n")}\n` +
        `from\n` +
        `${indent(build(this.from))}` +
        `${whereExpression}` +
        `${
          hasDimensions
            ? `\ngroup by ${Array.from(
                new Array(Object.keys(this.selectedDimensions).length).keys()
              )
                .map(i => i + 1)
                .join(", ")}`
            : ""
        }` +
        `${orderingExpression}` +
        `${limitExpression}`
    );
  }
}
