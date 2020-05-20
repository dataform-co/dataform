import {
  build,
  indent,
  IOrdering,
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

  public where(where: string) {
    this.whereClauses.push(where);
    return this;
  }

  public wheres(wheres: string[]) {
    wheres.forEach(where => this.whereClauses.push(where));
    return this;
  }

  public limit(limit: number) {
    this.selectedLimit = limit;
    return this;
  }

  public build() {
    return Select.create<S>(
      `select\n` +
        (Object.keys(this.selectedDimensions).length + Object.keys(this.selectedMetrics).length ===
        0
          ? "*"
          : "") +
        `${Object.keys(this.selectedDimensions)
          .map(alias => `  ${this.selectedDimensions[alias]} as ${alias}`)
          .join(",\n")}${Object.keys(this.selectedMetrics).length > 0 ? ",\n" : ""}` +
        `${Object.keys(this.selectedMetrics)
          .map(alias => `  ${this.selectedMetrics[alias]} as ${alias}`)
          .join(",\n")}\n` +
        `from\n` +
        `${indent(build(this.from))}` +
        `${
          this.whereClauses.length > 0 ? `\nwhere\n  ${this.whereClauses.join(" and\n  ")}` : ""
        }` +
        `${
          Object.keys(this.selectedDimensions).length > 0
            ? `\ngroup by\n  ${Array.from(new Array(this.selectedDimensions.length).keys())
                .map(i => i + 1)
                .join(", ")}`
            : ""
        }` +
        `${
          this.selectedOrdering
            ? `\norder by\n  ${this.selectedOrdering.expression} ${
                this.selectedOrdering.descending ? "desc" : "asc"
              }`
            : ""
        }` +
        `${this.selectedLimit ? `\nlimit ${this.selectedLimit}` : ""}`
    );
  }
}
