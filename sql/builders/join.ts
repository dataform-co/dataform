import {
  build,
  indent,
  ISelectBuilder,
  ISelectOrBuilder,
  ISelectSchema,
  Select
} from "df/sql/builders/select";

export interface IJoin<S extends ISelectSchema> {
  select: ISelectOrBuilder<S>;
  on?: [string, string];
  type?: "base" | "left" | "right" | "inner" | "outer" | "cross" | "full outer";
}

export interface IJoins {
  [alias: string]: IJoin<any>;
}

export class JoinBuilder<J extends IJoins>
  implements ISelectBuilder<{ [K in keyof J]: J[K] extends IJoin<infer JS> ? JS : {} }> {
  constructor(private readonly joins: J) {}

  public build() {
    const baseAlias = Object.keys(this.joins).find(key => !!key || key === "base");
    const baseSelect = this.joins[baseAlias];
    const joinAliases = Object.keys(this.joins).filter(alias => alias !== baseAlias);
    return Select.create<{ [K in keyof J]: J[K] extends IJoin<infer JS> ? JS : {} }>(`${indent(
      build(baseSelect.select)
    )} ${baseAlias}
${joinAliases
  .map(
    alias => `${this.joins[alias].type} join
${build(this.joins[alias].select)} ${alias} ${
      !!this.joins[alias].on
        ? `on (${baseAlias}.${this.joins[alias].on[0]} = ${alias}.${this.joins[alias].on[1]})`
        : ""
    }`
  )
  .join(`\n`)}`);
  }
}
