import {
  build,
  ISelectBuilder,
  ISelectOrBuilder,
  ISelectSchema,
  Select,
  indent
} from "df/sql/builders/select";

export interface IJoin<S extends ISelectSchema> {
  select: ISelectOrBuilder<S>;
  using?: string;
  type?: "base" | "left" | "right" | "inner" | "outer";
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
    return Select.create<{ [K in keyof J]: J[K] extends IJoin<infer JS> ? JS : {} }>(`(
select
  ${Object.keys(this.joins).join(", ")}
from 
  ${indent(build(baseSelect.select))} ${baseAlias}
${joinAliases
  .map(
    alias => `${this.joins[alias].type} join
${indent(build(this.joins[alias].select))} ${alias} on (${baseAlias}.${
      this.joins[alias].using
    } = ${alias}.${this.joins[alias].using})`
  )
  .join(`\n`)}
)`);
  }
}
