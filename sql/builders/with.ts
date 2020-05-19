import { build, ISelectOrBuilder, ISelectSchema, Select, indent } from "df/sql/builders/select";

export interface IWiths {
  [alias: string]: ISelectOrBuilder<any>;
}

export class WithBuilder {
  constructor(private readonly withs: IWiths) {}

  public select<S extends ISelectSchema>(select: ISelectOrBuilder<S>) {
    return Select.create<S>(
      (Object.keys(this.withs).length > 0
        ? `with
${Object.keys(this.withs)
  .map(
    key => `${key} as (
${indent(build(this.withs[key]))}
)`
  )
  .join(",\n")}
`
        : "") + build(select).query
    );
  }
}
