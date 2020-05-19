export interface ISelectSchema {
  [alias: string]: string | number | ISelectSchema;
}

export class Select<T extends ISelectSchema> {
  public static create<T extends ISelectSchema>(query: string) {
    return new Select<T>(query);
  }
  constructor(public readonly query: string) {}

  public toString() {
    return this.query;
  }
}

export interface ISelectBuilder<T extends ISelectSchema> {
  build(): Select<T>;
}

export type ISelectOrBuilder<T extends ISelectSchema> = ISelectBuilder<T> | Select<T> | string;

export function build(selectOrBuilder: ISelectOrBuilder<any>) {
  if (typeof selectOrBuilder === "string") {
    return Select.create(selectOrBuilder);
  }

  if (selectOrBuilder instanceof Select) {
    return selectOrBuilder;
  }

  return selectOrBuilder.build();
}

export interface IOrdering {
  expression: string;
  descending?: boolean;
}

export function indent(value: string | Select<any>) {
  return String(value)
    .split("\n")
    .map(row => `  ${row}`)
    .join("\n");
}
