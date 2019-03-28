import { dataform } from "@dataform/protos";

export abstract class Adapter {
  public where(query: string, where: string) {
    return `select * from (
        ${query})
        where ${where}`;
  }

  public baseTableType(type: string) {
    if (type == "incremental") {
      return "table";
    }
    return type;
  }

  public oppositeTableType(type: string) {
    return this.baseTableType(type) == "table" ? "view" : "table";
  }

  public insertInto(target: dataform.ITarget, columns: string[], query: string) {
    return `
      insert into ${this.resolveTarget(target)}
      (${columns.join(",")})
      select ${columns.join(",")}
      from (${query})`;
  }

  public dropIfExists(target: dataform.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)} ${
      this.baseTableType(type) == "table" ? "cascade" : ""
    }`;
  }

  public abstract resolveTarget(target: dataform.ITarget): string;
}
