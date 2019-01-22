import * as protos from "@dataform/protos";

export abstract class Adapter {
  where(query: string, where: string) {
    return `select * from (
        ${query})
        where ${where}`;
  }

  baseTableType(type: string) {
    if (type == "incremental") {
      return "table";
    }
    return type;
  }

  oppositeTableType(type: string) {
    return this.baseTableType(type) == "table" ? "view" : "table";
  }

  insertInto(target: protos.ITarget, columns: string[], query: string) {
    return `
      insert into ${this.resolveTarget(target)}
      (${columns.join(",")})
      select ${columns.join(",")}
      from (${query})`;
  }

  dropIfExists(target: protos.ITarget, type: string) {
    return `drop ${this.baseTableType(type)} if exists ${this.resolveTarget(target)}`;
  }

  abstract resolveTarget(target: protos.ITarget);
}
