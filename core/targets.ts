import { JSONObjectStringifier } from "df/common/strings/stringifier";
import { dataform } from "df/protos/ts";

export class Targets {
  public static STRINGIFIER = JSONObjectStringifier.create();

  public static equal(a: dataform.ITarget, b: dataform.ITarget) {
    return a.database === b.database && a.schema === b.schema && a.name === b.name;
  }

  public static getId(
    targetOrTargetable: dataform.ITarget | { target?: dataform.ITarget }
  ): string {
    const target: dataform.ITarget =
      "target" in targetOrTargetable
        ? targetOrTargetable.target
        : (targetOrTargetable as dataform.ITarget);

    const nameParts = [target.name, target.schema];
    if (!!target.database) {
      nameParts.push(target.database);
    }
    return nameParts.reverse().join(".");
  }
}
