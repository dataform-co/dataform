import * as objectHash from "object-hash";
import { dataform } from "@dataform/protos";

export const hashExecutionAction = (action: dataform.IExecutionAction) => {
  return objectHash.sha1(action);
};
