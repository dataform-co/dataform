import * as objectHash from "object-hash";
import { dataform } from "@dataform/protos";

export const hashTableDefinition = (action: dataform.IExecutionAction) => {
  return objectHash.sha1(action);
};
