import * as objectHash from "object-hash";
import { dataform } from "@dataform/protos";

export const hashTableDefinition = (table: dataform.ITable) => {
  const definition = {
    query: table.query,
    incrementalQuery: table.incrementalQuery,
    preOps: table.preOps,
    postOps: table.postOps,
    incrementalPreOps: table.incrementalPreOps,
    incrementalPostOps: table.incrementalPostOps
  };
  return objectHash.sha1(definition);
};
