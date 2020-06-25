import * as objectHash from "object-hash";

import { dataform } from "df/protos/ts";

export interface IMetadataRow {
  target: string;
  metadata_proto: string;
}

export const hashExecutionAction = (action: dataform.IExecutionAction) => {
  return objectHash.sha1(action);
};

export const decodePersistedTableMetadata = (protoString: string) => {
  const encodedProto = Buffer.from(protoString, "base64");
  return dataform.PersistedTableMetadata.decode(encodedProto);
};

export const encodePersistedTableMetadata = (table: dataform.IPersistedTableMetadata) => {
  return Buffer.from(dataform.PersistedTableMetadata.encode(table).finish()).toString("base64");
};
