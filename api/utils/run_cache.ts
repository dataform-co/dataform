import * as objectHash from "object-hash";

import { decode, encode } from "df/common/protos";
import { dataform } from "df/protos/ts";

export interface IMetadataRow {
  target: string;
  metadata_proto: string;
}

export function hashExecutionAction(action: dataform.IExecutionAction) {
  return objectHash.sha1(action);
}

export function toRowKey(target: dataform.ITarget) {
  const parts = [target.name, target.schema];
  if (target.database) {
    parts.push(target.database);
  }
  return parts.reverse().join(".");
}

export function decodePersistedTableMetadata(protoString: string) {
  return decode(dataform.PersistedTableMetadata, protoString);
}

export function encodePersistedTableMetadata(table: dataform.IPersistedTableMetadata) {
  return encode(dataform.PersistedTableMetadata, table);
}
