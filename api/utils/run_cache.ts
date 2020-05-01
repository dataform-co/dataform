import { dataform } from "df/protos";
import * as objectHash from "object-hash";

export interface IMetadataRow {
  target_name: string;
  metadata_json: string;
  metadata_proto: string;
}

export const hashExecutionAction = (action: dataform.IExecutionAction) => {
  return objectHash.sha1(action);
};

export const decodePersistedTableMetadata = (protoString: string) => {
  const encodedProto = Buffer.from(protoString, "base64");
  return dataform.PersistedTableMetadata.decode(encodedProto);
};

export const encodePersistedTableMetadata = (table: dataform.PersistedTableMetadata) => {
  const encodedProtoBuffer = Buffer.from(dataform.PersistedTableMetadata.encode(table).finish());
  return encodedProtoBuffer.toString("base64");
};

export const buildQuery = (targetName: string, table: dataform.PersistedTableMetadata) => {
  const encodedProtoString = encodePersistedTableMetadata(table);
  const query = `SELECT 
          '${targetName}' AS target_name,
          '${JSON.stringify(table.toJSON())}' AS metadata_json,
          '${encodedProtoString}' as metadata_proto`;
  return query;
};
