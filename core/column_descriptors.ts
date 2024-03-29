import { IColumnsDescriptor, IRecordDescriptor, IRecordDescriptorProperties } from "df/core/common";
import * as utils from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * @hidden
 */
export class ColumnDescriptors {
  public static mapToColumnProtoArray(
    columns: IColumnsDescriptor,
    reportError: (e: Error) => void
  ): dataform.IColumnDescriptor[] {
    return Object.keys(columns)
      .map(column =>
        ColumnDescriptors.mapColumnDescriptionToProto([column], columns[column], reportError)
      )
      .flat();
  }

  public static mapColumnDescriptionToProto(
    currentPath: string[],
    description: string | IRecordDescriptor,
    reportError: (e: Error) => void
  ): dataform.IColumnDescriptor[] {
    if (typeof description === "string") {
      return [
        dataform.ColumnDescriptor.create({
          description,
          path: currentPath
        })
      ];
    }
    utils.checkExcessProperties(
      reportError,
      description,
      IRecordDescriptorProperties(),
      `${currentPath.join(".")} column descriptor`
    );
    const columnDescriptor: dataform.IColumnDescriptor[] = !!description
      ? [
          dataform.ColumnDescriptor.create({
            path: currentPath,
            description: description.description,
            displayName: description.displayName,
            tags: typeof description.tags === "string" ? [description.tags] : description.tags,
            bigqueryPolicyTags:
              typeof description.bigqueryPolicyTags === "string"
                ? [description.bigqueryPolicyTags]
                : description.bigqueryPolicyTags
          })
        ]
      : [];
    const nestedColumns = description.columns ? Object.keys(description.columns) : [];
    return columnDescriptor.concat(
      nestedColumns
        .map(nestedColumn =>
          ColumnDescriptors.mapColumnDescriptionToProto(
            currentPath.concat([nestedColumn]),
            description.columns[nestedColumn],
            reportError
          )
        )
        .flat()
    );
  }
}
