import { IColumnsDescriptor, IRecordDescriptor, IRecordDescriptorProperties } from "df/core/common";
import * as utils from "df/core/utils";
import * as core from "df/protos/core";
import * as execution from "df/protos/execution";

/**
 * @hidden
 */
export class ColumnDescriptors {
  public static mapToColumnProtoArray(
    columns: IColumnsDescriptor,
    reportError: (e: Error) => void
  ): core.ColumnDescriptor[] {
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
  ): core.ColumnDescriptor[] {
    if (typeof description === "string") {
      return [
        core.ColumnDescriptor.create({
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
    const columnDescriptor: core.ColumnDescriptor[] = !!description
      ? [
          core.ColumnDescriptor.create({
            path: currentPath,
            description: description.description,
            displayName: description.displayName,
            dimensionType: ColumnDescriptors.mapDimensionType(description.dimension),
            aggregation: ColumnDescriptors.mapAggregation(description.aggregator),
            expression: description.expression,
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

  public static mapAggregation(aggregation: string) {
    switch (aggregation) {
      case "sum":
        return core.ColumnDescriptor_Aggregation.SUM;
      case "distinct":
        return core.ColumnDescriptor_Aggregation.DISTINCT;
      case "derived":
        return core.ColumnDescriptor_Aggregation.DERIVED;
      case undefined:
        return undefined;
      default:
        throw new Error(`'${aggregation}' is not a valid aggregation option.`);
    }
  }

  public static mapFromAggregation(aggregation: core.ColumnDescriptor_Aggregation) {
    switch (aggregation) {
      case core.ColumnDescriptor_Aggregation.SUM:
        return "sum";
      case core.ColumnDescriptor_Aggregation.DISTINCT:
        return "distinct";
      case core.ColumnDescriptor_Aggregation.DERIVED:
        return "derived";
      case core.ColumnDescriptor_Aggregation.UNKNOWN_AGGREGATION:
        return undefined;
      case undefined:
        return undefined;
      default:
        throw new Error(`Aggregation type not recognized: ${aggregation}`);
    }
  }

  public static mapDimensionType(dimensionType: string) {
    switch (dimensionType) {
      case "category":
        return core.ColumnDescriptor_DimensionType.CATEGORY;
      case "timestamp":
        return core.ColumnDescriptor_DimensionType.TIMESTAMP;
      case "number":
        return core.ColumnDescriptor_DimensionType.NUMBER;
      case undefined:
        return undefined;
      default:
        throw new Error(`'${dimensionType}' is not a valid dimension type.`);
    }
  }

  public static mapFromDimensionType(dimensionType: core.ColumnDescriptor_DimensionType) {
    switch (dimensionType) {
      case core.ColumnDescriptor_DimensionType.CATEGORY:
        return "category";
      case core.ColumnDescriptor_DimensionType.TIMESTAMP:
        return "timestamp";
      case core.ColumnDescriptor_DimensionType.NUMBER:
        return "number";
      case core.ColumnDescriptor_DimensionType.UNKNOWN_DIMENSION:
        return undefined;
      case undefined:
        return undefined;
      default:
        throw new Error(`Dimension type not recognized: ${dimensionType}`);
    }
  }
}
