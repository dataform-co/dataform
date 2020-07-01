import { flatten } from "df/common/arrays/arrays";
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
    return flatten(
      Object.keys(columns).map(column =>
        ColumnDescriptors.mapColumnDescriptionToProto([column], columns[column], reportError)
      )
    );
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
            dimensionType: ColumnDescriptors.mapDimensionType(description.dimension),
            aggregation: ColumnDescriptors.mapAggregation(description.aggregator),
            expression: description.expression
          })
        ]
      : [];
    const nestedColumns = description.columns ? Object.keys(description.columns) : [];
    return columnDescriptor.concat(
      flatten(
        nestedColumns.map(nestedColumn =>
          ColumnDescriptors.mapColumnDescriptionToProto(
            currentPath.concat([nestedColumn]),
            description.columns[nestedColumn],
            reportError
          )
        )
      )
    );
  }

  public static mapAggregation(aggregation: string) {
    switch (aggregation) {
      case "sum":
        return dataform.ColumnDescriptor.Aggregation.SUM;
      case "distinct":
        return dataform.ColumnDescriptor.Aggregation.DISTINCT;
      case "derived":
        return dataform.ColumnDescriptor.Aggregation.DERIVED;
      case undefined:
        return undefined;
      default:
        throw new Error(`'${aggregation}' is not a valid aggregation option.`);
    }
  }

  public static mapFromAggregation(aggregation: dataform.ColumnDescriptor.Aggregation) {
    switch (aggregation) {
      case dataform.ColumnDescriptor.Aggregation.SUM:
        return "sum";
      case dataform.ColumnDescriptor.Aggregation.DISTINCT:
        return "distinct";
      case dataform.ColumnDescriptor.Aggregation.DERIVED:
        return "derived";
      case dataform.ColumnDescriptor.Aggregation.UNKNOWN_AGGREGATION:
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
        return dataform.ColumnDescriptor.DimensionType.CATEGORY;
      case "timestamp":
        return dataform.ColumnDescriptor.DimensionType.TIMESTAMP;
      case "numerical":
        return dataform.ColumnDescriptor.DimensionType.NUMERICAL;
      case undefined:
        return undefined;
      default:
        throw new Error(`'${dimensionType}' is not a valid dimension type.`);
    }
  }

  public static mapFromDimensionType(dimensionType: dataform.ColumnDescriptor.DimensionType) {
    switch (dimensionType) {
      case dataform.ColumnDescriptor.DimensionType.CATEGORY:
        return "category";
      case dataform.ColumnDescriptor.DimensionType.TIMESTAMP:
        return "timestamp";
      case dataform.ColumnDescriptor.DimensionType.NUMERICAL:
        return "numerical";
      case dataform.ColumnDescriptor.DimensionType.UNKNOWN_DIMENSION:
        return undefined;
      case undefined:
        return undefined;
      default:
        throw new Error(`Dimension type not recognized: ${dimensionType}`);
    }
  }
}
