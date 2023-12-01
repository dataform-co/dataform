import { Assertion, IAssertionConfig } from "df/core/actions/assertion";
import { Declaration, IDeclarationConfig } from "df/core/actions/declaration";
import { IOperationConfig, Operation } from "df/core/actions/operation";
import { ITableConfig, Table, TableType } from "df/core/actions/table";
import { ITestConfig, Test } from "df/core/actions/test";
import { dataform } from "df/protos/ts";

export type Action = Table | Operation | Assertion | Declaration;

/**
 * @deprecated
 * Configs are soon to be replaced with pure protobuf representations.
 */
export type SqlxConfig = (
  | (ITableConfig & { type: TableType })
  | (IAssertionConfig & { type: "assertion" })
  | (IOperationConfig & { type: "operations" })
  | (IDeclarationConfig & { type: "declaration" })
  | (ITestConfig & { type: "test" })
) & { name: string };

export interface IActionBuilder<T> {
  /**
   * @deprecated
   * Configs are soon to be replaced with pure protobuf representations.
   */
  config: (config: any) => IActionBuilder<T>;

  /** Retrieves the filename from the config. */
  getFileName: () => string;

  /** Retrieves the resolved target from the proto. */
  getTarget: () => dataform.Target;

  /** Creates the final protobuf representation. */
  compile: () => T;
}
