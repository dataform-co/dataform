import { expect } from "chai";
import Long from "long";

import { printExecutionGraph } from "df/cli/console";
import {
  formatBytesInHumanReadableFormat,
  formatDatasetWarehouseState,
  formatExecutionSuffix,
  validateIcebergConfigBucketName,
  validateIcebergConfigTableFolderRoot,
  validateIcebergConfigTableFolderSubpath,
} from "df/cli/util";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

suite("format execution suffix", () => {
  test("format execution suffix", () => {
    expect(formatExecutionSuffix([], [])).deep.equals("");
    expect(formatExecutionSuffix(["dataform-915a03fe1"], [])).deep.equals(
      " \n \t jobId: dataform-915a03fe1",
    );
    expect(formatExecutionSuffix([], ["10 MiB"])).deep.equals(" \n \t Bytes billed: 10 MiB");
    expect(formatExecutionSuffix(["dataform-915a03fe1"], ["17 KiB"])).deep.equals(
      " \n \t jobId: dataform-915a03fe1,\n \t Bytes billed: 17 KiB",
    );
    expect(
      formatExecutionSuffix(["dataform-915a03fe1", "dataform-915a03fe2"], ["17 KiB", "1 GiB"]),
    ).deep.equals(
      " \n \t jobId: dataform-915a03fe1, dataform-915a03fe2,\n \t Bytes billed: 17 KiB, 1 GiB",
    );
  });
});

suite("format bytes in human readable format", () => {
  test("format bytes in human readable format", () => {
    expect(formatBytesInHumanReadableFormat(-1)).deep.equals("0 B");
    expect(formatBytesInHumanReadableFormat(0)).deep.equals("0 B");
    expect(formatBytesInHumanReadableFormat(1)).deep.equals("1.00 B");
    expect(formatBytesInHumanReadableFormat(500)).deep.equals("500.00 B");
    expect(formatBytesInHumanReadableFormat(1024)).deep.equals("1.00 KiB");
    expect(formatBytesInHumanReadableFormat(1500)).deep.equals("1.46 KiB");
    expect(formatBytesInHumanReadableFormat(1048576)).deep.equals("1.00 MiB");
    expect(formatBytesInHumanReadableFormat(1024 * 1024 * 1.5)).deep.equals("1.50 MiB");
    expect(formatBytesInHumanReadableFormat(1073741824)).deep.equals("1.00 GiB");
    expect(formatBytesInHumanReadableFormat(1099511627776)).deep.equals("1.00 TiB");
    expect(formatBytesInHumanReadableFormat(1125899906842624)).deep.equals("1.00 PiB");
  });
});

suite("Iceberg Config Validation", () => {
  suite("validateIcebergConfigBucketName", () => {
    test("valid bucket names do not throw errors", () => {
      expect(() => validateIcebergConfigBucketName("my-bucket")).to.not.throw();
      expect(() => validateIcebergConfigBucketName("a.b_c-d123")).to.not.throw();
      expect(() => validateIcebergConfigBucketName("bucket123")).to.not.throw();
      expect(() => validateIcebergConfigBucketName("123bucket")).to.not.throw();
      expect(() => validateIcebergConfigBucketName("aaa")).to.not.throw();
    });

    test("invalidates bucket names with uppercase letters", () => {
      expect(() => validateIcebergConfigBucketName("MyBucket")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("mY-bucket")).to.throw("Invalid bucket name.");
    });

    test("invalidates bucket names with invalid characters", () => {
      expect(() => validateIcebergConfigBucketName("bucket!")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("bucket//")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("bucket?")).to.throw("Invalid bucket name.");
    });

    test("invalidates bucket names starting or ending with disallowed characters", () => {
      expect(() => validateIcebergConfigBucketName(".bucket")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("bucket.")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("-bucket")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("bucket-")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("_bucket")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("bucket_")).to.throw("Invalid bucket name.");
    });

    test('invalidates bucket names containing ".."', () => {
      expect(() => validateIcebergConfigBucketName("buck..et")).to.throw("Invalid bucket name.");
    });

    test('invalidates bucket names starting with "goog"', () => {
      expect(() => validateIcebergConfigBucketName("goog-bucket")).to.throw(
        "Bucket name cannot start with 'goog' or contain '--'.",
      );
      expect(() => validateIcebergConfigBucketName("googlebucket")).to.throw(
        "Bucket name cannot start with 'goog' or contain '--'.",
      );
    });

    test('invalidates bucket names containing "--"', () => {
      expect(() => validateIcebergConfigBucketName("buck--et")).to.throw(
        "Bucket name cannot start with 'goog' or contain '--'.",
      );
    });

    test('invalidates bucket names containing "google" or "g00gle"', () => {
      expect(() => validateIcebergConfigBucketName("bucket-google")).to.throw(
        "Bucket name cannot contain 'google' or close misspellings such as 'g00gle'.",
      );
      expect(() => validateIcebergConfigBucketName("g00glebucket")).to.throw(
        "Bucket name cannot contain 'google' or close misspellings such as 'g00gle'.",
      );
    });

    test("invalidates bucket names that are under 3 or over 63 characters", () => {
      expect(() => validateIcebergConfigBucketName("aa")).to.throw(
        "Bucket name must be between 3 and 63 characters long.",
      );
      expect(() => validateIcebergConfigBucketName("a".repeat(64))).to.throw(
        "Bucket name must be between 3 and 63 characters long.",
      );
    });
  });

  suite("validateIcebergConfigTableFolderRoot", () => {
    test("valid table folder roots do not throw errors", () => {
      expect(() => validateIcebergConfigTableFolderRoot("my-root")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderRoot("a.b_c-d123")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderRoot("root123")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderRoot("123root")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderRoot("MyRoot")).to.not.throw();
    });

    test("invalidates roots with invalid characters", () => {
      expect(() => validateIcebergConfigTableFolderRoot("root!")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("root/")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("root\\")).to.throw("Invalid input.");
    });

    test("invalidates roots starting or ending with disallowed characters", () => {
      expect(() => validateIcebergConfigTableFolderRoot(".root")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("root.")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("-root")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("root-")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("_root")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("root_")).to.throw("Invalid input.");
    });

    test('invalidates roots containing ".."', () => {
      expect(() => validateIcebergConfigTableFolderRoot("ro..ot")).to.throw("Invalid input.");
    });
  });

  suite("validateIcebergConfigTableFolderSubpath", () => {
    test("valid table folder subpaths do not throw errors", () => {
      expect(() => validateIcebergConfigTableFolderSubpath("my-subpath")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderSubpath("a.b_c-d123")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderSubpath("path/to/data")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderSubpath("path/123")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderSubpath("MySubpath")).to.not.throw();
    });

    test("invalidates subpaths with invalid characters (not allowed)", () => {
      expect(() => validateIcebergConfigTableFolderSubpath("subpath!")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderSubpath("subpath?")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderSubpath("sub\\path")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderSubpath("/subpath")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderSubpath("subpath/")).to.throw("Invalid input.");
    });

    test('invalidates subpaths containing "./" or "../"', () => {
      expect(() => validateIcebergConfigTableFolderSubpath("subp./ath")).to.throw(
        "Input cannot contain './' or '../'.",
      );
    });

    test('invalidates subpaths containing ".."', () => {
      expect(() => validateIcebergConfigTableFolderSubpath("subp..ath")).to.throw("Invalid input.");
    });
  });
});

suite("formatDatasetWarehouseState", () => {
  const timestampMillis = Long.fromNumber(Date.UTC(2026, 10, 4, 14, 8, 23));

  test("returns does not exist when metadata is missing", () => {
    expect(formatDatasetWarehouseState("table", undefined)).equals("(does not exist)");
  });

  test("formats last modified date when types match", () => {
    expect(
      formatDatasetWarehouseState("table", {
        type: dataform.TableMetadata.Type.TABLE,
        lastUpdatedMillis: timestampMillis,
      }),
    ).equals("(last modified: 2026-11-04 14:08:23 UTC)");

    expect(
      formatDatasetWarehouseState("incremental", {
        type: dataform.TableMetadata.Type.TABLE,
        lastUpdatedMillis: timestampMillis,
      }),
    ).equals("(last modified: 2026-11-04 14:08:23 UTC)");

    expect(
      formatDatasetWarehouseState("view", {
        type: dataform.TableMetadata.Type.VIEW,
        lastUpdatedMillis: timestampMillis,
      }),
    ).equals("(last modified: 2026-11-04 14:08:23 UTC)");
  });

  test("includes old and new types when warehouse type differs from action type", () => {
    expect(
      formatDatasetWarehouseState("view", {
        type: dataform.TableMetadata.Type.TABLE,
        lastUpdatedMillis: timestampMillis,
      }),
    ).equals("(type: table -> view, last modified: 2026-11-04 14:08:23 UTC)");

    expect(
      formatDatasetWarehouseState("table", {
        type: dataform.TableMetadata.Type.VIEW,
        lastUpdatedMillis: timestampMillis,
      }),
    ).equals("(type: view -> table, last modified: 2026-11-04 14:08:23 UTC)");

    expect(
      formatDatasetWarehouseState("incremental", {
        type: dataform.TableMetadata.Type.VIEW,
        lastUpdatedMillis: timestampMillis,
      }),
    ).equals("(type: view -> table, last modified: 2026-11-04 14:08:23 UTC)");
  });

  test("printExecutionGraph outputs plain text summary with warehouse state", () => {
    const executionGraph = dataform.ExecutionGraph.create({
      actions: [
        {
          type: "table",
          tableType: "table",
          target: { database: "proj", schema: "ds", name: "existing_table" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "table",
          tableType: "view",
          target: { database: "proj", schema: "ds", name: "type_changed_to_view" },
          tasks: [{ type: "statement", statement: "SELECT 1" }],
        },
        {
          type: "table",
          tableType: "incremental",
          target: { database: "proj", schema: "ds", name: "missing_table" },
          tasks: [],
        },
        {
          type: "assertion",
          target: { database: "proj", schema: "ds", name: "my_assertion" },
          tasks: [{ type: "assertion", statement: "SELECT 1" }],
        },
      ],
      warehouseState: {
        tables: [
          {
            target: { database: "proj", schema: "ds", name: "existing_table" },
            type: dataform.TableMetadata.Type.TABLE,
            lastUpdatedMillis: timestampMillis,
          },
          {
            target: { database: "proj", schema: "ds", name: "type_changed_to_view" },
            type: dataform.TableMetadata.Type.TABLE,
            lastUpdatedMillis: timestampMillis,
          },
        ],
      },
    });

    const chunks: string[] = [];
    const originalWrite = process.stdout.write.bind(process.stdout);
    (process.stdout as any).write = (chunk: string) => {
      chunks.push(String(chunk));
      return true;
    };
    try {
      printExecutionGraph(executionGraph, false);
    } finally {
      process.stdout.write = originalWrite;
    }

    const plainOutput = chunks.join("").replace(/\x1b\[[0-9;]*m/g, "");
    expect(plainOutput).equals(
      [
        "3 dataset(s):",
        "  ds.existing_table [table] (last modified: 2026-11-04 14:08:23 UTC)",
        "  ds.type_changed_to_view [view] (type: table -> view, last modified: 2026-11-04 14:08:23 UTC)",
        "  ds.missing_table [incremental] [disabled] (does not exist)",
        "1 assertion(s):",
        "  ds.my_assertion",
        "",
      ].join("\n"),
    );
  });
});
