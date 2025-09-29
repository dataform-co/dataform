import { expect } from "chai";

import {
  formatBytesInHumanReadableFormat,
  formatExecutionSuffix,
  validateIcebergConfigBucketName,
  validateIcebergConfigTableFolderRoot,
  validateIcebergConfigTableFolderSubpath,
} from "df/cli/util";
import { suite, test } from "df/testing";

suite('format execution suffix', () => {
    test('format execution suffix', () => {
        expect(formatExecutionSuffix([], [])).deep.equals('');
        expect(formatExecutionSuffix(["dataform-915a03fe1"], [])).deep.equals(" \n \t jobId: dataform-915a03fe1");
        expect(formatExecutionSuffix([], ["10 MiB"])).deep.equals(" \n \t Bytes billed: 10 MiB");
        expect(formatExecutionSuffix(["dataform-915a03fe1"], ["17 KiB"])).deep.equals(" \n \t jobId: dataform-915a03fe1,\n \t Bytes billed: 17 KiB");
        expect(formatExecutionSuffix(["dataform-915a03fe1", "dataform-915a03fe2"], ["17 KiB", "1 GiB"])).deep.equals(" \n \t jobId: dataform-915a03fe1, dataform-915a03fe2,\n \t Bytes billed: 17 KiB, 1 GiB");
    });
});

suite('format bytes in human readable format', () => {
    test('format bytes in human readable format', () => {
        expect(formatBytesInHumanReadableFormat(-1)).deep.equals('0 B');
        expect(formatBytesInHumanReadableFormat(0)).deep.equals('0 B');
        expect(formatBytesInHumanReadableFormat(1)).deep.equals('1.00 B');
        expect(formatBytesInHumanReadableFormat(500)).deep.equals('500.00 B');
        expect(formatBytesInHumanReadableFormat(1024)).deep.equals('1.00 KiB');
        expect(formatBytesInHumanReadableFormat(1500)).deep.equals('1.46 KiB');
        expect(formatBytesInHumanReadableFormat(1048576)).deep.equals('1.00 MiB');
        expect(formatBytesInHumanReadableFormat(1024 * 1024 * 1.5)).deep.equals('1.50 MiB');
        expect(formatBytesInHumanReadableFormat(1073741824)).deep.equals('1.00 GiB');
        expect(formatBytesInHumanReadableFormat(1099511627776)).deep.equals('1.00 TiB');
        expect(formatBytesInHumanReadableFormat(1125899906842624)).deep.equals('1.00 PiB');
    });
});

suite('Iceberg Config Validation', () => {
  suite('validateIcebergConfigBucketName', () => {
    test('valid bucket names do not throw errors', () => {
      expect(() => validateIcebergConfigBucketName("my-bucket")).to.not.throw();
      expect(() => validateIcebergConfigBucketName("a.b_c-d123")).to.not.throw();
      expect(() => validateIcebergConfigBucketName("bucket123")).to.not.throw();
      expect(() => validateIcebergConfigBucketName("123bucket")).to.not.throw();
      expect(() => validateIcebergConfigBucketName("aaa")).to.not.throw();
    });

    test('invalidates bucket names with uppercase letters', () => {
      expect(() => validateIcebergConfigBucketName("MyBucket")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("mY-bucket")).to.throw("Invalid bucket name.");
    });

    test('invalidates bucket names with invalid characters', () => {
      expect(() => validateIcebergConfigBucketName("bucket!")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("bucket//")).to.throw("Invalid bucket name.");
      expect(() => validateIcebergConfigBucketName("bucket?")).to.throw("Invalid bucket name.");
    });

    test('invalidates bucket names starting or ending with disallowed characters', () => {
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
      expect(() => validateIcebergConfigBucketName("goog-bucket")).to.throw("Bucket names cannot start with 'goog' or contain '--'.");
      expect(() => validateIcebergConfigBucketName("googlebucket")).to.throw("Bucket names cannot start with 'goog' or contain '--'.");
    });

    test('invalidates bucket names containing "--"', () => {
      expect(() => validateIcebergConfigBucketName("buck--et")).to.throw("Bucket names cannot start with 'goog' or contain '--'.");
    });

    test('invalidates bucket names that are under 3 or over 63 characters', () => {
      expect(() => validateIcebergConfigBucketName("aa")).to.throw("Bucket name must be between 3 and 63 characters long.");
      expect(() => validateIcebergConfigBucketName("a".repeat(64))).to.throw("Bucket name must be between 3 and 63 characters long.");
    });
  });

  suite('validateIcebergConfigTableFolderRoot', () => {
    test('valid table folder roots do not throw errors', () => {
      expect(() => validateIcebergConfigTableFolderRoot("my-root")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderRoot("a.b_c-d123")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderRoot("root123")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderRoot("123root")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderRoot("MyRoot")).to.not.throw();
    });

    test('invalidates roots with invalid characters', () => {
      expect(() => validateIcebergConfigTableFolderRoot("root!")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("root/")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderRoot("root\\")).to.throw("Invalid input.");
    });

    test('invalidates roots starting or ending with disallowed characters', () => {
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

  suite('validateIcebergConfigTableFolderSubpath', () => {
    test('valid table folder subpaths do not throw errors', () => {
      expect(() => validateIcebergConfigTableFolderSubpath("my-subpath")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderSubpath("a.b_c-d123")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderSubpath("path/to/data")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderSubpath("path/123")).to.not.throw();
      expect(() => validateIcebergConfigTableFolderSubpath("MySubpath")).to.not.throw();
    });

    test('invalidates subpaths with invalid characters (not allowed)', () => {
      expect(() => validateIcebergConfigTableFolderSubpath("subpath!")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderSubpath("subpath?")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderSubpath("sub\\path")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderSubpath("/subpath")).to.throw("Invalid input.");
      expect(() => validateIcebergConfigTableFolderSubpath("subpath/")).to.throw("Invalid input.");
    });

    test('invalidates subpaths containing "./" or "../"', () => {
      expect(() => validateIcebergConfigTableFolderSubpath("subp./ath")).to.throw("Input cannot contain './' or '../'.");
    });

    test('invalidates subpaths containing ".."', () => {
      expect(() => validateIcebergConfigTableFolderSubpath("subp..ath")).to.throw("Invalid input.");
    });
  });
});
