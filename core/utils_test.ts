import {expect} from "chai";
import {
  getConnectionForIcebergTable,
  getEffectiveBucketName,
  getEffectiveTableFolderRoot,
  getEffectiveTableFolderSubpath,
  getFileFormatValueForIcebergTable,
  getStorageUriForIcebergTable,
  validateConnectionFormat,
  validateStorageUriFormat,
} from './utils';

import {dataform} from "df/protos/ts";
import {suite, test} from 'df/testing';

/**
 * Executes a function and returns the Error if one is thrown, otherwise returns undefined.
 * Assumes all caught values are Error instances.
 */
function getThrownError(fn: () => void): Error | undefined {
  try {
    fn();
    return undefined;
  } catch (e) {
    // All thrown values should be instances of Error.
    if (e instanceof Error) {
      return e;
    }
    // Fallback for unexpected non-Error throws.
    return new Error(`Caught non-Error: ${e}`);
  }
}

/**
 * Asserts that the provided function does NOT throw any error.
 */
function assertNoThrow(testFn: () => void) {
  const error = getThrownError(testFn);
  if (error) {
    throw new Error(`Expected no error to be thrown, but caught: "${error.message}"`);
  }
}

/**
 * Asserts that the provided function throws an error with a specific message.
 */
function assertThrowsWithMessage(testFn: () => void, expectedMessage: string) {
  const error = getThrownError(testFn);
  if (!error) {
    throw new Error(`Expected an error to be thrown with message "${expectedMessage}", but no error was caught.`);
  }
  if (error.message !== expectedMessage) {
    throw new Error(`Expected error message "${expectedMessage}", but got "${error.message}"`);
  }
}

suite('Dataform Utility Validations', () => {
  const CONNECTION_ERROR_MSG = 'The connection must be in the format `{project}.{location}.{connection_id}` or `projects/{project}/locations/{location}/connections/{connection_id}`, or be set to `DEFAULT`.';
  const STORAGE_ERROR_MSG = 'The storage URI must be in the format `gs://{bucket_name}/{path_to_data}`.';

  suite('validateConnectionFormat', () => {
    test('does not throw for the "DEFAULT" connection', () => {
      assertNoThrow(() => validateConnectionFormat('DEFAULT'));
    });

    test('does not throw for a valid dot-separated connection format', () => {
      assertNoThrow(() => validateConnectionFormat('my-project.us-central1.my-connection'));
      assertNoThrow(() => validateConnectionFormat('gcp-proj-123.europe-west4.dataform-conn_id'));
    });

    test('does not throw for a valid resource-formatted connection format', () => {
      assertNoThrow(() => validateConnectionFormat('projects/my-project/locations/us-central1/connections/my-connection'));
      assertNoThrow(() => validateConnectionFormat('projects/gcp-proj-123/locations/europe-west4/connections/dataform-conn_id'));
    });

    test('throws for an empty connection string', () => {
      assertThrowsWithMessage(() => validateConnectionFormat(''), CONNECTION_ERROR_MSG);
    });

    test('throws for an invalid dot-separated format (too few parts)', () => {
      assertThrowsWithMessage(() => validateConnectionFormat('my-project.my-connection'), CONNECTION_ERROR_MSG);
    });

    test('throws for an invalid dot-separated format (too many parts)', () => {
      assertThrowsWithMessage(() => validateConnectionFormat('a.b.c.d'), CONNECTION_ERROR_MSG);
    });

    test('throws for an invalid resource format (missing segment value)', () => {
      assertThrowsWithMessage(() => validateConnectionFormat('projects/my-project/locations//connections/my-connection'), CONNECTION_ERROR_MSG);
    });

    test('throws for an invalid resource format (incorrect separators)', () => {
      assertThrowsWithMessage(() => validateConnectionFormat('projects/my-project/locations-us-central1/connections/my-connection'), CONNECTION_ERROR_MSG);
    });

    test('throws for a completely unmatching string', () => {
      assertThrowsWithMessage(() => validateConnectionFormat('some-other-format'), CONNECTION_ERROR_MSG);
    });
  });

  suite('validateStorageUriFormat', () => {
    test('does not throw for a valid gs:// URI', () => {
      assertNoThrow(() => validateStorageUriFormat('gs://my-bucket/path/to/data'));
    });

    test('does not throw for a valid gs:// URI with just a bucket and root path', () => {
      assertNoThrow(() => validateStorageUriFormat('gs://another-bucket/file.csv'));
    });

    test('throws for a URI missing the gs:// prefix', () => {
      assertThrowsWithMessage(() => validateStorageUriFormat('my-bucket/path/to/data'), STORAGE_ERROR_MSG);
    });

    test('throws for an incorrect scheme prefix', () => {
      assertThrowsWithMessage(() => validateStorageUriFormat('s3://my-bucket/path'), STORAGE_ERROR_MSG);
    });

    test('throws for a URI with no bucket name', () => {
      assertThrowsWithMessage(() => validateStorageUriFormat('gs:///path/to/data'), STORAGE_ERROR_MSG);
    });

    test('throws for a URI with no path after the bucket', () => {
      assertThrowsWithMessage(() => validateStorageUriFormat('gs://my-bucket/'), STORAGE_ERROR_MSG);
    });

    test('throws for an empty storage URI string', () => {
      assertThrowsWithMessage(() => validateStorageUriFormat(''), STORAGE_ERROR_MSG);
    });
  });

  suite('getEffectiveBucketName', () => {
    test('returns configBucketName if provided', () => {
      expect(getEffectiveBucketName('ws-default', 'config-bucket')).to.equal('config-bucket');
    });

    test('returns defaultBucketName when configBucketName is undefined', () => {
      expect(getEffectiveBucketName('ws-default', undefined)).to.equal('ws-default');
    });

    test('returns defaultBucketName when configBucketName is an empty string', () => {
      expect(getEffectiveBucketName('ws-default', '')).to.equal('ws-default');
    });

    test('throws error if both configBucketName and defaultBucketName are undefined', () => {
      assertThrowsWithMessage(
        () => getEffectiveBucketName(undefined, undefined),
        'When defining an Iceberg table, bucket name must be defined in workspace_settings.yaml or the config block.'
      );
    });

    test('throws error if both configBucketName and defaultBucketName are empty strings', () => {
      assertThrowsWithMessage(
        () => getEffectiveBucketName('', ''),
        'When defining an Iceberg table, bucket name must be defined in workspace_settings.yaml or the config block.'
      );
    });
  });

  suite('getEffectiveTableFolderRoot', () => {
    test('returns configTableFolderRoot if provided', () => {
      expect(getEffectiveTableFolderRoot('ws-root', 'config-root')).to.equal('config-root');
    });

    test('returns defaultTableFolderRoot when configTableFolderRoot is undefined', () => {
      expect(getEffectiveTableFolderRoot('ws-root', undefined)).to.equal('ws-root');
    });

    test('returns defaultTableFolderRoot when configTableFolderRoot is an empty string', () => {
      expect(getEffectiveTableFolderRoot('ws-root', '')).to.equal('ws-root');
    });

    test('returns "_dataform" when both config and default are undefined', () => {
      expect(getEffectiveTableFolderRoot(undefined, undefined)).to.equal('_dataform');
    });

     test('returns "_dataform" when both config and default are empty strings', () => {
      expect(getEffectiveTableFolderRoot('', '')).to.equal('_dataform');
    });
  });

  suite('getEffectiveTableFolderSubpath', () => {
    const testDataset = 'my_dataset';
    const testTable = 'my_table';

    test('returns configTableFolderSubpath when provided', () => {
      expect(getEffectiveTableFolderSubpath(testDataset, testTable, 'ws/sub', 'config/sub')).to.equal('config/sub');
    });

    test('returns defaultTableFolderSubpath when configTableFolderSubpath is undefined', () => {
      expect(getEffectiveTableFolderSubpath(testDataset, testTable, 'ws/sub', undefined)).to.equal('ws/sub');
    });

    test('returns defaultTableFolderSubpath when configTableFolderSubpath is an empty string', () => {
      expect(getEffectiveTableFolderSubpath(testDataset, testTable, 'ws/sub', '')).to.equal('ws/sub');
    });

    test('constructs from dataset/table when both config and default are undefined', () => {
      expect(getEffectiveTableFolderSubpath(testDataset, testTable, undefined, undefined)).to.equal('my_dataset/my_table');
    });

    test('constructs from dataset/table when both config and default are empty strings', () => {
      expect(getEffectiveTableFolderSubpath(testDataset, testTable, '', '')).to.equal('my_dataset/my_table');
    });
  });

  suite('getConnectionForIcebergTable', () => {
    test('returns the config connection string if it exists', () => {
      expect(getConnectionForIcebergTable('config-connection', 'default-connection')).to.equal('config-connection');
    });

    test('returns the default connection string if the config connection is undefined', () => {
      expect(getConnectionForIcebergTable(undefined, 'default-connection')).to.equal('default-connection');
    });

    test('returns the default connection string if the config connection is an empty string', () => {
      expect(getConnectionForIcebergTable('', 'default-connection')).to.equal('default-connection');
    });

    test('returns "DEFAULT" when the connections are undefined', () => {
      expect(getConnectionForIcebergTable(undefined, undefined)).to.equal('DEFAULT');
    });

    test('returns "DEFAULT" when the connections are empty strings', () => {
      expect(getConnectionForIcebergTable('', '')).to.equal('DEFAULT');
    });
  });

  suite('getFileFormatValueForIcebergTable', () => {
    test('returns PARQUET when configFileFormat is undefined', () => {
      expect(getFileFormatValueForIcebergTable(undefined)).to.equal(dataform.FileFormat.PARQUET);
    });

    test('returns PARQUET when configFileFormat is "PARQUET"', () => {
      expect(getFileFormatValueForIcebergTable('PARQUET')).to.equal(dataform.FileFormat.PARQUET);
    });

    test('returns PARQUET when fileFormat is an empty string', () => {
      expect(getFileFormatValueForIcebergTable('')).to.equal(dataform.FileFormat.PARQUET);
    });

    test('is case insensitive ', () => {
      expect(getFileFormatValueForIcebergTable('parquet')).to.equal(dataform.FileFormat.PARQUET);
      expect(getFileFormatValueForIcebergTable('pArQuEt')).to.equal(dataform.FileFormat.PARQUET);
    });

    test('throws an error for an unsupported file format string', () => {
      assertThrowsWithMessage(
        () => getFileFormatValueForIcebergTable('CSV'),
        'File format CSV is not supported.'
      );
    });
  });

  suite('getStorageUriForIcebergTable', () => {
    const testBucket = 'my-iceberg-bucket';
    const testRoot = '_dataform';
    const testSubpath = 'data/v1';

    test('constructs the URI with a provided tableFolderRoot', () => {
      expect(getStorageUriForIcebergTable(testBucket, testRoot, testSubpath)).to.equal('gs://my-iceberg-bucket/_dataform/data/v1');
    });

    test('handles empty bucket name, resulting in an invalid but formed URI', () => {
      expect(getStorageUriForIcebergTable('', testRoot, testSubpath)).to.equal('gs:///_dataform/data/v1');
    });

    test('handles empty tableFolderRoot, resulting in an invalid but formed URI', () => {
      expect(getStorageUriForIcebergTable(testBucket, '', testSubpath)).to.equal('gs://my-iceberg-bucket//data/v1');
    });

    test('handles empty tableFolderSubpath', () => {
      expect(getStorageUriForIcebergTable(testBucket, testRoot, '')).to.equal('gs://my-iceberg-bucket/_dataform/');
    });
  });
});
