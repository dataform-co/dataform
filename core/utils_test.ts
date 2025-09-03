import {validateConnectionFormat, validateStorageUriFormat} from './utils';
import { suite, test } from 'df/testing';

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
    const expectedStorageError = new Error(STORAGE_ERROR_MSG);

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
});
