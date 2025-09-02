import { validateConnectionFormat, validateStorageUriFormat } from './utils';

import { Session } from 'df/core/session';
import { suite, test } from 'df/testing';

// Manual Mock for Session to track calls to compileError
class MockSession {
  private readonly compileErrorCalls: Array<{ error: Error; filename: string }> = [];

  public getCompileErrorCalls(): ReadonlyArray<{ error: Error; filename: string }> {
    return this.compileErrorCalls;
  }

  public reset() {
    this.compileErrorCalls.length = 0;
  }

  private compileError(error: Error, filename: string) {
    this.compileErrorCalls.push({ error, filename });
  }
}

// Helper assertion function for compileErrors
function assertCompileErrorNotCalled(mock: MockSession) {
  if (mock.getCompileErrorCalls().length > 0) {
    throw new Error(`Expected compileError not to be called, but it was. Calls: ${JSON.stringify(mock.getCompileErrorCalls())}`);
  }
}

// Helper assertion function for compileErrors
function assertCompileErrorCalledWith(mock: MockSession, expectedError: Error, expectedFilename: string) {
  const calls = mock.getCompileErrorCalls();
  if (calls.length === 0) {
    throw new Error(`Expected compileError to be called, but it was not.`);
  }
  const lastCall = calls[calls.length - 1];
  if (lastCall.error.message !== expectedError.message || lastCall.filename !== expectedFilename) {
    throw new Error(`Expected compileError to be called with message "${expectedError.message}" and filename "${expectedFilename}", but got message "${lastCall.error.message}" and filename "${lastCall.filename}"`);
  }
}

// Helper assertion function for compileErrors
function assertCompileErrorCalledTimes(mock: MockSession, expectedCount: number) {
  const calls = mock.getCompileErrorCalls();
  if (calls.length !== expectedCount) {
    throw new Error(`Expected compileError to be called ${expectedCount} times, but was called ${calls.length} times.`);
  }
}

suite('Dataform Utility Validations', () => {
  const TEST_FILENAME = 'test_config.yaml';
  let mockSession: MockSession;

  function setup() {
    mockSession = new MockSession();
  }

  suite('validateConnectionFormat', () => {
    test('does not call compileError for the "DEFAULT" connection', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, 'DEFAULT', TEST_FILENAME);
      assertCompileErrorNotCalled(mockSession);
    });

    test('does not call compileError for a valid dot-separated connection format', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, 'my-project.us-central1.my-connection', TEST_FILENAME);
      assertCompileErrorNotCalled(mockSession);
      mockSession.reset();
      validateConnectionFormat(mockSession as unknown as Session, 'gcp-proj-123.europe-west4.dataform-conn_id', TEST_FILENAME);
      assertCompileErrorNotCalled(mockSession);
    });

    test('does not call compileError for a valid resource-formatted connection format', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, 'projects/my-project/locations/us-central1/connections/my-connection', TEST_FILENAME);
      assertCompileErrorNotCalled(mockSession);
      mockSession.reset();
      validateConnectionFormat(mockSession as unknown as Session, 'projects/gcp-proj-123/locations/europe-west4/connections/dataform-conn_id', TEST_FILENAME);
      assertCompileErrorNotCalled(mockSession);
    });

    test('calls compileError for an empty connection string', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, '', TEST_FILENAME);
      assertCompileErrorCalledWith(
        mockSession,
        new Error('The connection must be in the format `{project}.{location}.{connection_id}` or `projects/{project}/locations/{location}/connections/{connection_id}`, or be set to `DEFAULT`.'),
        TEST_FILENAME
      );
    });

    test('calls compileError for an invalid dot-separated format (too few parts)', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, 'my-project.my-connection', TEST_FILENAME);
      assertCompileErrorCalledTimes(mockSession, 1);
    });

    test('calls compileError for an invalid dot-separated format (too many parts)', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, 'a.b.c.d', TEST_FILENAME);
      assertCompileErrorCalledTimes(mockSession, 1);
    });

    test('calls compileError for an invalid resource format (missing segment value)', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, 'projects/my-project/locations//connections/my-connection', TEST_FILENAME);
      assertCompileErrorCalledTimes(mockSession, 1);
    });

    test('calls compileError for an invalid resource format (incorrect separators)', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, 'projects/my-project/locations-us-central1/connections/my-connection', TEST_FILENAME);
      assertCompileErrorCalledTimes(mockSession, 1);
    });

    test('calls compileError for a completely unmatching string', () => {
      setup();
      validateConnectionFormat(mockSession as unknown as Session, 'some-other-format', TEST_FILENAME);
      assertCompileErrorCalledTimes(mockSession, 1);
    });
  });

  suite('validateStorageUriFormat', () => {
    const expectedStorageError = new Error('The storage URI must be in the format `gs://{bucket_name}/{path_to_data}`.');

    test('does not call compileError for a valid gs:// URI', () => {
      setup();
      validateStorageUriFormat(mockSession as unknown as Session, 'gs://my-bucket/path/to/data', TEST_FILENAME);
      assertCompileErrorNotCalled(mockSession);
    });

    test('does not call compileError for a valid gs:// URI with just a bucket and root path', () => {
      setup();
      validateStorageUriFormat(mockSession as unknown as Session, 'gs://another-bucket/file.csv', TEST_FILENAME);
      assertCompileErrorNotCalled(mockSession);
    });

    test('calls compileError for a URI missing the gs:// prefix', () => {
      setup();
      validateStorageUriFormat(mockSession as unknown as Session, 'my-bucket/path/to/data', TEST_FILENAME);
      assertCompileErrorCalledWith(mockSession, expectedStorageError, TEST_FILENAME);
    });

    test('calls compileError for an incorrect scheme prefix', () => {
      setup();
      validateStorageUriFormat(mockSession as unknown as Session, 's3://my-bucket/path', TEST_FILENAME);
      assertCompileErrorCalledWith(mockSession, expectedStorageError, TEST_FILENAME);
    });

    test('calls compileError for a URI with no bucket name', () => {
      setup();
      validateStorageUriFormat(mockSession as unknown as Session, 'gs:///path/to/data', TEST_FILENAME);
      assertCompileErrorCalledWith(mockSession, expectedStorageError, TEST_FILENAME);
    });

    test('calls compileError for a URI with no path after the bucket', () => {
      setup();
      validateStorageUriFormat(mockSession as unknown as Session, 'gs://my-bucket/', TEST_FILENAME);
      assertCompileErrorCalledWith(mockSession, expectedStorageError, TEST_FILENAME);
    });

    test('calls compileError for an empty storage URI string', () => {
      setup();
      validateStorageUriFormat(mockSession as unknown as Session, '', TEST_FILENAME);
      assertCompileErrorCalledWith(mockSession, expectedStorageError, TEST_FILENAME);
    });
  });
});
