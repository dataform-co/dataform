type RetryReturnType = (func: () => Promise<any>, retries: number, error?: Error) => Promise<any>;

export const retryPromise: RetryReturnType = async (func, retries, error) => {
  if (retries < 0) {
    return error;
  }

  try {
    return func();
  } catch (e) {
    return retryPromise(func, retries - 1, e);
  }
};
