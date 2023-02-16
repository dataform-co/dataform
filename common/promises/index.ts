export function runAsyncIgnoringErrors(promise: Promise<any>) {
  // tslint:disable-next-line: no-console
  promise.catch(e => console.error(`runAsyncIgnoringErrors caught error: ${e}`));
}

export async function runWithTimeout<T>(
  fn: () => Promise<T>,
  timeoutFn: () => Promise<T>,
  timeoutMillis: number
): Promise<T> {
  let timer: NodeJS.Timer;
  try {
    return await Promise.race([
      fn(),
      new Promise<T>((resolve, reject) => {
        timer = setTimeout(async () => {
          try {
            resolve(await timeoutFn());
          } catch (e) {
            reject(e);
          }
        }, timeoutMillis);
      })
    ]);
  } finally {
    clearTimeout(timer);
  }
}

export async function sleep(sleepMillis: number) {
  await new Promise<void>(resolve => setTimeout(() => resolve(), sleepMillis));
}

export async function sleepUntil(
  conditionFn: () => boolean | Promise<boolean>,
  sleepPeriodMillis: number = 100
) {
  while (!(await conditionFn())) {
    await sleep(sleepPeriodMillis);
  }
}

export async function sleepImmediate() {
  await new Promise(resolve => setImmediate(resolve));
}

export async function retry<T>(
  fn: () => Promise<T>,
  maxAttempts: number = 1,
  matchesRetriableError: (e: any) => boolean = () => true
): Promise<T> {
  let lastErr;
  for (let i = 0; i < maxAttempts; i++) {
    try {
      return await fn();
    } catch (e) {
      if (!matchesRetriableError(e)) {
        throw e;
      }
      lastErr = e;
    }
  }
  throw lastErr;
}

// This should behave similarly to the (currently experimental) Promise.any function.
// See https://github.com/tc39/proposal-promise-any for more context.
// This is added here instead of upgrading to esnext to avoid introducing additional
// experimental features.
export async function promiseAny<T>(promises: Array<Promise<T>>): Promise<T> {
  if (!promises) {
    throw new Error(`promiseAny given ${promises}; requires array of promises`);
  }
  if (promises.length === 0) {
    return new Promise(() => undefined);
  }
  return new Promise(async (resolve, reject) => {
    let storedError: Error;
    await Promise.all(
      promises.map(async promise => {
        try {
          await promise.then(result => resolve(result));
        } catch (e) {
          storedError = e;
        }
      })
    );
    reject(storedError);
  });
}
