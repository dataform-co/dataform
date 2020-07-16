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
  await new Promise(resolve => setTimeout(() => resolve(), sleepMillis));
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
