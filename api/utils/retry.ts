export async function retry<T>(func: () => Promise<T>, retries: number): Promise<T> {
  try {
    return await func();
  } catch (e) {
    if (retries === 0) {
      throw e;
    }
    return await retry(func, retries - 1);
  }
}
