export async function retry(func: () => Promise<any>, retries: number): Promise<any> {
  try {
    return await func();
  } catch (e) {
    if (retries === 0) {
      throw e;
    }
    return retry(func, retries - 1);
  }
}
