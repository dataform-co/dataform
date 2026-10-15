/**
 * Runs `fn` with `DATAFORM_LINEAGE_DEBUG` set to `value` (or unset when
 * `value` is `undefined`), then restores the previous value. The emitter reads
 * the variable in its constructor, so create the emitter inside `fn`.
 */
export async function withDebugEnv<T>(value: string | undefined, fn: () => Promise<T>): Promise<T> {
  const originalDebug = process.env.DATAFORM_LINEAGE_DEBUG;
  if (value === undefined) {
    delete process.env.DATAFORM_LINEAGE_DEBUG;
  } else {
    process.env.DATAFORM_LINEAGE_DEBUG = value;
  }
  try {
    return await fn();
  } finally {
    if (originalDebug === undefined) {
      delete process.env.DATAFORM_LINEAGE_DEBUG;
    } else {
      process.env.DATAFORM_LINEAGE_DEBUG = originalDebug;
    }
  }
}
