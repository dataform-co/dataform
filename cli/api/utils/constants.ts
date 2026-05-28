// Top-level compilation worker timeout (single dataform compile worker that
// runs once per invocation). Per-model JiT compile workers use
// DEFAULT_JIT_COMPILATION_TIMEOUT_MILLIS instead.
export const DEFAULT_COMPILATION_TIMEOUT_MILLIS = 300000;

// Per-model JiT compilation worker timeout. Each action with JiT code gets its
// own fresh budget; this is per-model, not a shared budget across the run.
export const DEFAULT_JIT_COMPILATION_TIMEOUT_MILLIS = 60 * 1000;
