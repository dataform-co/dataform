/**
 * Contextable arguments can either pass a plain value for their
 * generic type `T` or can pass a function that will be called
 * with the context object for this type of operation.
 */
export type Contextable<Context, T> = T | ((ctx: Context) => T);
