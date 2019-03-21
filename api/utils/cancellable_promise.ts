import { EventEmitter } from "events";

export class CancellablePromise<T> implements PromiseLike<T> {
  private static readonly CANCEL_EVENT = "cancel";

  private readonly emitter = new EventEmitter();
  private readonly promise: Promise<T>;

  private cancelled = false;

  constructor(
    method: (
      resolve: (value?: T | PromiseLike<T>) => void,
      reject: (reason?: any) => void,
      onCancel?: (handleCancel: () => void) => void
    ) => void
  ) {
    this.promise = new Promise<T>((resolve, reject) =>
      method(resolve, reject, handleCancel => {
        if (this.cancelled) {
          handleCancel();
        } else {
          this.emitter.on(CancellablePromise.CANCEL_EVENT, handleCancel);
        }
      })
    );
  }

  public then<S>(
    onfulfilled?: (value: T) => S | PromiseLike<S>,
    onrejected?: (reason: any) => PromiseLike<never>
  ): Promise<S> {
    return this.promise.then(onfulfilled, onrejected);
  }

  public catch(onRejected?: (reason: any) => PromiseLike<never>): Promise<T> {
    return this.promise.catch(onRejected);
  }

  public cancel(): void {
    this.cancelled = true;
    this.emitter.emit(CancellablePromise.CANCEL_EVENT);
  }
}
