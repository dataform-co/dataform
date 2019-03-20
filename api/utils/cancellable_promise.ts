import { EventEmitter } from "events";

export class CancellablePromise<T> extends Promise<T> {
    private static CANCEL_EVENT = "cancel";
  
    private emitter = new EventEmitter();
  
    constructor(
      method: (
        resolve: (value?: T | PromiseLike<T>) => void,
        reject: (reason?: any) => void,
        onCancel?: (handleCancel: () => void) => void
      ) => void
    ) {
      super((resolve, reject) =>
        method(resolve, reject, handleCancel => this.emitter.on(CancellablePromise.CANCEL_EVENT, handleCancel))
      );
    }
  
    public cancel(): void {
      this.emitter.emit(CancellablePromise.CANCEL_EVENT);
    }
  }