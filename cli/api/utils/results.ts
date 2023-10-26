import sizeof from "object-sizeof";

export class LimitedResultSet {
  private readonly results: any[] = [];
  private currentBytes = 0;

  constructor(
    private readonly options?: {
      rowLimit?: number;
      byteLimit?: number;
    }
  ) {}

  public push(row: any) {
    this.currentBytes += sizeof(row);
    if (
      (this.options?.rowLimit && this.results.length >= this.options?.rowLimit) ||
      (this.options?.byteLimit && this.currentBytes >= this.options?.byteLimit)
    ) {
      // Limit(s) have been hit, do not add this row to the result set.
      return false;
    }
    this.results.push(row);
    return true;
  }

  public get rows() {
    return this.results;
  }
}
