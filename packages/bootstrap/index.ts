import alea from "alea";
import "reflect-metadata";

interface IBootstrapOptions {
  iterations: number;
  seed: string;
  confidenceInterval: number;
}

export interface IEstimate {
  lower: number;
  value: number;
  upper: number;
}

const DEFAULT_OPTIONS: IBootstrapOptions = {
  iterations: 1000,
  seed: "random",
  confidenceInterval: 0.95
};

export class Reducers {
  public static sum() {
    return (values: number[]) => values.reduce((acc, curr) => acc + curr, 0);
  }
  public static mean() {
    return (values: number[]) => Reducers.sum()(values) / values.length;
  }
}

export class Bootstrap {

  @Reflect.metadata("udf", true)
  public static estimate<T>(
    values: T[],
    reducer: (values: T[]) => number,
    optionOverrides?: Partial<IBootstrapOptions>
  ): IEstimate {
    const options = { ...DEFAULT_OPTIONS, ...optionOverrides };
    const estimates: number[] = [];
    const rng = alea(options.seed);
    for (let i = 0; i < options.iterations; i++) {
      estimates.push(reducer(Bootstrap.sample(values, rng)));
    }
    // Sort the estimates. Make sure these get ordered as numbers, not strings... #javascript
    estimates.sort((a, b) => +a - +b);

    // Compute the top and bottom fractiles we are looking for from the interval.
    const remainderIntervalSize = 1 - options.confidenceInterval;
    const lowerFractile = remainderIntervalSize / 2;
    const upperFractile = 1 - lowerFractile;
    return {
      lower: Bootstrap.interpolatedFractile(estimates, lowerFractile),
      value: reducer(values),
      upper: Bootstrap.interpolatedFractile(estimates, upperFractile)
    };
  }

  public static sample<T>(values: T[], rng: () => number): T[] {
    const result: T[] = [];
    // tslint:disable-next-line: prefer-for-of
    for (let i = 0; i < values.length; i++) {
      result.push(values[Math.floor(rng() * values.length)]);
    }
    return result;
  }

  public static interpolatedFractile(values: number[], fractile: number) {
    // We treat the last value as the fractile 1.0 otherwise it won't be symmetric, hence the -1.
    // With 1000 values, this maps the fractile range (0.0, 1.0) to the indices range (0, 999).
    const targetIndex = fractile * (values.length - 1);
    const floorIndex = Math.floor(targetIndex);
    const ceilIndex = Math.ceil(targetIndex);
    const floorValue = values[floorIndex];
    const ceilValue = values[ceilIndex];
    if (floorIndex === ceilIndex) {
      return floorValue;
    }
    const indexInterval = targetIndex - floorIndex;
    // If the value we actually want is between two values, we linearly interpolate between them.
    return indexInterval * floorValue + (1 - indexInterval) * ceilValue;
  }
}
