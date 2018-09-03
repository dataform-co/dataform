import {Warehouse} from "dft";

export default function compile(warehouse: Warehouse, query: string): string[] {
  return [`create view as (${query})`];
}
