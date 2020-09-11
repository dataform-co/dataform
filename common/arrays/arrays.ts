export function flatten<T>(nestedArray: T[][]) {
  return (
    nestedArray?.reduce((previousValue: T[], currentValue: T[]) => {
      return previousValue.concat(currentValue);
    }, []) || []
  );
}
