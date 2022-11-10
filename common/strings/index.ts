export function timingSafeEqual(a: string, b: string) {
  let result = 0;

  if (a.length !== b.length) {
    b = a;
    result = 1;
  }

  for (let i = 0; i < a.length; i++) {
    // tslint:disable-next-line: no-bitwise
    result |= a.charCodeAt(i) ^ b.charCodeAt(i);
  }

  return result === 0;
}
