import Long from "long";

const MIN_INCLUSIVE_SIGNED_32_BIT_INT = -Math.pow(2, 31);
const MAX_EXCLUSIVE_SIGNED_32_BIT_INT = -MIN_INCLUSIVE_SIGNED_32_BIT_INT;

const MIN_INCLUSIVE_UNSIGNED_32_BIT_INT = 0;
const MAX_EXCLUSIVE_UNSIGNED_32_BIT_INT = Math.pow(2, 32);

const MIN_INCLUSIVE_32_BIT_FLOAT = -3.4028234663852886e38;
const MAX_INCLUSIVE_32_BIT_FLOAT = 3.4028234663852886e38;

const MIN_INCLUSIVE_64_BIT_FLOAT = -1.7976931348623157e308;
const MAX_INCLUSIVE_64_BIT_FLOAT = 1.7976931348623157e308;

export function requireNonOptional(val: any) {
  if (val === null) {
    throw new Error("Field may not be set to null.");
  }
  if (val === undefined) {
    throw new Error("Field may not be set to undefined.");
  }
}

export function checkSignedInt32(val: number | number[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal =>
      check32BitInteger(MIN_INCLUSIVE_SIGNED_32_BIT_INT, MAX_EXCLUSIVE_SIGNED_32_BIT_INT, singleVal)
    );
    return;
  }
  check32BitInteger(MIN_INCLUSIVE_SIGNED_32_BIT_INT, MAX_EXCLUSIVE_SIGNED_32_BIT_INT, val);
}

export function checkUnsignedInt32(val: number | number[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal =>
      check32BitInteger(
        MIN_INCLUSIVE_UNSIGNED_32_BIT_INT,
        MAX_EXCLUSIVE_UNSIGNED_32_BIT_INT,
        singleVal
      )
    );
    return;
  }
  check32BitInteger(MIN_INCLUSIVE_UNSIGNED_32_BIT_INT, MAX_EXCLUSIVE_UNSIGNED_32_BIT_INT, val);
}

function check32BitInteger(minInclusive: number, maxExclusive: number, val: number) {
  if (val - Math.floor(val) !== 0) {
    throw new Error(`${val} is not an integer.`);
  }
  if (val < minInclusive) {
    throw new Error(`${val} is less than ${minInclusive}.`);
  }
  if (val >= maxExclusive) {
    throw new Error(`${val} is greater than or equal to ${maxExclusive}.`);
  }
}

export function checkSignedInt64(val: Long | Long[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal => check64BitInteger(false, singleVal));
    return;
  }
  check64BitInteger(false, val);
}

export function checkUnsignedInt64(val: Long | Long[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal => check64BitInteger(true, singleVal));
    return;
  }
  check64BitInteger(true, val);
}

function check64BitInteger(unsigned: boolean, val: Long) {
  if (val.unsigned !== unsigned) {
    throw new Error(`${val} should be ${unsigned ? "un" : ""}signed.`);
  }
}

export function check32BitFloat(val: number | number[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal =>
      checkFloat(MIN_INCLUSIVE_32_BIT_FLOAT, MAX_INCLUSIVE_32_BIT_FLOAT, singleVal)
    );
    return;
  }
  checkFloat(MIN_INCLUSIVE_32_BIT_FLOAT, MAX_INCLUSIVE_32_BIT_FLOAT, val);
}

export function check64BitFloat(val: number | number[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal =>
      checkFloat(MIN_INCLUSIVE_64_BIT_FLOAT, MAX_INCLUSIVE_64_BIT_FLOAT, singleVal)
    );
    return;
  }
  checkFloat(MIN_INCLUSIVE_64_BIT_FLOAT, MAX_INCLUSIVE_64_BIT_FLOAT, val);
}

function checkFloat(minInclusive: number, maxInclusive: number, val: number) {
  if (val === Number.POSITIVE_INFINITY || val === Number.NEGATIVE_INFINITY || val === Number.NaN) {
    return;
  }
  if (val < minInclusive) {
    throw new Error(`${val} is less than ${minInclusive}.`);
  }
  if (val > maxInclusive) {
    throw new Error(`${val} is greater than ${maxInclusive}.`);
  }
}
