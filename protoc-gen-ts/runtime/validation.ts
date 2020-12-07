import Long from "long";

const TWO_TO_31 = Math.pow(2, 31);
const NEGATIVE_TWO_TO_31 = -TWO_TO_31;
const TWO_TO_32 = Math.pow(2, 32);

const MIN_32_BIT_FLOAT = -3.4028234663852886e38;
const MAX_32_BIT_FLOAT = 3.4028234663852886e38;

const MIN_64_BIT_FLOAT = -1.7976931348623157e308;
const MAX_64_BIT_FLOAT = 1.7976931348623157e308;

export function checkSignedInt32(val: number | number[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal => check32BitInteger(NEGATIVE_TWO_TO_31, TWO_TO_31, singleVal));
    return;
  }
  check32BitInteger(NEGATIVE_TWO_TO_31, TWO_TO_31, val);
}

export function checkUnsignedInt32(val: number | number[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal => check32BitInteger(0, TWO_TO_32, singleVal));
    return;
  }
  check32BitInteger(0, TWO_TO_32, val);
}

function check32BitInteger(min: number, max: number, val: number) {
  if (val - Math.floor(val) !== 0) {
    throw new Error(`${val} is not an integer.`);
  }
  if (val < min) {
    throw new Error(`${val} is less than ${min}.`);
  }
  if (val >= max) {
    throw new Error(`${val} is greater than or equal to ${max}.`);
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
    val.forEach(singleVal => checkFloat(MIN_32_BIT_FLOAT, MAX_32_BIT_FLOAT, singleVal));
    return;
  }
  checkFloat(MIN_32_BIT_FLOAT, MAX_32_BIT_FLOAT, val);
}

export function check64BitFloat(val: number | number[]) {
  if (Array.isArray(val)) {
    val.forEach(singleVal => checkFloat(MIN_64_BIT_FLOAT, MAX_64_BIT_FLOAT, singleVal));
    return;
  }
  checkFloat(MIN_64_BIT_FLOAT, MAX_64_BIT_FLOAT, val);
}

function checkFloat(min: number, max: number, val: number) {
  if (val === Number.POSITIVE_INFINITY || val === Number.NEGATIVE_INFINITY || val === Number.NaN) {
    return;
  }
  if (val < min) {
    throw new Error(`${val} is less than ${min}.`);
  }
  if (val > max) {
    throw new Error(`${val} is greater than ${max}.`);
  }
}
