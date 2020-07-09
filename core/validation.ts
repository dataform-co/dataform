// GC definition of valid suffixes: https://cloud.google.com/bigquery/docs/schemasa-z
// A-Z, 0-9, _, start with letter or underscore, 0 < n <= 128 characters.
const VALID_SUFFIX_REGEXP = /^[a-zA-Z_][a-zA-Z_0-9]{0,127}$/;
const VALID_SUFFIX_SANITIZER = /[^a-zA-Z_0-9]/g;
const VALID_SUFFIX_FIRST_CHARACTER = /[a-zA-Z_]/;

export function isValidSuffix(suffix: string) {
  return VALID_SUFFIX_REGEXP.test(suffix);
}

export function sanitizeSuffix(suffix: string) {
  if (!suffix) {
    return "";
  }
  suffix = suffix.replace(/-/g, "_").replace(VALID_SUFFIX_SANITIZER, "");
  while (suffix.length > 1 && !suffix[0].match(VALID_SUFFIX_FIRST_CHARACTER)) {
    suffix = suffix.substring(1);
  }
  suffix = suffix.length > 128 ? suffix.substr(0, 127) : suffix;
  return suffix;
}

// * Contain up to 50 characters
// * Contain letters (upper or lower case), numbers, and underscores
const VALID_PREFIX_REGEXP = /^[a-zA-Z_0-9]{0,50}$/;
const VALID_PREFIX_SANITIZER = /[^a-zA-Z_0-9]/g;

export function isValidPrefix(prefix: string) {
  return VALID_PREFIX_REGEXP.test(prefix);
}

export function sanitizePrefix(prefix: string) {
  if (!prefix) {
    return "";
  }
  prefix = prefix.replace(/-/g, "_").replace(VALID_PREFIX_SANITIZER, "");
  prefix = prefix.length > 50 ? prefix.substr(0, 49) : prefix;
  return prefix;
}
