import { snakeToCamelKeys } from "df/core/utils";
import { dataform } from "df/protos/ts";

export const SEMANTIC_ONTOLOGY_VERSION = "0.2.0.dev0/google";

export const CATALOG_NOT_SUPPORTED_MESSAGE =
  "Catalog-based data sources (BigLake/Iceberg/external catalogs) are not yet supported.";

export const SUPPORTED_METRIC_AGGREGATES = ["SUM", "AVG", "COUNT", "MIN", "MAX"];

const DEPLOYMENT_TARGET_URI_REGEX =
  /^(?:\/\/)?bigquery\.googleapis\.com\/projects\/([^/]+)\/datasets\/([^/]+)\/propertyGraphs\/([^/]+)$/;

const TABLE_SOURCE_URI_REGEX =
  /^(?:\/\/)?bigquery\.googleapis\.com\/projects\/([^/]+)\/datasets\/([^/]+)\/tables\/([^/]+)$/;

const STRING_LITERAL_REGEX = /'(?:\\.|[^'\\])*'|"(?:\\.|[^"\\])*"/g;

// GoogleSQL reserved keywords that must be backtick-quoted when used as bare
// identifiers (entity/relationship aliases, label names, key columns, or
// property aliases) in CREATE PROPERTY GRAPH DDL.
const GOOGLE_SQL_RESERVED_KEYWORDS = new Set([
  "ALL",
  "AND",
  "ANY",
  "ARRAY",
  "AS",
  "ASC",
  "ASSERT_ROWS_MODIFIED",
  "AT",
  "BETWEEN",
  "BY",
  "CASE",
  "CAST",
  "COLLATE",
  "CONTAINS",
  "CREATE",
  "CROSS",
  "CUBE",
  "CURRENT",
  "DEFAULT",
  "DEFINE",
  "DESC",
  "DISTINCT",
  "ELSE",
  "END",
  "ENUM",
  "ESCAPE",
  "EXCEPT",
  "EXCLUDE",
  "EXISTS",
  "EXTRACT",
  "FALSE",
  "FETCH",
  "FOLLOWING",
  "FOR",
  "FROM",
  "FULL",
  "GRAPH_TABLE",
  "GROUP",
  "GROUPING",
  "GROUPS",
  "HASH",
  "HAVING",
  "IF",
  "IGNORE",
  "IN",
  "INNER",
  "INTERSECT",
  "INTERVAL",
  "INTO",
  "IS",
  "JOIN",
  "LATERAL",
  "LEFT",
  "LIKE",
  "LIMIT",
  "LOOKUP",
  "MERGE",
  "NATURAL",
  "NEW",
  "NO",
  "NOT",
  "NULL",
  "NULLS",
  "OF",
  "ON",
  "OR",
  "ORDER",
  "OUTER",
  "OVER",
  "PARTITION",
  "PRECEDING",
  "PROTO",
  "QUALIFY",
  "RANGE",
  "RECURSIVE",
  "RESPECT",
  "RIGHT",
  "ROLLUP",
  "ROWS",
  "SELECT",
  "SET",
  "SOME",
  "STRUCT",
  "TABLESAMPLE",
  "THEN",
  "TO",
  "TREAT",
  "TRUE",
  "UNBOUNDED",
  "UNION",
  "UNNEST",
  "USING",
  "WHEN",
  "WHERE",
  "WINDOW",
  "WITH",
  "WITHIN",
]);

export interface IResolvedDeploymentTarget {
  projectId?: string;
  datasetId?: string;
}

export function normalizePropertyGraphConfig(rawConfig: any): any {
  if (!rawConfig || typeof rawConfig !== "object" || Array.isArray(rawConfig)) {
    return rawConfig;
  }
  const config = snakeToCamelKeys(rawConfig, new Set(["jobLabels"]));
  if (!Array.isArray(config.semanticModel)) {
    return config;
  }
  for (const model of config.semanticModel) {
    if (!model || typeof model !== "object") {
      continue;
    }
    for (const entity of model.entities || []) {
      if (!entity || typeof entity !== "object") {
        continue;
      }
      normalizeSource(entity);
      normalizeStringList(entity, "primaryKey");
      normalizeUniqueKeys(entity);
      normalizeFields(entity);
    }
    for (const rel of model.relationships || []) {
      if (!rel || typeof rel !== "object") {
        continue;
      }
      normalizeSource(rel);
      normalizeStringList(rel, "primaryKey");
      normalizeStringList(rel, "fromColumns");
      normalizeStringList(rel, "toColumns");
    }
  }
  return config;
}

function normalizeSource(container: any) {
  if (container.source === undefined || container.source === null) {
    return;
  }
  if (typeof container.source === "string") {
    container.sourceString = container.source;
    delete container.source;
    return;
  }
  if (typeof container.source !== "object" || Array.isArray(container.source)) {
    throw new Error("'source' must be a table URI string or a '{ ref: ... }' object.");
  }
  if (!("ref" in container.source)) {
    throw new Error("'source' object must contain a 'ref' property.");
  }
  const extraKeys = Object.keys(container.source).filter((key) => key !== "ref");
  if (extraKeys.length > 0) {
    throw new Error(
      `Unexpected property '${extraKeys[0]}' in 'source' ` +
        `(only 'ref' is allowed; nest target properties inside 'source.ref').`,
    );
  }
  if (typeof container.source.ref === "string") {
    container.sourceRef = { name: container.source.ref };
  } else if (
    container.source.ref &&
    typeof container.source.ref === "object" &&
    !Array.isArray(container.source.ref)
  ) {
    container.sourceRef = container.source.ref;
  } else {
    container.sourceRef = {};
  }
  delete container.source;
}

function normalizeStringList(container: any, fieldName: string) {
  if (typeof container[fieldName] === "string") {
    container[fieldName] = [container[fieldName]];
  }
}

function normalizeUniqueKeys(entity: any) {
  if (!Array.isArray(entity.uniqueKeys)) {
    return;
  }
  entity.uniqueKeys = entity.uniqueKeys.map((item: any) => {
    if (typeof item === "string") {
      return { columns: [item] };
    }
    if (Array.isArray(item)) {
      return { columns: item };
    }
    return item;
  });
}

function normalizeFields(entity: any) {
  if (Array.isArray(entity.fields)) {
    entity.fields = entity.fields.map((field: string | dataform.IGraphFieldConfig) =>
      typeof field === "string" ? { name: field, expression: field } : field,
    );
  }
}

export function resolveDeploymentTarget(
  model: dataform.ISemanticModelConfig,
  graphName: string,
): IResolvedDeploymentTarget {
  const deployments = model.deployments || [];
  if (deployments.length === 0) {
    return {};
  }
  if (deployments.length > 1) {
    throw new Error(
      `Property graph '${graphName}': at most one deployment is supported in V1 ` +
        `(found ${deployments.length}).`,
    );
  }
  const deployment = deployments[0];
  const rawTarget = (deployment.target || "").trim();
  if (!rawTarget) {
    throw new Error(`Property graph '${graphName}': deployment must declare a non-empty 'target'.`);
  }
  const match = rawTarget.match(DEPLOYMENT_TARGET_URI_REGEX);
  if (!match) {
    throw new Error(
      `Property graph '${graphName}': deployment 'target' must match ` +
        `'bigquery.googleapis.com/projects/<project>/datasets/<dataset>/propertyGraphs/<graph>' ` +
        `(got '${deployment.target}').`,
    );
  }
  const [, projectId, datasetId, targetGraphName] = match;
  if (targetGraphName !== graphName) {
    throw new Error(
      `Property graph '${graphName}': deployment target graph name '${targetGraphName}' ` +
        `must match semantic_model name '${graphName}'.`,
    );
  }
  return { projectId, datasetId };
}

export function parseSourceTableString(rawSource: string, where: string): dataform.Target {
  const trimmed = rawSource.trim();
  if (
    trimmed.startsWith("bigquery.googleapis.com/") ||
    trimmed.startsWith("//bigquery.googleapis.com/")
  ) {
    const match = trimmed.match(TABLE_SOURCE_URI_REGEX);
    if (!match) {
      throw new Error(
        `${where}: 'source' URI must match ` +
          `'bigquery.googleapis.com/projects/<project>/datasets/<dataset>/tables/<table>' ` +
          `(got '${rawSource}').`,
      );
    }
    return dataform.Target.create({
      database: match[1],
      schema: match[2],
      name: match[3],
    });
  }

  const withoutPrefix = trimmed.startsWith("bigquery:")
    ? trimmed.slice("bigquery:".length)
    : trimmed;
  const parts = withoutPrefix.split(".");
  if (parts.length === 4 && parts.every((p) => p.length > 0)) {
    throw new Error(`${where}: ${CATALOG_NOT_SUPPORTED_MESSAGE}`);
  }
  if (parts.length === 3 && parts.every((p) => p.length > 0)) {
    return dataform.Target.create({
      database: parts[0],
      schema: parts[1],
      name: parts[2],
    });
  }
  throw new Error(
    `${where}: 'source' must be a '{ ref: ... }' object, 'bigquery:project.dataset.table', ` +
      `'project.dataset.table', or ` +
      `'bigquery.googleapis.com/projects/<project>/datasets/<dataset>/tables/<table>' ` +
      `(got '${rawSource}').`,
  );
}

export function unquoteIdentifier(name: string): string {
  if (!name || typeof name !== "string") {
    return name;
  }
  const trimmed = name.trim();
  if (trimmed.length >= 2 && trimmed.startsWith("`") && trimmed.endsWith("`")) {
    return trimmed.slice(1, -1);
  }
  return trimmed;
}

export function isSimpleIdentifier(name: string): boolean {
  return /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}

export function isReservedKeyword(name: string): boolean {
  return GOOGLE_SQL_RESERVED_KEYWORDS.has(name.toUpperCase());
}

export function quoteIdentifier(name: string): string {
  const trimmed = name.trim();
  if (/^`[^`]+`$/.test(trimmed)) {
    return trimmed;
  }
  if (isSimpleIdentifier(trimmed)) {
    return isReservedKeyword(trimmed) ? `\`${trimmed}\`` : trimmed;
  }
  return `\`${trimmed}\``;
}

export function quoteIfReserved(value: string): string {
  const trimmed = value.trim();
  if (isSimpleIdentifier(trimmed) && isReservedKeyword(trimmed)) {
    return `\`${trimmed}\``;
  }
  return value;
}

export function validateIdentifierName(name: string, itemKind: string, where: string): void {
  const unquoted = unquoteIdentifier(name);
  if (!isSimpleIdentifier(unquoted)) {
    throw new Error(
      `${where}: ${itemKind} name '${name}' must be a valid identifier ` +
        `(letters, digits, and underscores, starting with a letter or underscore).`,
    );
  }
}

const SQL_LITERAL_KEYWORDS = new Set<string>(["TRUE", "FALSE", "NULL"]);

export function isPhysicalColumnName(value: string): boolean {
  if (!value || typeof value !== "string") {
    return false;
  }
  const trimmed = value.trim();
  if (/^`[^`]+`$/.test(trimmed)) {
    return true;
  }
  return isSimpleIdentifier(trimmed) && !SQL_LITERAL_KEYWORDS.has(trimmed.toUpperCase());
}

export function validatePhysicalColumnList(
  columns: string[],
  fieldLabel: string,
  where: string,
): void {
  for (const col of columns) {
    if (!isPhysicalColumnName(col)) {
      throw new Error(
        `${where}: '${fieldLabel}' entry '${col}' must be a physical column name ` +
          `(not a qualified reference or SQL expression).`,
      );
    }
  }
}

export function validateCaseInsensitiveUnique(
  names: string[],
  itemKind: string,
  where: string,
): void {
  const seen = new Map<string, string>();
  for (const name of names) {
    const lower = unquoteIdentifier(name).toLowerCase();
    const existing = seen.get(lower);
    if (existing !== undefined) {
      throw new Error(
        `${where}: duplicate ${itemKind} name '${name}'` +
          (existing !== name ? ` (conflicts case-insensitively with '${existing}')` : "") +
          ".",
      );
    }
    seen.set(lower, name);
  }
}

export function formatDescriptionWithExamples(
  description?: string | null,
  aiContext?: dataform.IGraphAiContextConfig | null,
): string {
  const base = description || "";
  const examples = (aiContext?.examples || []).filter(
    (ex) => ex !== undefined && ex !== null && ex !== "",
  );
  if (examples.length === 0) {
    return base;
  }
  const examplesText = `Examples: ${examples.join(", ")}`;
  return base ? `${base}\n${examplesText}` : examplesText;
}

const QUALIFIER_OR_BACKTICK_TOKEN_REGEX = /(`[^`]*`)(\.?)|([A-Za-z_][A-Za-z0-9_]*)\./g;

function extractQualifierIdent(
  backtickToken: string | undefined,
  dot: string | undefined,
  bareIdent: string | undefined,
  offset: number,
  segment: string,
): string | null {
  if (offset > 0 && /[\w`.-]/.test(segment[offset - 1])) {
    return null;
  }
  if (backtickToken !== undefined) {
    if (dot !== "." || backtickToken.length <= 2) {
      return null;
    }
    return unquoteIdentifier(backtickToken);
  }
  if (bareIdent !== undefined) {
    return bareIdent;
  }
  return null;
}

function mapOutsideStringLiterals(expression: string, fn: (segment: string) => string): string {
  let out = "";
  let last = 0;
  const regex = new RegExp(STRING_LITERAL_REGEX.source, "g");
  let match: RegExpExecArray | null;
  while ((match = regex.exec(expression)) !== null) {
    out += fn(expression.slice(last, match.index));
    out += match[0];
    last = match.index + match[0].length;
  }
  out += fn(expression.slice(last));
  return out;
}

function blankStringLiterals(expression: string): string {
  return expression.replace(STRING_LITERAL_REGEX, (m) => " ".repeat(m.length));
}

export function stripEntityQualifier(expression: string, entityName: string): string {
  const targetLower = unquoteIdentifier(entityName).toLowerCase();
  return mapOutsideStringLiterals(expression, (seg) =>
    seg.replace(
      QUALIFIER_OR_BACKTICK_TOKEN_REGEX,
      (
        match: string,
        backtickToken: string | undefined,
        dot: string | undefined,
        bareIdent: string | undefined,
        offset: number,
        str: string,
      ) => {
        const qualifier = extractQualifierIdent(backtickToken, dot, bareIdent, offset, str);
        if (qualifier !== null && qualifier.toLowerCase() === targetLower) {
          return "";
        }
        return match;
      },
    ),
  );
}

export function findReferencedEntityQualifiers(
  expression: string,
  entityNames: string[],
  allowedLocalQualifiers: ReadonlySet<string> = new Set(),
): string[] {
  const scannable = blankStringLiterals(expression);
  const foundQualifiersLower = new Set<string>();
  const regex = new RegExp(QUALIFIER_OR_BACKTICK_TOKEN_REGEX.source, "g");
  let match: RegExpExecArray | null;
  while ((match = regex.exec(scannable)) !== null) {
    const qualifier = extractQualifierIdent(match[1], match[2], match[3], match.index, scannable);
    if (qualifier !== null) {
      foundQualifiersLower.add(qualifier.toLowerCase());
    }
  }
  const referenced: string[] = [];
  for (const name of entityNames) {
    const lowerName = unquoteIdentifier(name).toLowerCase();
    if (allowedLocalQualifiers.has(lowerName)) {
      continue;
    }
    if (foundQualifiersLower.has(lowerName)) {
      referenced.push(name);
    }
  }
  return referenced;
}

export interface IParsedAggregate {
  fn: string;
  operand: string;
  distinct: boolean;
}

export function extractSupportedAggregate(expression: string): IParsedAggregate | null {
  const s = expression.trim();
  const head = s.match(/^([A-Za-z_][A-Za-z0-9_]*)\s*\(/);
  if (!head) {
    return null;
  }
  const fn = head[1];
  if (!SUPPORTED_METRIC_AGGREGATES.includes(fn.toUpperCase())) {
    return null;
  }

  let depth = 0;
  let inSingle = false;
  let inDouble = false;
  let inTick = false;
  let start = -1;
  let end = -1;
  let hasTopLevelComma = false;

  for (let i = head[0].length - 1; i < s.length; i++) {
    const c = s[i];
    if (inSingle) {
      if (c === "\\" && i + 1 < s.length) {
        i++;
      } else if (c === "'") {
        inSingle = false;
      }
      continue;
    }
    if (inDouble) {
      if (c === "\\" && i + 1 < s.length) {
        i++;
      } else if (c === '"') {
        inDouble = false;
      }
      continue;
    }
    if (inTick) {
      if (c === "`") {
        inTick = false;
      }
      continue;
    }
    if (c === "'") {
      inSingle = true;
      continue;
    }
    if (c === '"') {
      inDouble = true;
      continue;
    }
    if (c === "`") {
      inTick = true;
      continue;
    }
    if (c === "(") {
      if (depth === 0) {
        start = i + 1;
      }
      depth++;
      continue;
    }
    if (c === ")") {
      depth--;
      if (depth === 0) {
        end = i;
        break;
      }
      continue;
    }
    if (c === "," && depth === 1) {
      hasTopLevelComma = true;
    }
  }

  if (end === -1 || end !== s.length - 1 || hasTopLevelComma) {
    return null;
  }

  const rawInner = s.slice(start, end).trim();
  if (!rawInner) {
    return null;
  }

  const distinctMatch = rawInner.match(/^DISTINCT(?:\s+|(?=\())(.*)$/i);
  const distinct = !!distinctMatch;
  const operand = (distinctMatch ? distinctMatch[1] : rawInner).trim();
  if (!operand) {
    return null;
  }

  // Reject nested aggregates inside the operand.
  const operandWithoutStrings = blankStringLiterals(operand).replace(/`[^`]*`/g, (m) =>
    " ".repeat(m.length),
  );
  const nestedAggRegex = new RegExp(`\\b(?:${SUPPORTED_METRIC_AGGREGATES.join("|")})\\s*\\(`, "i");
  if (nestedAggRegex.test(operandWithoutStrings)) {
    return null;
  }

  return { fn, operand, distinct };
}

export function buildEntityLabels(
  entityConfig: dataform.IGraphEntityConfig,
  entityMetrics: dataform.IGraphMetricConfig[],
  allEntityNames: string[],
  graphName: string,
): dataform.GraphLabel[] {
  const entityName = entityConfig.name;
  const where = `Property graph '${graphName}': entity '${entityName}'`;
  const configuredFields = entityConfig.fields || [];

  if (entityMetrics.length > 0 && configuredFields.length === 0) {
    throw new Error(
      `${where} must declare 'fields' explicitly when 'metrics' target it, ` +
        `because BigQuery does not allow combining PROPERTIES ALL COLUMNS with MEASURE(...).`,
    );
  }

  for (const field of configuredFields) {
    if (!field.name) {
      throw new Error(`${where}: every field must have a 'name'.`);
    }
    validateIdentifierName(field.name, "field", where);
  }
  validateCaseInsensitiveUnique(
    configuredFields.map((f) => f.name),
    "field",
    where,
  );

  const builtFields: dataform.GraphField[] = configuredFields.map((field) => {
    const cleanName = unquoteIdentifier(field.name);
    const strippedExpr = field.expression
      ? stripEntityQualifier(field.expression, entityName)
      : cleanName;
    const normalizedExpr = unquoteIdentifier(strippedExpr) === cleanName ? cleanName : strippedExpr;
    return dataform.GraphField.create({
      name: cleanName,
      expression: normalizedExpr,
      description: formatDescriptionWithExamples(field.description, field.aiContext),
      synonyms: field.aiContext?.synonyms || [],
    });
  });

  const declaredFieldsByLowerName = new Map<string, dataform.GraphField>();
  const exposedNamesLower = new Map<string, string>();
  for (const field of builtFields) {
    const lowerName = unquoteIdentifier(field.name).toLowerCase();
    declaredFieldsByLowerName.set(lowerName, field);
    exposedNamesLower.set(lowerName, field.name);
  }

  const allowedLocalQualifiers = new Set<string>([
    unquoteIdentifier(entityName).toLowerCase(),
    ...exposedNamesLower.keys(),
  ]);

  const assertNoMetricFieldCollisions = () => {
    for (const metric of entityMetrics) {
      const lowerMetricName = unquoteIdentifier(metric.name).toLowerCase();
      const metricConflict = exposedNamesLower.get(lowerMetricName);
      if (metricConflict !== undefined) {
        throw new Error(
          `Property graph '${graphName}': metric '${metric.name}' collides with field ` +
            `'${metricConflict}' on entity '${entityName}'.`,
        );
      }
    }
  };

  assertNoMetricFieldCollisions();

  const autoExposedFields: dataform.GraphField[] = [];
  const measureFields: dataform.GraphField[] = [];

  for (const metric of entityMetrics) {
    const metricWhere = `Property graph '${graphName}': metric '${metric.name}'`;
    if (!metric.expression || !metric.expression.trim()) {
      throw new Error(`${metricWhere} must declare 'expression'.`);
    }

    const strippedMetricExpr = stripEntityQualifier(metric.expression, entityName);
    const referencedEntities = findReferencedEntityQualifiers(
      strippedMetricExpr,
      allEntityNames,
      allowedLocalQualifiers,
    );
    for (const refEntity of referencedEntities) {
      if (
        unquoteIdentifier(refEntity).toLowerCase() !== unquoteIdentifier(entityName).toLowerCase()
      ) {
        throw new Error(
          `${metricWhere} targets entity '${entityName}' but its expression references ` +
            `entity '${refEntity}'.`,
        );
      }
    }

    const parsedAgg = extractSupportedAggregate(strippedMetricExpr);
    if (!parsedAgg) {
      throw new Error(
        `${metricWhere}: expression '${metric.expression}' must be a single supported ` +
          `aggregate (${SUPPORTED_METRIC_AGGREGATES.join(", ")}) over one operand or COUNT(*).`,
      );
    }

    let loweredOperand = parsedAgg.operand;

    if (parsedAgg.operand === "*") {
      if (parsedAgg.fn.toUpperCase() !== "COUNT" || parsedAgg.distinct) {
        throw new Error(
          `${metricWhere}: '${parsedAgg.fn}(${parsedAgg.distinct ? "DISTINCT " : ""}*)' ` +
            `is not supported (only COUNT(*) is allowed).`,
        );
      }
      // BigQuery graph MEASURE(...) expressions must aggregate an exposed node
      // property rather than `*` (see
      // https://cloud.google.com/bigquery/docs/reference/standard-sql/graph-ddl-statements).
      // Lower COUNT(*) to COUNT(<primary_key[0]>) and ensure that key column is
      // exposed as a property on the node label.
      const pkCol = unquoteIdentifier(entityConfig.primaryKey[0]);
      const lowerPk = pkCol.toLowerCase();
      if (!exposedNamesLower.has(lowerPk)) {
        exposedNamesLower.set(lowerPk, pkCol);
        autoExposedFields.push(
          dataform.GraphField.create({
            name: pkCol,
            expression: pkCol,
          }),
        );
      }
      loweredOperand = quoteIdentifier(pkCol);
    } else if (isPhysicalColumnName(parsedAgg.operand)) {
      const cleanOperand = unquoteIdentifier(parsedAgg.operand);
      const lowerOperand = cleanOperand.toLowerCase();
      const matchedField = declaredFieldsByLowerName.get(lowerOperand);
      if (matchedField) {
        const isDerivedField =
          unquoteIdentifier(matchedField.expression) !== unquoteIdentifier(matchedField.name);
        loweredOperand = isDerivedField
          ? matchedField.expression
          : quoteIdentifier(matchedField.name);
      } else {
        if (!exposedNamesLower.has(lowerOperand)) {
          exposedNamesLower.set(lowerOperand, cleanOperand);
          autoExposedFields.push(
            dataform.GraphField.create({
              name: cleanOperand,
              expression: cleanOperand,
            }),
          );
        }
        loweredOperand = quoteIdentifier(cleanOperand);
      }
    }

    const loweredAgg = `${parsedAgg.fn}(${parsedAgg.distinct ? "DISTINCT " : ""}${loweredOperand})`;
    measureFields.push(
      dataform.GraphField.create({
        name: unquoteIdentifier(metric.name),
        expression: `MEASURE(${loweredAgg})`,
        description: formatDescriptionWithExamples(metric.description, metric.aiContext),
        synonyms: metric.aiContext?.synonyms || [],
      }),
    );
  }

  assertNoMetricFieldCollisions();

  const allFields = [...builtFields, ...autoExposedFields, ...measureFields];
  const entityDescription = formatDescriptionWithExamples(
    entityConfig.description,
    entityConfig.aiContext,
  );
  const entitySynonyms = entityConfig.aiContext?.synonyms || [];
  const hasOptions = !!entityDescription || entitySynonyms.length > 0;

  if (allFields.length === 0 && !hasOptions) {
    return [];
  }

  return [
    dataform.GraphLabel.create({
      name: entityName,
      description: entityDescription || undefined,
      synonyms: entitySynonyms,
      fields: allFields,
      importAll: allFields.length === 0,
      importExcept: [],
      isDefault: true,
    }),
  ];
}

export function buildRelationshipLabels(
  relConfig: dataform.IGraphRelationshipConfig,
): dataform.GraphLabel[] {
  const description = formatDescriptionWithExamples(relConfig.description, relConfig.aiContext);
  const synonyms = relConfig.aiContext?.synonyms || [];
  if (!description && synonyms.length === 0) {
    return [];
  }
  return [
    dataform.GraphLabel.create({
      name: relConfig.name,
      description: description || undefined,
      synonyms,
      fields: [],
      importAll: false,
      importExcept: [],
      isDefault: true,
    }),
  ];
}

export function renderNode(node: dataform.IGraphNodeTable): string {
  const pieces: string[] = [
    `${renderQualifiedTable(node.dataSource)} AS ${quoteIdentifier(node.name)}`,
    `KEY (${node.keys.map((k) => quoteIdentifier(k)).join(", ")})`,
  ];
  if (!node.labels || node.labels.length === 0) {
    pieces.push("PROPERTIES ALL COLUMNS");
    return pieces.join(" ");
  }
  for (const label of node.labels) {
    pieces.push(renderNodeLabelClause(label));
  }
  return pieces.join(" ");
}

export function renderEdge(edge: dataform.IGraphEdgeTable): string {
  const pieces: string[] = [
    `${renderQualifiedTable(edge.dataSource)} AS ${quoteIdentifier(edge.name)}`,
  ];
  if (edge.keys && edge.keys.length > 0) {
    pieces.push(`KEY (${edge.keys.map((k) => quoteIdentifier(k)).join(", ")})`);
  }
  pieces.push(renderEndpoint("SOURCE", edge.source));
  pieces.push(renderEndpoint("DESTINATION", edge.destination));
  if (!edge.labels || edge.labels.length === 0) {
    pieces.push("NO PROPERTIES");
    return pieces.join(" ");
  }
  for (const label of edge.labels) {
    pieces.push(renderEdgeLabelClause(label));
  }
  return pieces.join(" ");
}

function renderQualifiedTable(target: dataform.ITarget): string {
  const parts: string[] = [];
  if (target.database) {
    parts.push(target.database);
  }
  if (target.schema) {
    parts.push(target.schema);
  }
  parts.push(target.name);
  return "`" + parts.join(".") + "`";
}

function renderEndpoint(role: "SOURCE" | "DESTINATION", endpoint: dataform.IGraphEndpoint): string {
  return (
    `${role} KEY (${endpoint.edgeColumns.map((c) => quoteIdentifier(c)).join(", ")}) ` +
    `REFERENCES ${quoteIdentifier(endpoint.node)} ` +
    `(${endpoint.nodeColumns.map((c) => quoteIdentifier(c)).join(", ")})`
  );
}

function renderNodeLabelClause(label: dataform.IGraphLabel): string {
  const parts: string[] = [];
  if (label.isDefault) {
    const options = renderOptionsClause(label.description, label.synonyms);
    if (options) {
      parts.push(`DEFAULT LABEL ${options}`);
    }
  } else {
    parts.push(`LABEL ${quoteIdentifier(label.name)}`);
  }
  parts.push(renderNodePropertiesClause(label));
  return parts.join(" ");
}

function renderEdgeLabelClause(label: dataform.IGraphLabel): string {
  const parts: string[] = [];
  if (label.isDefault) {
    const options = renderOptionsClause(label.description, label.synonyms);
    if (options) {
      parts.push(`DEFAULT LABEL ${options}`);
    }
  } else {
    parts.push(`LABEL ${quoteIdentifier(label.name)}`);
  }
  parts.push("NO PROPERTIES");
  return parts.join(" ");
}

function renderNodePropertiesClause(label: dataform.IGraphLabel): string {
  if (label.importAll || !label.fields || label.fields.length === 0) {
    return "PROPERTIES ALL COLUMNS";
  }
  const rendered = label.fields.map((field) => renderField(field));
  return `PROPERTIES (${rendered.join(", ")})`;
}

function renderField(field: dataform.IGraphField): string {
  const alias = quoteIdentifier(field.name);
  const hasDistinctExpression =
    field.expression && unquoteIdentifier(field.expression) !== unquoteIdentifier(field.name);
  const expr = hasDistinctExpression ? `${quoteIfReserved(field.expression)} AS ${alias}` : alias;
  const options = renderOptionsClause(field.description, field.synonyms);
  return options ? `${expr} ${options}` : expr;
}

export function quoteBigQueryString(value: string): string {
  const escaped = value
    .replace(/\\/g, "\\\\")
    .replace(/"/g, '\\"')
    .replace(/\n/g, "\\n")
    .replace(/\r/g, "\\r")
    .replace(/\t/g, "\\t")
    .replace(
      /[\x00-\x08\x0b\x0c\x0e-\x1f]/g,
      (ch) => `\\x${ch.charCodeAt(0).toString(16).padStart(2, "0")}`,
    );
  return `"${escaped}"`;
}

export function renderOptionsClause(
  description: string | null | undefined,
  synonyms: string[] | null | undefined,
): string {
  const parts: string[] = [];
  if (description) {
    parts.push(`description=${quoteBigQueryString(description)}`);
  }
  if (synonyms && synonyms.length > 0) {
    parts.push(`synonyms=[${synonyms.map((s) => quoteBigQueryString(s)).join(", ")}]`);
  }
  return parts.length > 0 ? `OPTIONS(${parts.join(", ")})` : "";
}
