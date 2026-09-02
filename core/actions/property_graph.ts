import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { Session } from "df/core/session";
import { checkAssertionsForDependency } from "df/core/utils";
import { dataform } from "df/protos/ts";

const CATALOG_NOT_SUPPORTED_MESSAGE =
  "Catalog-based data sources (BigLake/Iceberg/external catalogs) are not yet supported.";

function entityRefKey(name: string): string {
  return `entity:${name}`;
}

function relationshipRefKey(name: string): string {
  return `relationship:${name}`;
}

export class PropertyGraph extends ActionBuilder<dataform.PropertyGraph> {
  public session: Session;
  public dependOnDependencyAssertions: boolean = false;

  private proto = dataform.PropertyGraph.create({ description: "", disabled: false });
  // Populated during construction for entity/relationship configs that use a ref-style
  // dataSource; drained in finalize() once all actions are indexed, since the target
  // schema/database of a ref cannot be resolved until every action is known.
  // Keys are produced by entityRefKey / relationshipRefKey.
  private pendingRefs = new Map<string, dataform.ITarget>();
  private dependencyKeys = new Set<string>();

  constructor(session?: Session, unverifiedConfig?: any, filename?: string) {
    super(session);
    this.session = session;

    if (!unverifiedConfig) {
      return;
    }

    this.normalizeEntitiesAndRelationships(unverifiedConfig);

    const config = this.verifyConfig(unverifiedConfig);

    if (!config.name) {
      throw new Error("Property graphs must have a populated 'name' field.");
    }
    if (!config.entities || config.entities.length === 0) {
      throw new Error(`Property graph '${config.name}' must declare at least one entity.`);
    }

    const configTarget = dataform.Target.create({
      name: config.name,
      schema: config.targetDataset?.datasetId,
      database: config.targetDataset?.projectId
    });
    this.proto.target = this.applySessionToTarget(configTarget, session.projectConfig, filename);
    this.proto.canonicalTarget = this.applySessionToTarget(
      configTarget,
      session.canonicalProjectConfig
    );

    this.proto.fileName = filename;
    if (config.description) {
      // Stored on the compiled proto but not yet emitted to graphBody OPTIONS;
      // DDL propagation is planned.
      this.proto.description = config.description;
    }
    if (config.disabled) {
      this.proto.disabled = config.disabled;
    }
    if (config.dependOnDependencyAssertions) {
      this.dependOnDependencyAssertions = config.dependOnDependencyAssertions;
    }

    this.proto.entities = config.entities.map(entityConfig =>
      this.buildEntity(entityConfig, config.name)
    );
    this.proto.relationships = (config.relationships || []).map(relConfig =>
      this.buildRelationship(relConfig, config.name)
    );

    this.validateUniqueNames(config.name);
    this.resolveEndpointDefaults();
    this.validateEndpointReferences(config.name);
  }

  public getFileName() {
    return this.proto.fileName;
  }

  public getTarget() {
    return dataform.Target.create(this.proto.target);
  }

  public compile() {
    this.proto = verifyObjectMatchesProto(
      dataform.PropertyGraph,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
    return this.proto;
  }

  public finalize() {
    let allResolved = true;
    for (const entity of this.proto.entities) {
      if (!this.resolveRefIntoDataSource(entity, entityRefKey(entity.name))) {
        allResolved = false;
      }
    }
    for (const rel of this.proto.relationships) {
      if (!this.resolveRefIntoDataSource(rel, relationshipRefKey(rel.name))) {
        allResolved = false;
      }
    }
    if (allResolved) {
      this.proto.graphBody = this.emitGraphBody();
    }
  }

  private resolveRefIntoDataSource(
    entityOrRel: dataform.IGraphEntity | dataform.IGraphRelationship,
    key: string
  ): boolean {
    const rawRef = this.pendingRefs.get(key);
    if (!rawRef) {
      return true;
    }
    const target = this.session.resolveTarget(rawRef);
    if (!target) {
      return false;
    }
    entityOrRel.dataSource = dataform.Target.create(target);
    return true;
  }

  private normalizeEntitiesAndRelationships(unverifiedConfig: any) {
    for (const entity of unverifiedConfig.entities || []) {
      normalizeRef(entity);
      normalizeKeys(entity);
      normalizeFields(entity);
      normalizeFieldsOnLabels(entity);
    }
    for (const relationship of unverifiedConfig.relationships || []) {
      normalizeRef(relationship);
      normalizeKeys(relationship);
      normalizeFields(relationship);
      normalizeFieldsOnLabels(relationship);
      normalizeJoinKeys(relationship.source);
      normalizeJoinKeys(relationship.destination);
    }
  }

  private verifyConfig(unverifiedConfig: any): dataform.PropertyGraphConfig {
    return verifyObjectMatchesProto(
      dataform.PropertyGraphConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK
    );
  }

  private buildEntity(
    entityConfig: dataform.IGraphEntityConfig,
    graphName: string
  ): dataform.GraphEntity {
    if (!entityConfig.name) {
      throw new Error(`Property graph '${graphName}': every entity must have a 'name'.`);
    }
    const entityName = entityConfig.name;
    const where = `Property graph '${graphName}': entity '${entityName}'`;
    const dataSource = this.resolveDataSource(entityConfig, entityRefKey(entityName), where);
    if (!entityConfig.keys || entityConfig.keys.length === 0) {
      throw new Error(`${where} must declare 'keys'.`);
    }

    const labels = buildLabels(entityName, entityConfig, where);

    return dataform.GraphEntity.create({
      name: entityName,
      dataSource,
      keys: entityConfig.keys,
      labels
    });
  }

  private buildRelationship(
    relConfig: dataform.IGraphRelationshipConfig,
    graphName: string
  ): dataform.GraphRelationship {
    if (!relConfig.name) {
      throw new Error(`Property graph '${graphName}': every relationship must have a 'name'.`);
    }
    const relName = relConfig.name;
    const where = `Property graph '${graphName}': relationship '${relName}'`;
    const dataSource = this.resolveDataSource(relConfig, relationshipRefKey(relName), where);

    if (!relConfig.source) {
      throw new Error(`${where} must declare 'source'.`);
    }
    if (!relConfig.destination) {
      throw new Error(`${where} must declare 'destination'.`);
    }

    const source = buildEndpoint(relConfig.source, `${where} source`);
    const destination = buildEndpoint(relConfig.destination, `${where} destination`);

    const labels = buildLabels(relName, relConfig, where);

    return dataform.GraphRelationship.create({
      name: relName,
      dataSource,
      keys: relConfig.keys || [],
      source,
      destination,
      labels
    });
  }

  private resolveDataSource(
    entityOrRel: dataform.IGraphEntityConfig | dataform.IGraphRelationshipConfig,
    pendingKey: string,
    where: string
  ): dataform.Target {
    if (entityOrRel.dataSourceCatalog) {
      throw new Error(`${where}: ${CATALOG_NOT_SUPPORTED_MESSAGE}`);
    }
    if (entityOrRel.dataSourceString) {
      return parseTablePath(entityOrRel.dataSourceString, where);
    }
    if (entityOrRel.dataSourceDataset) {
      const ds = entityOrRel.dataSourceDataset;
      if (!ds.table) {
        throw new Error(`${where}: 'dataSourceDataset.table' is required.`);
      }
      const project =
        ds.project || this.session.projectConfig.defaultDatabase || undefined;
      const dataset = ds.dataset || this.session.projectConfig.defaultSchema || undefined;
      if (!dataset) {
        throw new Error(
          `${where}: 'dataSourceDataset.dataset' is required (no defaultDataset in workflow ` +
            `settings).`
        );
      }
      return dataform.Target.create({ name: ds.table, schema: dataset, database: project });
    }
    if (entityOrRel.dataSourceRef) {
      const ref = entityOrRel.dataSourceRef;
      if (!ref.name) {
        throw new Error(`${where}: 'ref' must include a 'name'.`);
      }
      const rawRef: dataform.ITarget = { name: ref.name };
      if (ref.schema) {
        rawRef.schema = ref.schema;
      }
      if (ref.database) {
        rawRef.database = ref.database;
      }
      if (ref.includeDependentAssertions) {
        rawRef.includeDependentAssertions = ref.includeDependentAssertions;
      }
      this.pendingRefs.set(pendingKey, rawRef);
      const depKey = `${rawRef.database || ""}.${rawRef.schema || ""}.${rawRef.name}`;
      if (!this.dependencyKeys.has(depKey)) {
        this.dependencyKeys.add(depKey);
        const depTarget = checkAssertionsForDependency(this, rawRef);
        if (depTarget) {
          this.proto.dependencyTargets.push(depTarget);
        }
      }
      return undefined;
    }
    throw new Error(`${where}: must declare a data source.`);
  }

  private validateUniqueNames(graphName: string) {
    const entityNames = new Set<string>();
    for (const entity of this.proto.entities) {
      if (entityNames.has(entity.name)) {
        throw new Error(
          `Property graph '${graphName}': duplicate entity name '${entity.name}'.`
        );
      }
      entityNames.add(entity.name);
    }
    const relNames = new Set<string>();
    for (const rel of this.proto.relationships) {
      if (relNames.has(rel.name)) {
        throw new Error(
          `Property graph '${graphName}': duplicate relationship name '${rel.name}'.`
        );
      }
      relNames.add(rel.name);
    }
  }

  private resolveEndpointDefaults() {
    const keysByEntity = new Map<string, string[]>();
    for (const entity of this.proto.entities) {
      keysByEntity.set(entity.name, entity.keys);
    }
    for (const rel of this.proto.relationships) {
      for (const endpoint of [rel.source, rel.destination]) {
        if (endpoint.entityColumns.length === 0) {
          const targetKeys = keysByEntity.get(endpoint.entity);
          if (targetKeys) {
            endpoint.entityColumns = targetKeys.slice();
          }
        }
      }
    }
  }

  private validateEndpointReferences(graphName: string) {
    const keysByEntity = new Map<string, string[]>();
    for (const entity of this.proto.entities) {
      keysByEntity.set(entity.name, entity.keys);
    }
    for (const rel of this.proto.relationships) {
      for (const [role, endpoint] of [
        ["source", rel.source],
        ["destination", rel.destination]
      ] as const) {
        const targetKeys = keysByEntity.get(endpoint.entity);
        if (!targetKeys) {
          throw new Error(
            `Property graph '${graphName}': relationship '${rel.name}' ${role} references ` +
              `unknown entity '${endpoint.entity}'.`
          );
        }
        if (endpoint.entityColumns.length !== endpoint.relationshipColumns.length) {
          throw new Error(
            `Property graph '${graphName}': relationship '${rel.name}' ${role} join_keys arity ` +
              `mismatch: relationshipColumns has ${endpoint.relationshipColumns.length}, ` +
              `entityColumns has ${endpoint.entityColumns.length}.`
          );
        }
      }
    }
  }

  private emitGraphBody(): string {
    const parts: string[] = [];
    const nodeEntries = this.proto.entities.map(entity => renderNode(entity));
    parts.push(`NODE TABLES (\n  ${nodeEntries.join(",\n  ")}\n)`);
    if (this.proto.relationships.length > 0) {
      const edgeEntries = this.proto.relationships.map(rel => renderEdge(rel));
      parts.push(`EDGE TABLES (\n  ${edgeEntries.join(",\n  ")}\n)`);
    }
    return parts.join("\n");
  }
}

function normalizeRef(entityOrRel: any) {
  if (entityOrRel.ref === undefined || entityOrRel.ref === null) {
    return;
  }
  if (typeof entityOrRel.ref === "string") {
    entityOrRel.dataSourceRef = { name: entityOrRel.ref };
    delete entityOrRel.ref;
    return;
  }
  if (typeof entityOrRel.ref === "object") {
    entityOrRel.dataSourceRef = entityOrRel.ref;
    delete entityOrRel.ref;
  }
}

function normalizeKeys(entityOrRel: any) {
  if (typeof entityOrRel.keys === "string") {
    entityOrRel.keys = [entityOrRel.keys];
  }
}

function normalizeFields(container: any) {
  const f = container.fields;
  if (f && !Array.isArray(f) && typeof f === "object" && "importAll" in f) {
    container.fieldWildcard = f;
    delete container.fields;
    return;
  }
  if (Array.isArray(f)) {
    container.fields = f.map((field: string | dataform.IGraphFieldConfig) =>
      typeof field === "string" ? { name: field, expression: field } : field
    );
  }
}

function normalizeFieldsOnLabels(container: any) {
  if (Array.isArray(container.labels)) {
    for (const label of container.labels) {
      normalizeFields(label);
    }
  }
}

function normalizeJoinKeys(endpoint: any) {
  if (!endpoint) {
    return;
  }
  if (Array.isArray(endpoint.joinKeys)) {
    endpoint.joinKeys = {
      relationshipColumns: endpoint.joinKeys,
      entityColumns: []
    };
  }
}

function buildEndpoint(
  endpointConfig: dataform.IGraphEndpointConfig,
  where: string
): dataform.GraphEndpoint {
  if (!endpointConfig.entity) {
    throw new Error(`${where}: must declare 'entity'.`);
  }
  if (!endpointConfig.joinKeys) {
    throw new Error(`${where}: must declare 'joinKeys'.`);
  }
  const relCols = endpointConfig.joinKeys.relationshipColumns || [];
  const entityCols = endpointConfig.joinKeys.entityColumns || [];
  if (relCols.length === 0) {
    throw new Error(`${where}: 'joinKeys.relationshipColumns' must be non-empty.`);
  }
  return dataform.GraphEndpoint.create({
    entity: endpointConfig.entity,
    relationshipColumns: relCols,
    entityColumns: entityCols
  });
}

function buildLabels(
  defaultLabelName: string,
  config: dataform.IGraphEntityConfig | dataform.IGraphRelationshipConfig,
  where: string
): dataform.GraphLabel[] {
  const rootFields = config.fields || [];
  const rootWildcard = config.fieldWildcard;
  const configuredLabels = config.labels || [];
  if ((rootFields.length > 0 || rootWildcard) && configuredLabels.length > 0) {
    throw new Error(
      `${where} cannot combine root-level 'fields'/'fieldWildcard' with a 'labels' list.`
    );
  }
  if (configuredLabels.length > 0) {
    const built = configuredLabels.map(label => buildLabel(label, where, defaultLabelName));
    const defaultCount = built.filter(label => label.isDefault).length;
    if (defaultCount > 1) {
      throw new Error(`${where}: only one DEFAULT label is allowed.`);
    }
    return built;
  }
  const hasRootData =
    rootFields.length > 0 ||
    !!rootWildcard ||
    !!config.description ||
    (config.synonyms && config.synonyms.length > 0);
  if (!hasRootData) {
    return [];
  }
  const synthesized = dataform.GraphLabelConfig.create({
    name: defaultLabelName,
    description: config.description || undefined,
    synonyms: config.synonyms || [],
    fields: rootFields,
    fieldWildcard: rootWildcard
  });
  return [buildLabel(synthesized, where, defaultLabelName, true)];
}

function buildLabel(
  label: dataform.IGraphLabelConfig,
  where: string,
  defaultLabelName: string,
  forceDefault = false
): dataform.GraphLabel {
  const isDefault = forceDefault || !label.name || label.name.toUpperCase() === "DEFAULT";
  if (!isDefault && !label.name) {
    throw new Error(`${where}: every non-default label must have a 'name'.`);
  }
  const labelId = isDefault ? "DEFAULT" : `'${label.name}'`;
  const wildcard = label.fieldWildcard;
  if (wildcard && wildcard.importAll === false && wildcard.except && wildcard.except.length > 0) {
    throw new Error(
      `${where}: label ${labelId} has 'fieldWildcard.except' set but 'importAll' is false.`
    );
  }
  const hasFields = !!(label.fields && label.fields.length > 0);
  const hasWildcard = !!(wildcard && wildcard.importAll);
  const hasDescription = !!label.description;
  const hasSynonyms = !!(label.synonyms && label.synonyms.length > 0);
  const hasOptions = hasDescription || hasSynonyms;
  if (!isDefault && hasOptions) {
    throw new Error(
      `${where}: label ${labelId} cannot declare 'description' or 'synonyms'; ` +
        `these are only allowed on the DEFAULT label (name absent or 'DEFAULT').`
    );
  }
  if (!hasFields && !hasWildcard && !hasOptions) {
    throw new Error(
      `${where}: label ${labelId} must declare at least one of: 'fields', ` +
        `'fieldWildcard', 'description', or 'synonyms'.`
    );
  }
  return dataform.GraphLabel.create({
    name: isDefault ? defaultLabelName : label.name,
    description: label.description,
    synonyms: label.synonyms || [],
    fields: (label.fields || []).map(field =>
      dataform.GraphField.create({
        name: field.name,
        expression: field.expression || field.name,
        description: field.description,
        synonyms: field.synonyms || []
      })
    ),
    importAll: !!wildcard?.importAll,
    importExcept: wildcard?.except || [],
    isDefault
  });
}

function parseTablePath(path: string, where: string): dataform.Target {
  const parts = path.split(".");
  if (parts.length === 4) {
    throw new Error(`${where}: ${CATALOG_NOT_SUPPORTED_MESSAGE}`);
  }
  if (parts.length !== 3) {
    throw new Error(
      `${where}: 'dataSourceString' must be 'project.dataset.table' (got '${path}').`
    );
  }
  return dataform.Target.create({ database: parts[0], schema: parts[1], name: parts[2] });
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

function renderNode(entity: dataform.IGraphEntity): string {
  const pieces: string[] = [];
  pieces.push(`${renderQualifiedTable(entity.dataSource)} AS ${entity.name}`);
  pieces.push(`KEY (${entity.keys.join(", ")})`);
  for (const label of entity.labels) {
    pieces.push(renderLabelClause(label));
  }
  return pieces.join(" ");
}

function renderEdge(rel: dataform.IGraphRelationship): string {
  const pieces: string[] = [];
  pieces.push(`${renderQualifiedTable(rel.dataSource)} AS ${rel.name}`);
  if (rel.keys.length > 0) {
    pieces.push(`KEY (${rel.keys.join(", ")})`);
  }
  pieces.push(renderEndpoint("SOURCE", rel.source));
  pieces.push(renderEndpoint("DESTINATION", rel.destination));
  for (const label of rel.labels) {
    pieces.push(renderLabelClause(label));
  }
  return pieces.join(" ");
}

function renderEndpoint(role: "SOURCE" | "DESTINATION", endpoint: dataform.IGraphEndpoint): string {
  return (
    `${role} KEY (${endpoint.relationshipColumns.join(", ")}) ` +
    `REFERENCES ${endpoint.entity} (${endpoint.entityColumns.join(", ")})`
  );
}

function renderLabelClause(label: dataform.IGraphLabel): string {
  const head = label.isDefault ? "DEFAULT LABEL" : `LABEL ${label.name}`;
  const parts: string[] = [head];
  if (label.isDefault) {
    const options = renderOptionsClause(label.description, label.synonyms);
    if (options) {
      parts.push(options);
    }
  }
  const properties = renderPropertiesClause(label);
  if (properties) {
    parts.push(properties);
  }
  return parts.join(" ");
}

function renderPropertiesClause(label: dataform.IGraphLabel): string {
  if (label.importAll) {
    if (label.importExcept.length > 0) {
      return `PROPERTIES ARE ALL COLUMNS EXCEPT (${label.importExcept.join(", ")})`;
    }
    return "PROPERTIES ARE ALL COLUMNS";
  }
  if (label.fields.length === 0) {
    return "";
  }
  const rendered = label.fields.map(field => renderField(field));
  return `PROPERTIES (${rendered.join(", ")})`;
}

function renderField(field: dataform.IGraphField): string {
  const expr =
    field.expression && field.expression !== field.name
      ? `${field.expression} AS ${field.name}`
      : field.name;
  const options = renderOptionsClause(field.description, field.synonyms);
  return options ? `${expr} ${options}` : expr;
}

function renderOptionsClause(
  description: string | null | undefined,
  synonyms: string[] | null | undefined
): string {
  const parts: string[] = [];
  if (description) {
    parts.push(`description=${JSON.stringify(description)}`);
  }
  if (synonyms && synonyms.length > 0) {
    parts.push(`synonyms=[${synonyms.map(s => JSON.stringify(s)).join(", ")}]`);
  }
  return parts.length > 0 ? `OPTIONS(${parts.join(", ")})` : "";
}
