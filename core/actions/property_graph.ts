import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { Session } from "df/core/session";
import { checkAssertionsForDependency, setActionJobLabels } from "df/core/utils";
import { dataform } from "df/protos/ts";

const CATALOG_NOT_SUPPORTED_MESSAGE =
  "Catalog-based data sources (BigLake/Iceberg/external catalogs) are not yet supported.";

function nodeRefKey(name: string): string {
  return `node:${name}`;
}

function edgeRefKey(name: string): string {
  return `edge:${name}`;
}

export class PropertyGraph extends ActionBuilder<dataform.PropertyGraph> {
  public session: Session;
  public dependOnDependencyAssertions: boolean = false;

  private proto = dataform.PropertyGraph.create({ description: "", disabled: false });
  // Populated during construction for node/edge configs that use a ref-style
  // dataSource; drained in finalize() once all actions are indexed, since the target
  // schema/database of a ref cannot be resolved until every action is known.
  // Keys are produced by nodeRefKey / edgeRefKey.
  private pendingRefs = new Map<string, dataform.ITarget>();
  private dependencyKeys = new Set<string>();

  constructor(session?: Session, unverifiedConfig?: any, filename?: string) {
    super(session);
    this.session = session;

    if (!unverifiedConfig) {
      return;
    }

    this.normalizeNodesAndEdges(unverifiedConfig);

    const config = this.verifyConfig(unverifiedConfig);

    if (!config.name) {
      throw new Error("Property graphs must have a populated 'name' field.");
    }
    if (!config.nodes || config.nodes.length === 0) {
      throw new Error(`Property graph '${config.name}' must declare at least one node.`);
    }

    const configTarget = dataform.Target.create({
      name: config.name,
      schema: config.targetDataset?.datasetId,
      database: config.targetDataset?.projectId,
    });
    this.proto.target = this.applySessionToTarget(configTarget, session.projectConfig, filename);
    this.proto.canonicalTarget = this.applySessionToTarget(
      configTarget,
      session.canonicalProjectConfig,
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
    if (config.tags) {
      this.proto.tags = config.tags;
    }
    if (config.dependOnDependencyAssertions) {
      this.dependOnDependencyAssertions = config.dependOnDependencyAssertions;
    }

    setActionJobLabels(this.proto, config.jobLabels, this.session);

    this.proto.nodes = config.nodes.map((nodeConfig) => this.buildNode(nodeConfig, config.name));
    this.proto.edges = (config.edges || []).map((edgeConfig) =>
      this.buildEdge(edgeConfig, config.name),
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
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM,
    );
    return this.proto;
  }

  public finalize() {
    let allResolved = true;
    for (const node of this.proto.nodes) {
      if (!this.resolveRefIntoDataSource(node, nodeRefKey(node.name))) {
        allResolved = false;
      }
    }
    for (const edge of this.proto.edges) {
      if (!this.resolveRefIntoDataSource(edge, edgeRefKey(edge.name))) {
        allResolved = false;
      }
    }
    if (allResolved) {
      this.proto.graphBody = this.emitGraphBody();
    }
  }

  private resolveRefIntoDataSource(
    nodeOrEdge: dataform.IGraphNodeTable | dataform.IGraphEdgeTable,
    key: string,
  ): boolean {
    const rawRef = this.pendingRefs.get(key);
    if (!rawRef) {
      return true;
    }
    const target = this.session.resolveTarget(rawRef);
    if (!target) {
      return false;
    }
    nodeOrEdge.dataSource = dataform.Target.create(target);
    return true;
  }

  private normalizeNodesAndEdges(unverifiedConfig: any) {
    for (const node of unverifiedConfig.nodes || []) {
      normalizeRef(node);
      normalizeKeys(node);
      normalizeFields(node);
      normalizeFieldsOnLabels(node);
    }
    for (const edge of unverifiedConfig.edges || []) {
      normalizeRef(edge);
      normalizeKeys(edge);
      normalizeFields(edge);
      normalizeFieldsOnLabels(edge);
      normalizeJoinKeys(edge.source);
      normalizeJoinKeys(edge.destination);
    }
  }

  private verifyConfig(unverifiedConfig: any): dataform.PropertyGraphConfig {
    return verifyObjectMatchesProto(
      dataform.PropertyGraphConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
    );
  }

  private buildNode(
    nodeConfig: dataform.IGraphNodeTableConfig,
    graphName: string,
  ): dataform.GraphNodeTable {
    if (!nodeConfig.name) {
      throw new Error(`Property graph '${graphName}': every node must have a 'name'.`);
    }
    const nodeName = nodeConfig.name;
    const where = `Property graph '${graphName}': node '${nodeName}'`;
    const dataSource = this.resolveDataSource(nodeConfig, nodeRefKey(nodeName), where);
    if (!nodeConfig.keys || nodeConfig.keys.length === 0) {
      throw new Error(`${where} must declare 'keys'.`);
    }

    const labels = buildLabels(nodeName, nodeConfig, where);

    return dataform.GraphNodeTable.create({
      name: nodeName,
      dataSource,
      keys: nodeConfig.keys,
      labels,
    });
  }

  private buildEdge(
    edgeConfig: dataform.IGraphEdgeTableConfig,
    graphName: string,
  ): dataform.GraphEdgeTable {
    if (!edgeConfig.name) {
      throw new Error(`Property graph '${graphName}': every edge must have a 'name'.`);
    }
    const edgeName = edgeConfig.name;
    const where = `Property graph '${graphName}': edge '${edgeName}'`;
    const dataSource = this.resolveDataSource(edgeConfig, edgeRefKey(edgeName), where);

    if (!edgeConfig.source) {
      throw new Error(`${where} must declare 'source'.`);
    }
    if (!edgeConfig.destination) {
      throw new Error(`${where} must declare 'destination'.`);
    }

    const source = buildEndpoint(edgeConfig.source, `${where} source`);
    const destination = buildEndpoint(edgeConfig.destination, `${where} destination`);

    const labels = buildLabels(edgeName, edgeConfig, where);

    return dataform.GraphEdgeTable.create({
      name: edgeName,
      dataSource,
      keys: edgeConfig.keys || [],
      source,
      destination,
      labels,
    });
  }

  private resolveDataSource(
    nodeOrEdge: dataform.IGraphNodeTableConfig | dataform.IGraphEdgeTableConfig,
    pendingKey: string,
    where: string,
  ): dataform.Target {
    if (nodeOrEdge.dataSourceCatalog) {
      throw new Error(`${where}: ${CATALOG_NOT_SUPPORTED_MESSAGE}`);
    }
    if (nodeOrEdge.dataSourceString) {
      return parseTablePath(nodeOrEdge.dataSourceString, where);
    }
    if (nodeOrEdge.dataSourceDataset) {
      const ds = nodeOrEdge.dataSourceDataset;
      if (!ds.table) {
        throw new Error(`${where}: 'dataSourceDataset.table' is required.`);
      }
      const project = ds.project || this.session.projectConfig.defaultDatabase || undefined;
      const dataset = ds.dataset || this.session.projectConfig.defaultSchema || undefined;
      if (!dataset) {
        throw new Error(
          `${where}: 'dataSourceDataset.dataset' is required (no defaultDataset in workflow ` +
            `settings).`,
        );
      }
      return dataform.Target.create({ name: ds.table, schema: dataset, database: project });
    }
    if (nodeOrEdge.dataSourceRef) {
      const ref = nodeOrEdge.dataSourceRef;
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
      if (ref.hasOwnProperty("includeDependentAssertions")) {
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
    const nodeNames = new Set<string>();
    for (const node of this.proto.nodes) {
      if (nodeNames.has(node.name)) {
        throw new Error(`Property graph '${graphName}': duplicate node name '${node.name}'.`);
      }
      nodeNames.add(node.name);
    }
    const edgeNames = new Set<string>();
    for (const edge of this.proto.edges) {
      if (edgeNames.has(edge.name)) {
        throw new Error(`Property graph '${graphName}': duplicate edge name '${edge.name}'.`);
      }
      edgeNames.add(edge.name);
    }
  }

  private resolveEndpointDefaults() {
    const keysByNode = new Map<string, string[]>();
    for (const node of this.proto.nodes) {
      keysByNode.set(node.name, node.keys);
    }
    for (const edge of this.proto.edges) {
      for (const endpoint of [edge.source, edge.destination]) {
        if (endpoint.nodeColumns.length === 0) {
          const targetKeys = keysByNode.get(endpoint.node);
          if (targetKeys) {
            endpoint.nodeColumns = targetKeys.slice();
          }
        }
      }
    }
  }

  private validateEndpointReferences(graphName: string) {
    const keysByNode = new Map<string, string[]>();
    for (const node of this.proto.nodes) {
      keysByNode.set(node.name, node.keys);
    }
    for (const edge of this.proto.edges) {
      for (const [role, endpoint] of [
        ["source", edge.source],
        ["destination", edge.destination],
      ] as const) {
        const targetKeys = keysByNode.get(endpoint.node);
        if (!targetKeys) {
          throw new Error(
            `Property graph '${graphName}': edge '${edge.name}' ${role} references ` +
              `unknown node '${endpoint.node}'.`,
          );
        }
        if (endpoint.nodeColumns.length !== endpoint.edgeColumns.length) {
          throw new Error(
            `Property graph '${graphName}': edge '${edge.name}' ${role} join_keys arity ` +
              `mismatch: edgeColumns has ${endpoint.edgeColumns.length}, ` +
              `nodeColumns has ${endpoint.nodeColumns.length}.`,
          );
        }
      }
    }
  }

  private emitGraphBody(): string {
    const parts: string[] = [];
    const nodeEntries = this.proto.nodes.map((node) => renderNode(node));
    parts.push(`NODE TABLES (\n  ${nodeEntries.join(",\n  ")}\n)`);
    if (this.proto.edges.length > 0) {
      const edgeEntries = this.proto.edges.map((edge) => renderEdge(edge));
      parts.push(`EDGE TABLES (\n  ${edgeEntries.join(",\n  ")}\n)`);
    }
    return parts.join("\n");
  }
}

function normalizeRef(nodeOrEdge: any) {
  if (nodeOrEdge.ref === undefined || nodeOrEdge.ref === null) {
    return;
  }
  if (typeof nodeOrEdge.ref === "string") {
    nodeOrEdge.dataSourceRef = { name: nodeOrEdge.ref };
    delete nodeOrEdge.ref;
    return;
  }
  if (typeof nodeOrEdge.ref === "object") {
    nodeOrEdge.dataSourceRef = nodeOrEdge.ref;
    delete nodeOrEdge.ref;
  }
}

function normalizeKeys(nodeOrEdge: any) {
  if (typeof nodeOrEdge.keys === "string") {
    nodeOrEdge.keys = [nodeOrEdge.keys];
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
      typeof field === "string" ? { name: field, expression: field } : field,
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
      edgeColumns: endpoint.joinKeys,
      nodeColumns: [],
    };
  }
}

function buildEndpoint(
  endpointConfig: dataform.IGraphEndpointConfig,
  where: string,
): dataform.GraphEndpoint {
  if (!endpointConfig.node) {
    throw new Error(`${where}: must declare 'node'.`);
  }
  if (!endpointConfig.joinKeys) {
    throw new Error(`${where}: must declare 'joinKeys'.`);
  }
  const edgeCols = endpointConfig.joinKeys.edgeColumns || [];
  const nodeCols = endpointConfig.joinKeys.nodeColumns || [];
  if (edgeCols.length === 0) {
    throw new Error(`${where}: 'joinKeys.edgeColumns' must be non-empty.`);
  }
  return dataform.GraphEndpoint.create({
    node: endpointConfig.node,
    edgeColumns: edgeCols,
    nodeColumns: nodeCols,
  });
}

function buildLabels(
  defaultLabelName: string,
  config: dataform.IGraphNodeTableConfig | dataform.IGraphEdgeTableConfig,
  where: string,
): dataform.GraphLabel[] {
  const rootFields = config.fields || [];
  const rootWildcard = config.fieldWildcard;
  const configuredLabels = config.labels || [];
  if ((rootFields.length > 0 || rootWildcard) && configuredLabels.length > 0) {
    throw new Error(
      `${where} cannot combine root-level 'fields'/'fieldWildcard' with a 'labels' list.`,
    );
  }
  if (configuredLabels.length > 0) {
    const built = configuredLabels.map((label) => buildLabel(label, where, defaultLabelName));
    const defaultCount = built.filter((label) => label.isDefault).length;
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
    fieldWildcard: rootWildcard,
  });
  return [buildLabel(synthesized, where, defaultLabelName, true)];
}

function buildLabel(
  label: dataform.IGraphLabelConfig,
  where: string,
  defaultLabelName: string,
  forceDefault = false,
): dataform.GraphLabel {
  const isDefault = forceDefault || !label.name || label.name.toUpperCase() === "DEFAULT";
  if (!isDefault && !label.name) {
    throw new Error(`${where}: every non-default label must have a 'name'.`);
  }
  const labelId = isDefault ? "DEFAULT" : `'${label.name}'`;
  const wildcard = label.fieldWildcard;
  if (wildcard && wildcard.importAll === false && wildcard.except && wildcard.except.length > 0) {
    throw new Error(
      `${where}: label ${labelId} has 'fieldWildcard.except' set but 'importAll' is false.`,
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
        `these are only allowed on the DEFAULT label (name absent or 'DEFAULT').`,
    );
  }
  if (!hasFields && !hasWildcard && !hasOptions) {
    throw new Error(
      `${where}: label ${labelId} must declare at least one of: 'fields', ` +
        `'fieldWildcard', 'description', or 'synonyms'.`,
    );
  }
  return dataform.GraphLabel.create({
    name: isDefault ? defaultLabelName : label.name,
    description: label.description,
    synonyms: label.synonyms || [],
    fields: (label.fields || []).map((field) =>
      dataform.GraphField.create({
        name: field.name,
        expression: field.expression || field.name,
        description: field.description,
        synonyms: field.synonyms || [],
      }),
    ),
    importAll: !!wildcard?.importAll,
    importExcept: wildcard?.except || [],
    isDefault,
  });
}

function parseTablePath(path: string, where: string): dataform.Target {
  const parts = path.split(".");
  if (parts.length === 4) {
    throw new Error(`${where}: ${CATALOG_NOT_SUPPORTED_MESSAGE}`);
  }
  if (parts.length !== 3) {
    throw new Error(
      `${where}: 'dataSourceString' must be 'project.dataset.table' (got '${path}').`,
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

function renderNode(node: dataform.IGraphNodeTable): string {
  const pieces: string[] = [];
  pieces.push(`${renderQualifiedTable(node.dataSource)} AS ${node.name}`);
  pieces.push(`KEY (${node.keys.join(", ")})`);
  for (const label of node.labels) {
    pieces.push(renderLabelClause(label));
  }
  return pieces.join(" ");
}

function renderEdge(edge: dataform.IGraphEdgeTable): string {
  const pieces: string[] = [];
  pieces.push(`${renderQualifiedTable(edge.dataSource)} AS ${edge.name}`);
  if (edge.keys.length > 0) {
    pieces.push(`KEY (${edge.keys.join(", ")})`);
  }
  pieces.push(renderEndpoint("SOURCE", edge.source));
  pieces.push(renderEndpoint("DESTINATION", edge.destination));
  for (const label of edge.labels) {
    pieces.push(renderLabelClause(label));
  }
  return pieces.join(" ");
}

function renderEndpoint(role: "SOURCE" | "DESTINATION", endpoint: dataform.IGraphEndpoint): string {
  return (
    `${role} KEY (${endpoint.edgeColumns.join(", ")}) ` +
    `REFERENCES ${endpoint.node} (${endpoint.nodeColumns.join(", ")})`
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
  const rendered = label.fields.map((field) => renderField(field));
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
  synonyms: string[] | null | undefined,
): string {
  const parts: string[] = [];
  if (description) {
    parts.push(`description=${JSON.stringify(description)}`);
  }
  if (synonyms && synonyms.length > 0) {
    parts.push(`synonyms=[${synonyms.map((s) => JSON.stringify(s)).join(", ")}]`);
  }
  return parts.length > 0 ? `OPTIONS(${parts.join(", ")})` : "";
}
