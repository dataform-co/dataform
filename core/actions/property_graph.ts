import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import {
  buildEntityLabels,
  buildRelationshipLabels,
  formatDescriptionWithExamples,
  normalizePropertyGraphConfig,
  parseSourceTableString,
  renderEdge,
  renderNode,
  resolveDeploymentTarget,
  SEMANTIC_ONTOLOGY_VERSION,
  unquoteIdentifier,
  validateCaseInsensitiveUnique,
  validateIdentifierName,
  validatePhysicalColumnList,
} from "df/core/actions/property_graph_utils";
import { Session } from "df/core/session";
import { checkAssertionsForDependency, setActionJobLabels } from "df/core/utils";
import { dataform } from "df/protos/ts";

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
  // Populated during construction for entity/relationship configs that use a ref-style
  // source; drained in finalize() once all actions are indexed, since the target
  // schema/database of a ref cannot be resolved until every action is known.
  private pendingRefs = new Map<string, dataform.ITarget>();
  // Tracks 1:N FK relationships (without an explicit `source`) so finalize() can copy
  // the resolved `dataSource` from the `from` entity node once refs are resolved.
  private inferredEdgeFromNode = new Map<string, string>();
  private dependencyKeys = new Set<string>();

  constructor(session?: Session, unverifiedConfig?: any, filename?: string) {
    super(session);
    this.session = session;

    if (!unverifiedConfig) {
      return;
    }

    const normalizedConfig = normalizePropertyGraphConfig(unverifiedConfig);
    if (
      normalizedConfig &&
      typeof normalizedConfig === "object" &&
      !Array.isArray(normalizedConfig) &&
      !normalizedConfig.semanticModel &&
      ("nodes" in normalizedConfig || "edges" in normalizedConfig)
    ) {
      throw new Error(
        `Legacy property graph schema ('nodes'/'edges') is no longer supported; ` +
          `migrate to Semantic Ontology V1 ('version: ${SEMANTIC_ONTOLOGY_VERSION}' and 'semantic_model').`,
      );
    }

    const config = this.verifyConfig(normalizedConfig);

    if (!config.version) {
      throw new Error(
        `Property graph config must declare 'version: ${SEMANTIC_ONTOLOGY_VERSION}'.`,
      );
    }
    if (config.version !== SEMANTIC_ONTOLOGY_VERSION) {
      throw new Error(
        `Property graph config 'version' must be '${SEMANTIC_ONTOLOGY_VERSION}' ` +
          `(got '${config.version}').`,
      );
    }
    if (!config.semanticModel || config.semanticModel.length === 0) {
      throw new Error("Property graph config must declare a non-empty 'semantic_model' list.");
    }
    if (config.semanticModel.length > 1) {
      throw new Error(
        `Property graph config 'semantic_model' must contain exactly 1 model in V1 ` +
          `(found ${config.semanticModel.length}).`,
      );
    }

    const model = config.semanticModel[0];
    if (!model.name) {
      throw new Error("Property graphs must have a populated 'name' field.");
    }
    const graphName = model.name;

    if (!model.entities || model.entities.length === 0) {
      throw new Error(`Property graph '${graphName}' must declare at least one entity.`);
    }

    const deploymentTarget = resolveDeploymentTarget(model, graphName);
    const configTarget = dataform.Target.create({
      name: graphName,
      schema: deploymentTarget.datasetId,
      database: deploymentTarget.projectId,
    });
    this.proto.target = this.applySessionToTarget(configTarget, session.projectConfig, filename);
    this.proto.canonicalTarget = this.applySessionToTarget(
      configTarget,
      session.canonicalProjectConfig,
    );

    this.proto.fileName = filename;
    const modelDescription = formatDescriptionWithExamples(model.description, model.aiContext);
    if (modelDescription) {
      this.proto.description = modelDescription;
    }
    if (config.disabled) {
      this.proto.disabled = true;
    }
    if (config.tags && config.tags.length > 0) {
      this.proto.tags = config.tags;
    }
    if (config.dependOnDependencyAssertions) {
      this.dependOnDependencyAssertions = true;
    }

    const topJobLabels =
      config.jobLabels && Object.keys(config.jobLabels).length > 0 ? config.jobLabels : undefined;
    setActionJobLabels(this.proto, topJobLabels, this.session);

    const graphWhere = `Property graph '${graphName}'`;
    for (const entity of model.entities) {
      if (!entity.name) {
        throw new Error(`${graphWhere}: every entity must have a 'name'.`);
      }
      validateIdentifierName(entity.name, "entity", graphWhere);
    }
    validateCaseInsensitiveUnique(
      model.entities.map((e) => e.name),
      "entity",
      graphWhere,
    );

    const entityConfigByLowerName = new Map<string, dataform.IGraphEntityConfig>();
    for (const entity of model.entities) {
      entityConfigByLowerName.set(unquoteIdentifier(entity.name).toLowerCase(), entity);
    }

    const metrics = model.metrics || [];
    for (const metric of metrics) {
      if (!metric.name) {
        throw new Error(`${graphWhere}: every metric must have a 'name'.`);
      }
      validateIdentifierName(metric.name, "metric", graphWhere);
      if (!metric.entity) {
        throw new Error(`${graphWhere}: metric '${metric.name}' must declare 'entity'.`);
      }
      if (!entityConfigByLowerName.has(unquoteIdentifier(metric.entity).toLowerCase())) {
        throw new Error(
          `${graphWhere}: metric '${metric.name}' references unknown entity '${metric.entity}'.`,
        );
      }
    }
    validateCaseInsensitiveUnique(
      metrics.map((m) => m.name),
      "metric",
      graphWhere,
    );

    const metricsByEntityLower = new Map<string, dataform.IGraphMetricConfig[]>();
    for (const metric of metrics) {
      const entityLower = unquoteIdentifier(metric.entity).toLowerCase();
      const list = metricsByEntityLower.get(entityLower) || [];
      list.push(metric);
      metricsByEntityLower.set(entityLower, list);
    }

    this.proto.nodes = model.entities.map((entityConfig) =>
      this.buildNode(
        entityConfig,
        metricsByEntityLower.get(unquoteIdentifier(entityConfig.name).toLowerCase()) || [],
        graphName,
      ),
    );

    const nodesByLowerName = new Map<string, dataform.IGraphNodeTable>();
    for (const node of this.proto.nodes) {
      nodesByLowerName.set(unquoteIdentifier(node.name).toLowerCase(), node);
    }

    const relationships = model.relationships || [];
    for (const rel of relationships) {
      if (!rel.name) {
        throw new Error(`${graphWhere}: every relationship must have a 'name'.`);
      }
      validateIdentifierName(rel.name, "relationship", graphWhere);
    }
    validateCaseInsensitiveUnique(
      relationships.map((r) => r.name),
      "relationship",
      graphWhere,
    );

    this.proto.edges = relationships.map((relConfig) =>
      this.buildEdge(relConfig, nodesByLowerName, graphName),
    );
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
    const nodesByName = new Map<string, dataform.IGraphNodeTable>();
    for (const node of this.proto.nodes) {
      if (!this.resolveRefIntoDataSource(node, nodeRefKey(node.name))) {
        allResolved = false;
      }
      nodesByName.set(node.name, node);
    }
    for (const edge of this.proto.edges) {
      const inferredFrom = this.inferredEdgeFromNode.get(edge.name);
      if (inferredFrom) {
        const fromNode = nodesByName.get(inferredFrom);
        if (fromNode?.dataSource) {
          edge.dataSource = dataform.Target.create(fromNode.dataSource);
        } else {
          allResolved = false;
        }
        continue;
      }
      if (!this.resolveRefIntoDataSource(edge, edgeRefKey(edge.name))) {
        allResolved = false;
      }
    }
    // When any ref cannot be resolved, session.resolveTarget() has already recorded
    // a compilationError on the session, so leaving graphBody unset never emits a
    // valid action with an empty body.
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

  private verifyConfig(unverifiedConfig: any): dataform.PropertyGraphConfig {
    return verifyObjectMatchesProto(
      dataform.PropertyGraphConfig,
      unverifiedConfig,
      VerifyProtoErrorBehaviour.SHOW_DOCS_LINK,
    );
  }

  private buildNode(
    entityConfig: dataform.IGraphEntityConfig,
    entityMetrics: dataform.IGraphMetricConfig[],
    graphName: string,
  ): dataform.GraphNodeTable {
    const entityName = unquoteIdentifier(entityConfig.name);
    const where = `Property graph '${graphName}': entity '${entityName}'`;

    if (entityConfig.uniqueKeys && entityConfig.uniqueKeys.length > 0) {
      throw new Error(
        `${where}: 'unique_keys' are not supported in V1 because BigQuery CREATE PROPERTY GRAPH ` +
          `DDL only supports a single KEY(...) clause.`,
      );
    }

    const dataSource = this.resolveSource(entityConfig, nodeRefKey(entityName), where);
    if (!entityConfig.primaryKey || entityConfig.primaryKey.length === 0) {
      throw new Error(`${where} must declare 'primary_key'.`);
    }
    validatePhysicalColumnList(entityConfig.primaryKey, "primary_key", where);

    const normalizedEntityConfig: dataform.IGraphEntityConfig = {
      ...entityConfig,
      name: entityName,
    };
    const labels = buildEntityLabels(normalizedEntityConfig, entityMetrics, graphName);

    return dataform.GraphNodeTable.create({
      name: entityName,
      dataSource,
      keys: entityConfig.primaryKey,
      labels,
    });
  }

  private buildEdge(
    relConfig: dataform.IGraphRelationshipConfig,
    nodesByLowerName: Map<string, dataform.IGraphNodeTable>,
    graphName: string,
  ): dataform.GraphEdgeTable {
    const edgeName = unquoteIdentifier(relConfig.name);
    const where = `Property graph '${graphName}': relationship '${edgeName}'`;

    if (!relConfig.from) {
      throw new Error(`${where} must declare 'from'.`);
    }
    if (!relConfig.to) {
      throw new Error(`${where} must declare 'to'.`);
    }
    if (!relConfig.fromColumns || relConfig.fromColumns.length === 0) {
      throw new Error(`${where} must declare 'from_columns'.`);
    }
    validatePhysicalColumnList(relConfig.fromColumns, "from_columns", where);

    const fromNode = nodesByLowerName.get(unquoteIdentifier(relConfig.from).toLowerCase());
    if (!fromNode) {
      throw new Error(
        `Property graph '${graphName}': relationship '${edgeName}' 'from' references ` +
          `unknown entity '${relConfig.from}'.`,
      );
    }
    const toNode = nodesByLowerName.get(unquoteIdentifier(relConfig.to).toLowerCase());
    if (!toNode) {
      throw new Error(
        `Property graph '${graphName}': relationship '${edgeName}' 'to' references ` +
          `unknown entity '${relConfig.to}'.`,
      );
    }

    const hasExplicitSource = !!(relConfig.sourceString || relConfig.sourceRef);
    const labels = buildRelationshipLabels({ ...relConfig, name: edgeName });

    if (hasExplicitSource) {
      const dataSource = this.resolveSource(relConfig, edgeRefKey(edgeName), where);
      if (!relConfig.primaryKey || relConfig.primaryKey.length === 0) {
        throw new Error(`${where} with 'source' must declare 'primary_key'.`);
      }
      validatePhysicalColumnList(relConfig.primaryKey, "primary_key", where);

      if (!relConfig.toColumns || relConfig.toColumns.length === 0) {
        throw new Error(`${where} with 'source' must declare 'to_columns'.`);
      }
      validatePhysicalColumnList(relConfig.toColumns, "to_columns", where);

      if (relConfig.fromColumns.length !== fromNode.keys.length) {
        throw new Error(
          `${where}: 'from_columns' arity mismatch: from_columns has ` +
            `${relConfig.fromColumns.length}, entity '${fromNode.name}' primary_key has ` +
            `${fromNode.keys.length}.`,
        );
      }
      if (relConfig.toColumns.length !== toNode.keys.length) {
        throw new Error(
          `${where}: 'to_columns' arity mismatch: to_columns has ` +
            `${relConfig.toColumns.length}, entity '${toNode.name}' primary_key has ` +
            `${toNode.keys.length}.`,
        );
      }

      return dataform.GraphEdgeTable.create({
        name: edgeName,
        dataSource,
        keys: relConfig.primaryKey.slice(),
        source: dataform.GraphEndpoint.create({
          node: fromNode.name,
          edgeColumns: relConfig.fromColumns.slice(),
          nodeColumns: fromNode.keys.slice(),
        }),
        destination: dataform.GraphEndpoint.create({
          node: toNode.name,
          edgeColumns: relConfig.toColumns.slice(),
          nodeColumns: toNode.keys.slice(),
        }),
        labels,
      });
    }

    // 1:N FK relationship without an explicit `source` bridge table:
    // Reuse the `from` entity's backing table and `primary_key`.
    if (relConfig.primaryKey && relConfig.primaryKey.length > 0) {
      throw new Error(
        `${where} cannot declare 'primary_key' without 'source' ` +
          `(1:N foreign-key relationships inherit the 'from' entity's 'primary_key').`,
      );
    }

    let toColumns: string[];
    if (relConfig.toColumns && relConfig.toColumns.length > 0) {
      toColumns = relConfig.toColumns.slice();
      validatePhysicalColumnList(toColumns, "to_columns", where);
      const matchesTargetPrimaryKey =
        toColumns.length === toNode.keys.length &&
        toColumns.every(
          (col, idx) =>
            unquoteIdentifier(col).toLowerCase() ===
            unquoteIdentifier(toNode.keys[idx]).toLowerCase(),
        );
      if (!matchesTargetPrimaryKey) {
        throw new Error(
          `${where}: in a 1:N relationship (without 'source'), 'to_columns' ` +
            `([${toColumns.join(", ")}]) must match target entity '${toNode.name}' ` +
            `primary_key ([${toNode.keys.join(", ")}]).`,
        );
      }
    } else {
      toColumns = toNode.keys.slice();
    }

    if (relConfig.fromColumns.length !== toColumns.length) {
      throw new Error(
        `${where}: join column arity mismatch: from_columns has ` +
          `${relConfig.fromColumns.length}, to_columns has ${toColumns.length}.`,
      );
    }

    let dataSource: dataform.Target;
    if (fromNode.dataSource) {
      dataSource = dataform.Target.create(fromNode.dataSource);
    } else {
      this.inferredEdgeFromNode.set(edgeName, fromNode.name);
    }

    return dataform.GraphEdgeTable.create({
      name: edgeName,
      dataSource,
      keys: fromNode.keys.slice(),
      source: dataform.GraphEndpoint.create({
        node: fromNode.name,
        edgeColumns: fromNode.keys.slice(),
        nodeColumns: fromNode.keys.slice(),
      }),
      destination: dataform.GraphEndpoint.create({
        node: toNode.name,
        edgeColumns: relConfig.fromColumns.slice(),
        nodeColumns: toColumns,
      }),
      labels,
    });
  }

  private resolveSource(
    entityOrRel: dataform.IGraphEntityConfig | dataform.IGraphRelationshipConfig,
    pendingKey: string,
    where: string,
  ): dataform.Target {
    if (entityOrRel.sourceString) {
      return parseSourceTableString(entityOrRel.sourceString, where);
    }
    if (entityOrRel.sourceRef) {
      const ref = entityOrRel.sourceRef;
      if (!ref.name) {
        throw new Error(`${where}: 'source.ref' must include a 'name'.`);
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
    throw new Error(`${where} must declare 'source'.`);
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
