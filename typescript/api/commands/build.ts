import { utils, adapters } from "@dataform/core";
import * as protos from "@dataform/protos";

export function build(compiledGraph: protos.ICompiledGraph, runConfig: protos.IRunConfig) {
  return new Builder(compiledGraph, runConfig).build();
}

class Builder {
  private compiledGraph: protos.ICompiledGraph;
  private runConfig: protos.IRunConfig;

  private adapter: adapters.Adapter;

  constructor(compiledGraph: protos.ICompiledGraph, runConfig: protos.IRunConfig) {
    this.compiledGraph = compiledGraph;
    this.runConfig = runConfig;
    this.adapter = adapters.create(compiledGraph.projectConfig);
  }

  build(): protos.IExecutionGraph {
    // Firstly, turn every thing into an execution node.
    var allNodes: protos.IExecutionNode[] = [].concat(
      this.compiledGraph.materializations.map(m => this.buildMaterialization(m)),
      this.compiledGraph.operations.map(o => this.buildOperation(o)),
      this.compiledGraph.assertions.map(a => this.buildAssertion(a))
    );

    var allNodeNames = allNodes.map(n => n.name);
    var nodeNameMap: { [name: string]: protos.IExecutionNode } = {};
    allNodes.forEach(node => (nodeNameMap[node.name] = node));

    // Determine which nodes should be included.
    var includedNodeNames =
      this.runConfig.nodes && this.runConfig.nodes.length > 0
        ? utils.matchPatterns(this.runConfig.nodes, allNodeNames)
        : allNodeNames;
    var includedNodes = allNodes.filter(node => includedNodeNames.indexOf(node.name) >= 0);
    if (this.runConfig.includeDependencies) {
      // Compute all transitive dependencies.
      for (let i = 0; i < allNodes.length; i++) {
        includedNodes.forEach(node => {
          var matchingNodeNames =
            node.dependencies && node.dependencies.length > 0
              ? utils.matchPatterns(node.dependencies, allNodeNames)
              : [];
          // Update included node names.
          matchingNodeNames.forEach(nodeName => {
            if (includedNodeNames.indexOf(nodeName) < 0) {
              includedNodeNames.push(nodeName);
            }
          });
          // Update included nodes.
          includedNodes = allNodes.filter(node => includedNodeNames.indexOf(node.name) >= 0);
        });
      }
    }
    // Remove any excluded dependencies and evaluate wildcard dependencies.
    includedNodes.forEach(node => {
      node.dependencies = utils.matchPatterns(node.dependencies, includedNodeNames);
    });
    return {
      projectConfig: this.compiledGraph.projectConfig,
      runConfig: this.runConfig,
      nodes: includedNodes
    };
  }

  buildMaterialization(m: protos.IMaterialization) {
    // We try to make this common across warehouses.
    var statements: protos.IExecutionTask[] = [];
    var implicitTableType = m.type == "view" ? adapters.TableType.VIEW : adapters.TableType.TABLE;
    statements.push({
      statement: this.adapter.dropIfExists(
        m.target,
        implicitTableType == adapters.TableType.VIEW ? adapters.TableType.TABLE : adapters.TableType.VIEW
      ),
      ignoreErrors: true
    });
    if (m.type == "incremental") {
      if (m.protected && this.runConfig.fullRefresh) {
        throw Error(`Cannot run full-refresh on protected materialization "${m.name}"`);
      }
      if (!m.parsedColumns || m.parsedColumns.length == 0) {
        throw Error(`Incremental materializations must have explicitly named column selects in: ${m.name}"`);
      }
      statements.push({
        statement: (this.runConfig.fullRefresh ? this.adapter.createOrReplace : this.adapter.createIfNotExists)(
          m.target,
          this.adapter.where(m.query, "false"),
          implicitTableType,
          m.partitionBy
        )
      });
      statements.push({
        statement: this.adapter.insertInto(m.target, m.parsedColumns, this.adapter.where(m.query, m.where))
      });
    } else {
      statements.push({
        statement: this.adapter.createOrReplace(m.target, m.query, implicitTableType, m.partitionBy)
      });
    }

    return protos.ExecutionNode.create({
      name: m.name,
      dependencies: m.dependencies,
      tasks: ([] as protos.IExecutionTask[]).concat(
        m.pres.map(pre => ({ statement: pre })),
        statements,
        m.posts.map(post => ({ statement: post })),
        m.assertions.map(assertion => ({
          statement: assertion,
          type: "assertion"
        }))
      )
    });
  }

  buildOperation(operation: protos.IOperation) {
    return protos.ExecutionNode.create({
      name: operation.name,
      dependencies: operation.dependencies,
      tasks: operation.statements.map(statement => ({
        type: "statement",
        statement: statement
      }))
    });
  }

  buildAssertion(assertion: protos.IAssertion) {
    return protos.ExecutionNode.create({
      name: assertion.name,
      dependencies: assertion.dependencies,
      tasks: assertion.queries.map(query => ({
        type: "assertion",
        statement: query
      }))
    });
  }
}
