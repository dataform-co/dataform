import { verifyObjectMatchesProto, VerifyProtoErrorBehaviour } from "df/common/protos";
import { ActionBuilder } from "df/core/actions";
import { Resolvable } from "df/core/common";
import * as Path from "df/core/path";
import { Session } from "df/core/session";
import {
  actionConfigToCompiledGraphTarget,
  addDependenciesToActionDependencyTargets,
  configTargetToCompiledGraphTarget,
  nativeRequire,
  resolveActionsConfigFilename
} from "df/core/utils";
import { dataform } from "df/protos/ts";

/**
 * Notebooks run Jupyter Notebook files, and can output content to the storage buckets defined in
 * `workflow_settings.yaml` files.
 *
 * You can create notebooks in the following ways. Available config options are defined in
 * [NotebookConfig](configs#dataform-ActionConfig-NotebookConfig), and are shared across all the
 * following ways of creating notebooks.
 *
 * **Using action configs files:**
 *
 * ```yaml
 * # definitions/actions.yaml
 * actions:
 * - notebook:
 *   filename: name.ipynb
 * ```
 *
 * ```ipynb
 * # definitions/name.ipynb
 * { "cells": [] }
 * ```
 *
 * **Using the Javascript API:**
 *
 * ```js
 * // definitions/file.js
 * notebook("name", { filename: "name.ipynb" })
 * ```
 *
 * ```ipynb
 * # definitions/name.ipynb
 * { "cells": [] }
 * ```
 */
export class Notebook extends ActionBuilder<dataform.Notebook> {
  /**
   * @hidden Stores the generated proto for the compiled graph.
   * <!-- TODO(ekrekr): make this field private, to enforce proto update logic to happen in this
   * class. -->
   */
  public proto: dataform.INotebook = dataform.Notebook.create();

  /** @hidden Hold a reference to the Session instance. */
  public session: Session;

  /**
   * @hidden If true, adds the inline assertions of dependencies as direct dependencies for this
   * action.
   */
  public dependOnDependencyAssertions: boolean = false;

  /** @hidden */
  constructor(
    session?: Session,
    config?: dataform.ActionConfig.NotebookConfig,
    configPath?: string
  ) {
    super(session);

    if (!config.name) {
      config.name = Path.basename(config.filename);
    }
    const target = actionConfigToCompiledGraphTarget(config);
    config.filename = resolveActionsConfigFilename(config.filename, configPath);

    this.session = session;
    this.proto.target = this.applySessionToTarget(
      target,
      session.projectConfig,
      config.filename,
      true
    );
    this.proto.canonicalTarget = this.applySessionToTarget(target, session.canonicalProjectConfig);
    this.proto.tags = config.tags;
    this.dependOnDependencyAssertions = config.dependOnDependencyAssertions;
    if (config.dependencyTargets) {
      this.dependencies(
        config.dependencyTargets.map(dependencyTarget =>
          configTargetToCompiledGraphTarget(dataform.ActionConfig.Target.create(dependencyTarget))
        )
      );
    }
    this.proto.fileName = config.filename;
    if (config.disabled) {
      this.proto.disabled = config.disabled;
    }

    const notebookContents = nativeRequire(config.filename).asJson;
    this.proto.notebookContents = JSON.stringify(
      stripNotebookOutputs(notebookContents, config.filename)
    );
  }

  /**
   * Sets or overrides the contents of the notebook to run. Not recommended in general; using
   * separate `.ipynb` files for notebooks is preferred.
   */
  public ipynb(contents: object): Notebook {
    this.proto.notebookContents = JSON.stringify(contents);
    return this;
  }

  /** @hidden Verifies that the passed action config is a valid Notebook action config. */
  public config(config: any) {
    // TODO(ekrekr): call verifyObjectMatchesProto here.
    return this;
  }

  /** @hidden */
  private dependencies(value: Resolvable | Resolvable[]) {
    const newDependencies = Array.isArray(value) ? value : [value];
    newDependencies.forEach(resolvable =>
      addDependenciesToActionDependencyTargets(this, resolvable)
    );
    return this;
  }

  /** @hidden */
  public getFileName() {
    return this.proto.fileName;
  }

  /** @hidden */
  public getTarget() {
    return dataform.Target.create(this.proto.target);
  }

  /** @hidden */
  public compile() {
    return verifyObjectMatchesProto(
      dataform.Notebook,
      this.proto,
      VerifyProtoErrorBehaviour.SUGGEST_REPORTING_TO_DATAFORM_TEAM
    );
  }
}

/** @hidden Removes all notebook cell outputs. */
function stripNotebookOutputs(
  notebookAsJson: { [key: string]: unknown },
  path: string
): { [key: string]: unknown } {
  if (!("cells" in notebookAsJson)) {
    throw new Error(`Notebook at ${path} is invalid: cells field not present`);
  }
  (notebookAsJson.cells as Array<{ [key: string]: unknown }>).forEach((cell, index) => {
    if ("outputs" in cell) {
      cell.outputs = [];
      (notebookAsJson.cells as Array<{ [key: string]: unknown }>)[index] = cell;
    }
  });
  return notebookAsJson;
}
