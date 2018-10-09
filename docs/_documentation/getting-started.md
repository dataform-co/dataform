---
layout: documentation
title: Getting started
sub_headers: ["Installation", "Create new project"]
---

# Installation

Dataform can be installed via Node:

```bash
npm i -g @dataform/cli
```

# Create a new project

To create a new project in the folder `new_project`:

```bash
dataform init new_project --warehouse bigquery
```

Currently supported warehouse types are `[bigquery, redshift, snowflake, postgres]`

# Project structure

The default project structure is as follows:

<div class="bp3-tree bp3-elevation-0">
  <ul class="bp3-tree-node-list bp3-tree-root">
    <li class="bp3-tree-node bp3-tree-node-expanded">
      <div class="bp3-tree-node-content">
        <span class="bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard"></span>
        <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-folder-close"></span>
        <span class="bp3-tree-node-label">new_project</span>
      </div>
      <ul class="bp3-tree-node-list" style="margin-left: 20px">
        <li class="bp3-tree-node">
          <div class="bp3-tree-node-content">
            <span class="bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard"></span>
            <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-folder-close"></span>
            <span class="bp3-tree-node-label">models</span>
          </div>
        </li>
        <li class="bp3-tree-node">
          <div class="bp3-tree-node-content">
            <span class="bp3-tree-node-caret bp3-tree-node-caret-open bp3-icon-standard"></span>
            <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-folder-close"></span>
            <span class="bp3-tree-node-label">includes</span>
          </div>
        </li>
        <li class="bp3-tree-node">
          <div class="bp3-tree-node-content">
            <span class="bp3-tree-node-caret-none bp3-icon-standard"></span>
            <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-document"></span>
            <span class="bp3-tree-node-label">package.json</span>
          </div>
        </li>
        <li class="bp3-tree-node">
          <div class="bp3-tree-node-content">
            <span class="bp3-tree-node-caret-none bp3-icon-standard"></span>
            <span class="bp3-tree-node-icon bp3-icon-standard bp3-icon-document"></span>
            <span class="bp3-tree-node-label">dataform.json</span>
          </div>
        </li>
      </ul>
    </li>
  </ul>
</div>

## Models

The `models/` directory contains files that define [materializations](/docs/modelling/#Materializations), assertions, and operations.
