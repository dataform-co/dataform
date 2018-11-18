---
layout: documentation
title: MaterializationConfig reference
struct:
  description: "The `MaterializationConfig` interface can be used to set several properties of a materialization at once."
  fields:
    - name: type
      type: '"view" | "table" | "incremental"'
      description: "The type of the materialization"
    - name: query
      type: "Contextable<string>"
      description: "The materialization select query"
    - name: where
      type: "Contextable<string>"
      description: "The where clause for incremental tables"
    - name: preOps
      type: "Contextable<string | string[]>"
      description: "Operations to run before the materialization"
    - name: postOps
      type: "Contextable<string | string[]>"
      description: "Operations to run after the materialization"
    - name: dependencies
      type: "string | string[]"
      description: "The dependencies of this node"
    - name: descriptor
      type: "{ [field: string]: string }"
      description: "The descriptor for the table, a map of field names to descriptions"
---

{% include struct.md struct=page.struct %}
