---
layout: documentation
title: dataform.json reference
struct:
  description: "The `dataform.json` file contains project level information."
  fields:
    - name: warehouse
      type: '"bigquery" | "redshift"'
      description: "The type of the warehouse to compile against."
    - name: defaultSchema
      type: string
      description: "The default output schema/dataset for materializations."
    - name: assertionsSchema
      type: string
      description: "The default output schema/dataset for assertions."
---

{% include struct.md struct=page.struct %}
