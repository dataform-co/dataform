# BigQuery Property Graphs in Dataform

[<- Back to Home](https://dataform-co.github.io/dataform) · [CLI Reference](../cli/README.md) · [Configs Reference](reference/configs.md#dataform-PropertyGraphConfig)

Dataform supports defining [BigQuery Property Graphs](https://cloud.google.com/bigquery/docs/graph-overview) using YAML configuration files (`definitions/**/graph.yaml`) aligned with the Google Semantic Ontology specification (`version: 0.2.0.dev0/google`). During compilation, Dataform resolves backing table dependencies, validates the semantic model, and emits a `CREATE OR REPLACE PROPERTY GRAPH` DDL action that can be referenced by downstream SQLX actions via `${ref("<graph_name>")}`.

> **Note:** BigQuery Property Graphs and the `version: 0.2.0.dev0/google` `graph.yaml` schema are in Preview and subject to change.

---

## File Placement and Structure

> **Note:** The Dataform compiler enforces the following file discovery and cardinality rules:
> - **File path (`graph.yaml`):** Property graph configurations must be placed inside `definitions/` in a file named `graph.yaml` (for example, `definitions/graph.yaml` or `definitions/graphs/graph.yaml`).
> - **One graph per project:** At most one `graph.yaml` file and one entry in `semantic_model` are allowed per Dataform project.

- **Key casing:** Both `snake_case` (recommended, such as `semantic_model`, `primary_key`, `from_columns`, `to_columns`, `ai_context`, `is_time`, `job_labels`, `depend_on_dependency_assertions`, `include_dependent_assertions`) and `camelCase` keys are accepted.

---

## End-to-End Example

Assume the following `workflow_settings.yaml`:

```yaml
# workflow_settings.yaml
defaultProject: my-gcp-project
defaultDataset: dataform
defaultLocation: US
dataformCoreVersion: 3.1.4
```

### 1. Define Backing Tables (`definitions/*.sqlx`)

```sql
-- definitions/authors.sqlx
config { type: "table" }
SELECT
  1 AS id,
  "Alice" AS name,
  DATE "2020-01-15" AS joined_date,
  STRUCT("PL" AS country) AS address
```

```sql
-- definitions/books.sqlx
config { type: "table" }
SELECT 10 AS id, "Graph Systems" AS title, 320 AS pages
```

```sql
-- definitions/wrote.sqlx
config { type: "table" }
SELECT 1 AS author_id, 10 AS book_id
```

### 2. Define the Property Graph (`definitions/graph.yaml`)

```yaml
version: 0.2.0.dev0/google
tags: ["graph"]
depend_on_dependency_assertions: true
job_labels:
  team: "analytics"

semantic_model:
  - name: "LibraryGraph"
    description: "Library authorship semantic model"
    deployments:
      - name: "default"
        target: "bigquery.googleapis.com/projects/my-gcp-project/datasets/analytics/propertyGraphs/LibraryGraph"

    entities:
      - name: "Author"
        source:
          ref: "authors"
        primary_key: "id"
        description: "Library author"
        ai_context:
          instructions: "Query by author id or country"
          synonyms: ["writer", "creator"]
          examples: ["Alice"]
        fields:
          - name: "id"
            datatype: "INT64"
          - name: "name"
            datatype: "STRING"
            label: "Author Name"
          - name: "upper_name"
            datatype: "STRING"
            expression: "UPPER(name)"
          - name: "joined_date"
            dimension:
              is_time: true
          - name: "country"
            expression: "address.country"

      - name: "Book"
        source:
          ref: "books"
        primary_key: "id"
        fields:
          - "id"
          - "title"
          - "pages"

      - name: "Authorship"
        source:
          ref: "wrote"
        primary_key: ["author_id", "book_id"]

    relationships:
      - name: "Wrote"
        from: "Authorship"
        to: "Author"
        from_columns: ["author_id"]
        to_columns: ["id"]
        description: "Links an authorship record to its author"
      - name: "WroteBook"
        from: "Authorship"
        to: "Book"
        from_columns: ["book_id"]
        to_columns: ["id"]

    metrics:
      - name: "book_count"
        entity: "Book"
        expression: "COUNT(id)"
        description: "Total books"
      - name: "total_pages"
        entity: "Book"
        expression: "SUM(pages)"
        label: "Total Pages"
        description: "Sum of pages across books"
```

### 3. Compiled BigQuery DDL

When executed (`dataform run`), Dataform compiles the model above into the following BigQuery DDL statement:

```sql
CREATE OR REPLACE PROPERTY GRAPH `my-gcp-project.analytics.LibraryGraph`
NODE TABLES (
  `my-gcp-project.dataform.authors` AS Author KEY (id) DEFAULT LABEL OPTIONS(description="Library author\n\nQuery by author id or country\n\nExamples: Alice", synonyms=["writer", "creator"]) PROPERTIES (id, name OPTIONS(description="Author Name"), UPPER(name) AS upper_name, joined_date OPTIONS(description="Time dimension."), address.country AS country),
  `my-gcp-project.dataform.books` AS Book KEY (id) PROPERTIES (id, title, pages, MEASURE(COUNT(id)) AS book_count OPTIONS(description="Total books"), MEASURE(SUM(pages)) AS total_pages OPTIONS(description="Total Pages\n\nSum of pages across books")),
  `my-gcp-project.dataform.wrote` AS Authorship KEY (author_id, book_id) PROPERTIES ALL COLUMNS
)
EDGE TABLES (
  `my-gcp-project.dataform.wrote` AS Wrote KEY (author_id, book_id) SOURCE KEY (author_id, book_id) REFERENCES Authorship (author_id, book_id) DESTINATION KEY (author_id) REFERENCES Author (id) DEFAULT LABEL OPTIONS(description="Links an authorship record to its author") NO PROPERTIES,
  `my-gcp-project.dataform.wrote` AS WroteBook KEY (author_id, book_id) SOURCE KEY (author_id, book_id) REFERENCES Authorship (author_id, book_id) DESTINATION KEY (book_id) REFERENCES Book (id) NO PROPERTIES
)
```

---

## Configuration Reference

For full protocol message tables, see [`property_graph_config.proto` in the Configs Reference](reference/configs.md#property_graph_config-proto).

### Top-Level Settings ([`PropertyGraphConfig`](reference/configs.md#dataform-PropertyGraphConfig))

| Field | Required | Description |
| :--- | :--- | :--- |
| `version` | Yes | Must be `"0.2.0.dev0/google"`. |
| `semantic_model` | Yes | List of semantic models (must contain 1 model). |
| `disabled` | No | If `true`, skips executing the property graph action while keeping it referenceable via `ref()`. |
| `depend_on_dependency_assertions` | No | If `true`, adds assertions attached to referenced dependencies as dependencies of this graph action. |
| `tags` | No | User-defined action tags for selective execution (`dataform run --tags ...`). |
| `job_labels` | No | Key-value pairs of BigQuery job labels applied to the graph creation job (merged with `defaultJobLabels` from `workflow_settings.yaml`, with keys defined here taking precedence). |

Model-level `description` and `ai_context` (`instructions` and `examples`) on `semantic_model[0]` populate Dataform's action metadata (`PropertyGraph.description`) only and are not emitted in the BigQuery DDL because `CREATE PROPERTY GRAPH` does not support graph-level `OPTIONS`.

### Deployment Target (`deployments`)

- **With `deployments`:** Provide a single-element list whose `target` is a GCP resource URI without a leading `//` (where `<graph>` must match `semantic_model[0].name`):
  ```yaml
  deployments:
    - name: "default"
      target: "bigquery.googleapis.com/projects/my-project/datasets/my_dataset/propertyGraphs/MyGraph"
  ```
  Leading `//` on `target` URIs is rejected. Dataform session overrides (`databaseSuffix`, `schemaSuffix`, `namePrefix`) still apply when resolving the final action target.
- **Without `deployments`:** The property graph defaults to `defaultProject` (`defaultDatabase`) and `defaultDataset` (`defaultSchema`) from `workflow_settings.yaml`, using `semantic_model[0].name` as the graph name.

### Entities (`entities` / `datasets`)

Each entry under `entities` (or its alias `datasets`) compiles to a `NODE TABLES` entry and requires `name`, `source`, and `primary_key`.

#### Backing Table (`source`)

| Format | Example | Registers Dataform DAG Dependency? |
| :--- | :--- | :--- |
| Dataform `ref` shorthand | `source: { ref: "authors" }` | Yes |
| Qualified Dataform `ref` | `source: { ref: { name: "authors", schema: "raw", database: "my-proj", include_dependent_assertions: true } }` | Yes |
| `bigquery:` table shorthand | `source: "bigquery:my-proj.raw.authors"` | No (external table) |
| Bare 3-part table identifier | `source: "my-proj.raw.authors"` | No (external table) |
| GCP table resource URI (no leading `//`) | `source: "bigquery.googleapis.com/projects/my-proj/datasets/raw/tables/authors"` | No (external table) |

#### Properties (`fields`)

- **Omitted `fields`:** When `fields` is omitted (and no `metrics` target the entity), Dataform emits `PROPERTIES ALL COLUMNS`.
- **Explicit `fields`:** When `fields` is provided, Dataform emits `PROPERTIES (...)` containing only the listed fields (and any `metrics` targeting the entity):
  - **Bare string shorthand:** `- "id"` is equivalent to `{ name: "id" }` and emits the column identifier `id` as-is.
  - **Derived SQL expression:** Set `expression` to any valid BigQuery scalar expression (for example, `UPPER(name)` or `address.country`). Expressions are passed through as-is into `<expression> AS <name>`.
  - **Informational `datatype`:** Accepted by the parser (for example, `datatype: "INT64"`) for documentation and schema tooling; has no effect on the generated BigQuery DDL.
  - **Temporal dimension (`dimension`):** Set `dimension: { is_time: true }` to fold `"Time dimension."` into the field's `OPTIONS(description=...)`.

### Relationships (`relationships`)

Every relationship is an **N:1 foreign-key edge** from `from` to `to`, backed by the `from` entity's table and `primary_key`:

- **Required fields:** `name`, `from`, `to`, `from_columns`, `to_columns`.
- **`from_columns` validation:** `from_columns` must be physical column names on the `from` entity's backing table.
- **`to_columns` validation:** `to_columns` must match the target (`to`) entity's `primary_key` in both column count and column names (case-insensitively).
- **Compiled DDL:** Each relationship compiles to an `EDGE TABLES` entry with `SOURCE KEY (<from.primary_key>) REFERENCES <from> (<from.primary_key>) DESTINATION KEY (<from_columns>) REFERENCES <to> (<to_columns>)` and `NO PROPERTIES`.
- **Modeling M:N bridge tables:** Because relationships do not accept a separate `source` or `primary_key`, model a many-to-many join table (such as `wrote`) as an intermediate entity (`Authorship` with `primary_key: ["author_id", "book_id"]`) connected by two N:1 relationships (`Wrote` -> `Author` and `WroteBook` -> `Book`), as shown in the [End-to-End Example](#2-define-the-property-graph-definitionsgraphyaml).

### Metrics (`metrics`)

Top-level `metrics` entries compile to `MEASURE(<expression>) AS <name>` properties inside the target entity's `PROPERTIES (...)` list:

- **Aggregate expressions:** At compile time, Dataform validates that `expression` is a non-empty string and passes it through as-is into `MEASURE(<expression>) AS <name>`. BigQuery validates the aggregate expression (such as `SUM(...)`, `AVG(...)`, `COUNT(...)`, `MIN(...)`, or `MAX(...)`) when the `CREATE OR REPLACE PROPERTY GRAPH` DDL executes.
  - Because BigQuery requires a column argument inside `MEASURE(COUNT(<column>))` and rejects `MEASURE(COUNT(*))`, specify an explicit column (for example, `COUNT(id)`).
- **Target entity requirements:**
  1. When one or more `metrics` target an entity, that entity must declare an explicit `fields` list (because BigQuery does not allow `MEASURE(...)` alongside `PROPERTIES ALL COLUMNS`).
  2. Any column referenced inside a metric `expression` must be declared in the target entity's `fields` list.

### Metadata and `ai_context` Folding (`OPTIONS`)

On entities and relationships, metadata is emitted as `DEFAULT LABEL OPTIONS(...)` before `PROPERTIES (...)`, `PROPERTIES ALL COLUMNS`, or `NO PROPERTIES`. On fields and metrics, metadata is emitted as `OPTIONS(...)` immediately after the property definition.

- **`OPTIONS(description=...)`:** Dataform joins the following present non-empty sections with `"\n\n"` in order:
  1. `label` (on fields and metrics)
  2. `"Time dimension."` (on fields when `dimension.is_time` is `true`)
  3. `description`
  4. `ai_context.instructions` (or a string shorthand `ai_context: "..."`)
  5. `"Examples: " + ai_context.examples.join("; ")`
- **`OPTIONS(synonyms=[...])`:** Populated from `ai_context.synonyms`.

---

## Querying Property Graphs in Downstream SQLX Actions

Reference a property graph in any downstream SQLX table, view, or assertion using `${ref("<graph_name>")}` inside BigQuery's `GRAPH_TABLE` operator:

```sql
-- definitions/author_books.sqlx
config {
  type: "view"
}

SELECT
  author_name,
  book_title,
  pages
FROM GRAPH_TABLE(
  ${ref("LibraryGraph")}
  MATCH (a:Author)<-[:Wrote]-(:Authorship)-[:WroteBook]->(b:Book)
  COLUMNS (
    a.name AS author_name,
    b.title AS book_title,
    b.pages AS pages
  )
)
```
