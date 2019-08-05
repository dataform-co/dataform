import { Props as MethodProps } from "./components/method";

export const methods: { [name: string]: MethodProps } = {
  ref: {
    name: "ref()",
    signatures: ["ref(name)", 'ref({schema: "schema", name: "name"})'],
    description:
      "Returns the full, query-able name of the referenced dataset, and adds the dataset to dependencies.",
    fields: [
      {
        name: "name",
        type: "string",
        description:
          "The name of the dataset to reference. If schema is not provided, the default project configuration schema is used (or the config schema if available)."
      },
      {
        name: "schema",
        type: "string",
        description: "The schema name of the dataset to reference."
      }
    ]
  },
  self: {
    name: "self()",
    signatures: ["self()"],
    description: "Returns the full, query-able name of the current dataset"
  },
  dependencies: {
    name: "dependencies()",
    description:
      "Specifies one or more datasets, operations or assertions that this action depends on.",
    signatures: ["dependencies(deps)"],
    fields: [
      {
        name: "deps",
        type: "string | string[]",
        description: "Either a single dependency name, or a list"
      }
    ]
  },
  postops: {
    name: "postOps()",
    signatures: ["postOps(ops)"],
    description: "Provide one of more queries to execute after this dataset has completed.",
    fields: [
      {
        name: "ops",
        type: "Contextable<string | string[]>",
        description: "The queries to run"
      }
    ]
  },
  preops: {
    name: "preOps()",
    signatures: ["preOps(ops)"],
    description: "Provide one of more queries to execute before this dataset is created.",
    fields: [
      {
        name: "ops",
        type: "Contextable<string | string[]>",
        description: "The queries to run"
      }
    ]
  },
  type: {
    name: "type()",
    signatures: ["type(name)"],
    description:
      "Set the type of the dataset. View the [table guide](/guides/datasets) for more info.",
    fields: [
      {
        name: "name",
        type: '"view" | "table" | "incremental" | "inline"',
        description: "The type of the dataset"
      }
    ]
  },
  where: {
    name: "where()",
    signatures: ["where(clause)"],
    description: "Sets a where clause that is used for incremental datasets.",
    fields: [
      {
        name: "clause",
        type: "Contextable<string>",
        description: "The where clause. Can be a string or a context function."
      }
    ]
  },
  protected: {
    name: "protected()",
    signatures: ["protected()"],
    description: "A incremental dataset marked protected will never be rebuilt from scratch."
  },
  disabled: {
    name: "disabled()",
    signatures: ["disabled()"],
    description: "Disable this action from being run."
  },
  config: {
    name: "config()",
    signatures: ["config(config)"],
    description: "Sets several properties of the dataset at once.",
    fields: [
      {
        name: "config",
        type: "TableConfig",
        description: "The configuration object"
      }
    ]
  },
  descriptor: {
    name: "descriptor()",
    signatures: ["descriptor(fields)", "descriptor(field, description)", "descriptor(descriptor)"],
    description: "Sets the descriptor for fields in this dataset.",
    fields: [
      {
        name: "fields",
        type: "string[]",
        description: "A list of field names"
      },
      {
        name: "field",
        type: "string",
        description: "The field name"
      },
      {
        name: "description",
        type: "string",
        description: "The field description"
      },
      {
        name: "descriptor",
        type: "{[field: string]: string}",
        description: "A map of field names to field descriptions"
      }
    ]
  },
  describe: {
    name: "describe()",
    signatures: ["describe(field, description?)"],
    description:
      "Adds a field to the dataset descriptor with the given description (optional), and returns the field name.",
    fields: [
      {
        name: "field",
        type: "string",
        description: "The field name"
      },
      {
        name: "description",
        type: "string",
        description: "The field description"
      }
    ]
  },
  query: {
    name: "query()",
    signatures: ["query(query)"],
    description: "Sets the SQL query for this dataset or assertion.",
    fields: [
      {
        name: "query",
        type: "string",
        description: "The SQL query to run"
      }
    ]
  },
  queries: {
    name: "queries()",
    signatures: ["queries(queries)"],
    description: "Sets the SQL queries to run in order for this operation.",
    fields: [
      {
        name: "query",
        type: "string | string[]",
        description: "The SQL queries to run"
      }
    ]
  },
  publish: {
    name: "publish()",
    signatures: ["publish(name)", "publish(name, query)", "publish(name, config)"],
    description: "Returns a new [`Table`](/reference/datasets-js) with the given name.",
    fields: [
      {
        name: "name",
        type: "string",
        description: "The name of the dataset"
      },
      {
        name: "query",
        type: "Contextable<string>",
        description: "The query for the dataset"
      },
      {
        name: "config",
        type: "TableConfig",
        typeLink: "/reference/table-config",
        description: "The configuration object for this dataset"
      }
    ]
  },
  operate: {
    name: "operate()",
    signatures: ["operate(name, queries?)"],
    description: "Returns a new [`Operation`](/reference/operations-js) with the given name.",
    fields: [
      {
        name: "name",
        type: "string",
        description: "The name of the operation"
      },
      {
        name: "queries",
        type: "Contextable<string | string>",
        description: "The query for the dataset"
      }
    ]
  },
  assert: {
    name: "assert()",
    signatures: ["assert(name, query)"],
    description: "Returns a new [`Assertion`](/reference/assertion-js) with the given name.",
    fields: [
      {
        name: "name",
        type: "string",
        description: "The name of the assertion"
      },
      {
        name: "queries",
        type: "Contextable<string>",
        description: "The query for the assertion"
      }
    ]
  }
};
