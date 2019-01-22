import { Props as MethodProps } from "./components/method";

export const methods: { [name: string]: MethodProps } = {
  ref: {
    name: "ref()",
    signatures: ["ref(tableName)"],
    description: "Returns the full, query-able name of the referenced table, and adds the table to dependencies.",
    fields: [
      {
        name: "tableName",
        type: "string",
        description: "The name of the table to reference"
      }
    ]
  },
  self: {
    name: "self()",
    signatures: ["self()"],
    description: "Returns the full, query-able name of the current table"
  },
  dependencies: {
    name: "dependencies()",
    description:
      'Specifies one or more tables, operations or assertions that this node depends on. Supports wildcard matches with `"*"`.',
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
    description: "Provide one of more queries to execute after this table has completed.",
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
    description: "Provide one of more queries to execute before this table is created.",
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
    description: "Set the type of the table. View the [table guide](/guides/tables) for more info.",
    fields: [
      {
        name: "name",
        type: '"view" | "table" | "incremental"',
        description: "The type of the table"
      }
    ]
  },
  where: {
    name: "where()",
    signatures: ["where(clause)"],
    description: "Sets a where clause that is used for incremental tables.",
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
    description: "A incremental table marked protected will never be rebuilt from scratch."
  },
  disabled: {
    name: "disabled()",
    signatures: ["disabled()"],
    description: "Disable this action from being run."
  },
  config: {
    name: "config()",
    signatures: ["config(config)"],
    description: "Sets several properties of the table at once.",
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
    description: "Sets the descriptor for fields in this table.",
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
      "Adds a field to the table descriptor with the given description (optional), and returns the field name.",
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
    description: "Sets the SQL query for this table or assertion.",
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
    description: "Returns a new [`Table`](/reference/tables-js) with the given name.",
    fields: [
      {
        name: "name",
        type: "string",
        description: "The name of the table"
      },
      {
        name: "query",
        type: "Contextable<string>",
        description: "The query for the table"
      },
      {
        name: "config",
        type: "TableConfig",
        typeLink: "/reference/table-config",
        description: "The configuration object for this table"
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
        description: "The query for the table"
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
