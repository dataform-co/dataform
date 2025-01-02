import { expect } from "chai";
import * as path from "path";

import { Session } from "df/core/session";
import { targetAsReadableString } from "df/core/targets";
import { dataform } from "df/protos/ts";
import { suite, test } from "df/testing";

// TODO(ekrekr): migrate the tests in this file to core/main_test.ts.

class TestConfigs {
  public static bigquery = dataform.ProjectConfig.create({
    warehouse: "bigquery",
    defaultSchema: "schema",
    defaultLocation: "US"
  });

  public static bigqueryWithDefaultDatabase = dataform.ProjectConfig.create({
    ...TestConfigs.bigquery,
    defaultDatabase: "default-database"
  });

  public static bigqueryWithSchemaSuffix = dataform.ProjectConfig.create({
    ...TestConfigs.bigquery,
    schemaSuffix: "suffix"
  });

  public static bigqueryWithDefaultDatabaseAndSuffix = dataform.ProjectConfig.create({
    ...TestConfigs.bigqueryWithDefaultDatabase,
    databaseSuffix: "suffix"
  });

  public static bigqueryWithTablePrefix = dataform.ProjectConfig.create({
    ...TestConfigs.bigquery,
    tablePrefix: "prefix"
  });
}

suite("@dataform/core", () => {
  test("validation_bigquery_fail", () => {
    const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
    session.publish("example_partitionBy_view_fail", {
      type: "view",
      bigquery: {
        partitionBy: "some_partition"
      }
    });
    session.publish("example_clusterBy_view_fail", {
      type: "view",
      bigquery: {
        clusterBy: ["some_cluster"]
      }
    });
    session.publish("example_expiring_view_fail", {
      type: "view",
      bigquery: {
        partitionExpirationDays: 7
      }
    });
    session.publish("example_require_partition_filter_view_fail", {
      type: "view",
      bigquery: {
        requirePartitionFilter: true
      }
    });
    session.publish("example_expiring_materialized_view_fail", {
      type: "view",
      materialized: true,
      bigquery: {
        partitionBy: "some_partition",
        clusterBy: ["some_cluster"],
        partitionExpirationDays: 7
      }
    });
    session.publish("example_require_partition_filter_materialized_view_fail", {
      type: "view",
      materialized: true,
      bigquery: {
        partitionBy: "some_partition",
        clusterBy: ["some_cluster"],
        requirePartitionFilter: true
      }
    });
    session.publish("example_materialize_table_fail", {
      type: "table",
      materialized: true
    });
    session.publish("example_expiring_non_partitioned_fail", {
      type: "table",
      bigquery: {
        partitionExpirationDays: 7
      }
    });
    session.publish("example_duplicate_partition_expiration_days_fail", {
      type: "table",
      bigquery: {
        partitionBy: "partition",
        partitionExpirationDays: 1,
        additionalOptions: {
          partition_expiration_days: "7"
        }
      }
    });
    session.publish("example_duplicate_require_partition_filter_fail", {
      type: "table",
      bigquery: {
        partitionBy: "partition",
        requirePartitionFilter: true,
        additionalOptions: {
          require_partition_filter: "false"
        }
      }
    });

    const graph = session.compile();

    expect(
      graph.graphErrors.compilationErrors.map(({ message, actionName }) => ({
        message,
        actionName
      }))
    ).has.deep.members([
      {
        actionName: "schema.example_partitionBy_view_fail",
        message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`
      },
      {
        actionName: "schema.example_clusterBy_view_fail",
        message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`
      },
      {
        actionName: "schema.example_expiring_view_fail",
        message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`
      },
      {
        actionName: "schema.example_require_partition_filter_view_fail",
        message: `partitionBy/clusterBy/requirePartitionFilter/partitionExpirationDays are not valid for BigQuery views`
      },
      {
        actionName: "schema.example_expiring_materialized_view_fail",
        message: `requirePartitionFilter/partitionExpirationDays are not valid for BigQuery materialized views`
      },
      {
        actionName: "schema.example_require_partition_filter_materialized_view_fail",
        message: `requirePartitionFilter/partitionExpirationDays are not valid for BigQuery materialized views`
      },
      {
        actionName: "schema.example_materialize_table_fail",
        message: "The 'materialized' option is only valid for BigQuery views"
      },
      {
        actionName: "schema.example_expiring_non_partitioned_fail",
        message:
          "requirePartitionFilter/partitionExpirationDays are not valid for non partitioned BigQuery tables"
      },
      {
        actionName: "schema.example_duplicate_partition_expiration_days_fail",
        message: "partitionExpirationDays has been declared twice"
      },
      {
        actionName: "schema.example_duplicate_require_partition_filter_fail",
        message: "requirePartitionFilter has been declared twice"
      }
    ]);
  });

  test("validation_bigquery_pass", () => {
    const session = new Session(path.dirname(__filename), TestConfigs.bigquery);
    session.publish("example_partitionBy_view_fail", {
      type: "table",
      bigquery: {
        partitionBy: "some_partition",
        clusterBy: ["some_column", "some_other_column"],
        partitionExpirationDays: 7,
        requirePartitionFilter: false
      }
    });
    session.publish("example_materialized_view", {
      type: "view",
      materialized: true
    });
    session.publish("example_additional_options", {
      type: "table",
      bigquery: {
        additionalOptions: {
          friendlyName: "name"
        }
      }
    });

    const graph = session.compile();

    expect(graph.tables[0].bigquery).to.deep.equals(
      dataform.BigQueryOptions.create({
        clusterBy: ["some_column", "some_other_column"],
        partitionBy: "some_partition",
        partitionExpirationDays: 7,
        requirePartitionFilter: false
      })
    );
    expect(graph.tables[1].materialized).to.equals(true);
    expect(graph.tables[2].bigquery).to.deep.equals(
      dataform.BigQueryOptions.create({
        additionalOptions: {
          friendlyName: "name"
        }
      })
    );
    expect(graph.graphErrors.compilationErrors).to.deep.equals([]);
  });
});
