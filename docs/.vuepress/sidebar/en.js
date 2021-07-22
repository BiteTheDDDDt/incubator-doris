/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module.exports = [
  {
    title: "Downloads",
    directoryPath: "downloads/",
    children: ["downloads"],
    sidebarDepth: 1,
  },
  {
    title: "Compilation and Deployment",
    directoryPath: "installing/",
    children: [
      "compilation",
      "compilation-arm",
      "install-deploy",
      "upgrade",
    ],
  },
  {
    title: "Getting Started",
    directoryPath: "getting-started/",
    children: [
      "basic-usage",
      "advance-usage",
      "data-model-rollup",
      "data-partition",
      "hit-the-rollup",
      "best-practice",
    ],
  },
  {
    title: "Administrator Guide",
    directoryPath: "administrator-guide/",
    children: [
      {
        title: "Load Data",
        directoryPath: "load-data/",
        children: [
          "load-manual",
          "batch-delete-manual",
          "broker-load-manual",
          "routine-load-manual",
          "sequence-column-manual",
          "spark-load-manual",
          "stream-load-manual",
          "s3-load-manual",
          "delete-manual",
          "insert-into-manual",
          "load-json-format",
        ],
        sidebarDepth: 2,
      },
      {
        title: "Schema Change",
        directoryPath: "alter-table/",
        children: [
          "alter-table-bitmap-index",
          "alter-table-replace-table",
          "alter-table-rollup",
          "alter-table-schema-change",
          "alter-table-temp-partition",
        ],
        sidebarDepth: 2,
      },
      "materialized_view",
      {
        title: "HTTP API",
        directoryPath: "http-actions/",
        children: [
          {
            title: "FE",
            directoryPath: "fe/",
            children: [
                "bootstrap-action",
                "cancel-load-action",
                "check-decommission-action",
                "check-storage-type-action",
                "config-action",
                "connection-action",
                "get-ddl-stmt-action",
                "get-load-info-action",
                "get-load-state",
                "get-log-file-action",
                "get-small-file",
                "ha-action",
                "hardware-info-action",
                "health-action",
                "log-action",
                "logout-action",
                "meta-action",
                "meta-info-action",
                "meta-replay-state-action",
                "profile-action",
                "query-detail-action",
                "query-profile-action",
                "row-count-action",
                "session-action",
                "set-config-action",
                "show-data-action",
                "show-meta-info-action",
                "show-proc-action",
                "show-runtime-info-action",
                "statement-execution-action",
                "system-action",
                "table-query-plan-action",
                "table-row-count-action",
                "table-schema-action",
                "upload-action",
            ],
          },
          "cancel-label",
          "compaction-action",
          "connection-action",
          "fe-get-log-file",
          "get-load-state",
          "get-tablets",
          "profile-action",
          "query-detail-action",
          "restore-tablet",
          "show-data-action",
          "tablet-migration-action",
          "tablets_distribution",
        ],
        sidebarDepth: 1,
      },
      {
        title: "Maintainence Operation",
        directoryPath: "operation/",
        children: [
          "doris-error-code",
          "disk-capacity",
          "metadata-operation",
          "monitor-alert",
          "multi-tenant",
          "tablet-meta-tool",
          "tablet-repair-and-balance",
          "tablet-restore-tool",
          {
            title: "Metrics",
            directoryPath: "monitor-metrics/",
            children: [
              "be-metrics",
              "fe-metrics",
            ],
          },
        ],
        sidebarDepth: 2,
      },
      {
        title: "Configuration",
        directoryPath: "config/",
        children: [
          "be_config",
          "fe_config",
          "user_property",
        ],
        sidebarDepth: 1,
      },
      "backup-restore",
      "broker",
      "colocation-join",
      "bucket-shuffle-join",
      "dynamic-partition",
      "export-manual",
      "outfile",
      "privilege",
      "ldap",
      "resource-management",
      "running-profile",
      "runtime-filter",
      "small-file-mgr",
      "sql-mode",
      "time-zone",
      "variables",
      "update",
    ],
    sidebarDepth: 1,
  },
  {
    title: "Bast Practices",
    directoryPath: "best-practices/",
    children: [
      "fe-load-balance"
    ],
  },
  {
    title: "Extending Ability",
    directoryPath: "extending-doris/",
    children: [
      "audit-plugin",
      "doris-on-es",
      "logstash",
      "odbc-of-doris",
      "plugin-development-manual",
      "spark-doris-connector",
      "flink-doris-connector",
      {
        title: "UDF",
        directoryPath: "udf/",
        children: [
          "contribute-udf",
          "user-defined-function",
          {
            title: "Users contribute UDF",
            directoryPath: "contrib/",
            children:[
                "udaf-orthogonal-bitmap-manual",
            ],
          },          
        ],
      },
    ],
  },
  {
    title: "Design Documents",
    directoryPath: "internal/",
    children: [
      "doris_storage_optimization",
      "grouping_sets_design",
      "metadata-design",
    ],
  },
  {
    title: "SQL Manual",
    directoryPath: "sql-reference/",
    children: [
      {
        title: "SQL Functions",
        directoryPath: "sql-functions/",
        children: [
          {
            title: "Date Time Functions",
            directoryPath: "date-time-functions/",
            children: [
              "curdate",
              "current_timestamp",
              "date_add",
              "date_format",
              "date_sub",
              "datediff",
              "day",
              "dayname",
              "dayofmonth",
              "dayofweek",
              "dayofyear",
              "from_days",
              "from_unixtime",
              "hour",
              "makedate",
              "minute",
              "month",
              "monthname",
              "now",
              "second",
              "str_to_date",
              "time_round",
              "timediff",
              "timestampadd",
              "timestampdiff",
              "to_days",
              "unix_timestamp",
              "utc_timestamp",
              "week",
              "weekofyear",
              "year",
              "yearweek",
            ],
          },
          {
            title: "Sptial Functions",
            directoryPath: "spatial-functions/",
            children: [
              "st_astext",
              "st_circle",
              "st_contains",
              "st_distance_sphere",
              "st_geometryfromtext",
              "st_linefromtext",
              "st_point",
              "st_polygon",
              "st_x",
              "st_y",
            ],
          },
          {
            title: "String Functions",
            directoryPath: "string-functions/",
            children: [
              "append_trailing_char_if_absent",
              "ascii",
              "bit_length",
              "char_length",
              "concat",
              "concat_ws",
              "ends_with",
              "find_in_set",
              "get_json_double",
              "get_json_int",
              "get_json_string",
              "group_concat",
              "instr",
              "lcase",
              "left",
              "length",
              "locate",
              "lower",
              "lpad",
              "ltrim",
              "money_format",
              "null_or_empty",
              "repeat",
              "reverse",
              "right",
              "rpad",
              "split_part",
              "starts_with",
              "strleft",
              "strright",
              {
                title: "fuzzy match",
                directoryPath: "like/",
                children: [
                  "like",
                  "not_like",
                ],
              },
              {
                title: "regular match",
                directoryPath: "regexp/",
                children: [
                  "regexp",
                  "regexp_extract",
                  "regexp_replace",
                  "not_regexp",
                ],
              },
            ],
          },
          {
            title: "Aggregate Functions",
            directoryPath: "aggregate-functions/",
            children: [
              "approx_count_distinct",
              "avg",
              "bitmap_union",
              "count",
              "hll_union_agg",
              "max",
              "min",
              "percentile_approx",
              "stddev",
              "stddev_samp",
              "sum",
              "topn",
              "var_samp",
              "variance",
            ],
          },
          {
            title: "bitmap functions",
            directoryPath: "bitmap-functions/",
            children: [
              "bitmap_and",
              "bitmap_contains",
              "bitmap_empty",
              "bitmap_from_string",
              "bitmap_has_any",
              "bitmap_hash",
              "bitmap_intersect",
              "bitmap_or",
              "bitmap_xor",
              "bitmap_not",
              "bitmap_to_string",
              "bitmap_union",
              "bitmap_xor",
              "to_bitmap",
            ],
          },
          {
            title: "Hash Functions",
            directoryPath: "hash-functions/",
            children: ["murmur_hash3_32"],
          },
          "window-function",
          "cast",
          "digital-masking",
        ],
      },
      {
        title: "DDL Statements",
        directoryPath: "sql-statements/",
        children: [
          {
            title: "Account Management",
            directoryPath: "Account Management/",
            children: [
              "CREATE ROLE",
              "CREATE USER",
              "DROP ROLE",
              "DROP USER",
              "GRANT",
              "REVOKE",
              "SET PASSWORD",
              "SET PROPERTY",
              "SHOW GRANTS",
              "SHOW ROLES",
            ],
          },
          {
            title: "Administration",
            directoryPath: "Administration/",
            children: [
              "ADMIN CANCEL REPAIR",
              "ADMIN CLEAN TRASH",
              "ADMIN CHECK TABLET",
              "ADMIN REPAIR",
              "ADMIN SET CONFIG",
              "ADMIN SET REPLICA STATUS",
              "ADMIN SHOW CONFIG",
              "ADMIN SHOW REPLICA DISTRIBUTION",
              "ADMIN SHOW REPLICA STATUS",
              "ADMIN-SHOW-DATA-SKEW",
              "ALTER CLUSTER",
              "ALTER SYSTEM",
              "CANCEL DECOMMISSION",
              "CREATE CLUSTER",
              "CREATE FILE",
              "DROP CLUSTER",
              "DROP FILE",
              "ENTER",
              "INSTALL PLUGIN",
              "LINK DATABASE",
              "MIGRATE DATABASE",
              "SET LDAP_ADMIN_PASSWORD",
              "SHOW BACKENDS",
              "SHOW BROKER",
              "SHOW FILE",
              "SHOW FRONTENDS",
              "SHOW FULL COLUMNS",
              "SHOW INDEX",
              "SHOW MIGRATIONS",
              "SHOW PLUGINS",
              "SHOW TABLE STATUS",
              "UNINSTALL PLUGIN",
            ],
          },
          {
            title: "Data Definition",
            directoryPath: "Data Definition/",
            children: [
              "ALTER DATABASE",
              "ALTER TABLE",
              "ALTER VIEW",
              "BACKUP",
              "CANCEL ALTER",
              "CANCEL BACKUP",
              "CREATE ENCRYPTKEY",
              "CANCEL RESTORE",
              "CREATE DATABASE",
              "CREATE INDEX",
              "CREATE MATERIALIZED VIEW",
              "CREATE REPOSITORY",
              "CREATE TABLE LIKE",
              "CREATE TABLE",
              "CREATE VIEW",
              "Colocate Join",
              "DROP DATABASE",
              "DROP ENCRYPTKEY",
              "DROP INDEX",
              "DROP MATERIALIZED VIEW",
              "DROP REPOSITORY",
              "DROP TABLE",
              "DROP VIEW",
              "HLL",
              "RECOVER",
              "RESTORE",
              "SHOW ENCRYPTKEYS",
              "TRUNCATE TABLE",
              "create-function",
              "drop-function",
              "show-functions",
            ],
          },
          {
            title: "Data Manipulation",
            directoryPath: "Data Manipulation/",
            children: [
              "BROKER LOAD",
              "CANCEL DELETE",
              "CANCEL LABEL",
              "CANCEL LOAD",
              "DELETE",
              "EXPORT",
              "GET LABEL STATE",
              "GROUP BY",
              "LOAD",
              "MINI LOAD",
              "MULTI LOAD",
              "PAUSE ROUTINE LOAD",
              "RESTORE TABLET",
              "RESUME ROUTINE LOAD",
              "ROUTINE LOAD",
              "SHOW ALTER",
              "SHOW BACKUP",
              "SHOW CREATE FUNCTION",
              "SHOW CREATE ROUTINE LOAD",
              "SHOW DATA",
              "SHOW DATABASES",
              "SHOW DELETE",
              "SHOW DYNAMIC PARTITION TABLES",
              "SHOW EXPORT",
              "SHOW LOAD",
              "SHOW PARTITIONS",
              "SHOW PROPERTY",
              "SHOW REPOSITORIES",
              "SHOW RESTORE",
              "SHOW ROUTINE LOAD TASK",
              "SHOW ROUTINE LOAD",
              "SHOW SNAPSHOT",
              "SHOW TABLES",
              "SHOW TABLET",
              "SHOW TRANSACTION",
              "STOP ROUTINE LOAD",
              "STREAM LOAD",
              "alter-routine-load",
              "insert",
              "UPDATE",
            ],
          },
          {
            title: "Data Types",
            directoryPath: "Data Types/",
            children: [
              "BIGINT",
              "BITMAP",
              "BOOLEAN",
              "CHAR",
              "DATE",
              "DATETIME",
              "DECIMAL",
              "DOUBLE",
              "FLOAT",
              "HLL",
              "INT",
              "SMALLINT",
              "TINYINT",
              "VARCHAR",
            ],
          },
          {
            title: "Utility",
            directoryPath: "Utility/",
            children: ["util_stmt"],
          },
        ],
      },
    ],
  },
  {
    title: "Developer Guide",
    directoryPath: "developer-guide/",
    children: [
        "debug-tool",
        "fe-eclipse-dev",
        "fe-idea-dev",
        "be-vscode-dev",		
        "java-format-code",
        "cpp-format-code",
    ],
  },
  {
    title: "Apache Community",
    directoryPath: "community/",
    children: [
      "members",
      "gitter",
      "subscribe-mail-list",
      "feedback",
      "how-to-contribute",
      "committer-guide",
      "pull-request",
      "release-process",
      "verify-apache-release",
    ],
  },
]
