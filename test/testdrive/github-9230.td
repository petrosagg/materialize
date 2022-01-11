# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

$ set schema=[
    {
      "type": "array",
      "items": {
        "type": "record",
        "name": "update",
        "namespace": "com.materialize.cdc",
        "fields": [
          {
            "name": "data",
            "type": {
              "type": "record",
              "name": "data",
              "fields": [
                {
                  "name": "id",
                  "type": "long"
                },
                {
                  "name": "price",
                  "type": [
                    "null",
                    "int"
                  ]
                }
              ]
            }
          },
          {
            "name": "time",
            "type": "long"
          },
          {
            "name": "diff",
            "type": "long"
          }
        ]
      }
    },
    {
      "type": "record",
      "name": "progress",
      "namespace": "com.materialize.cdc",
      "fields": [
        {
          "name": "lower",
          "type": {
            "type": "array",
            "items": "long"
          }
        },
        {
          "name": "upper",
          "type": {
            "type": "array",
            "items": "long"
          }
        },
        {
          "name": "counts",
          "type": {
            "type": "array",
            "items": {
              "type": "record",
              "name": "counts",
              "fields": [
                {
                  "name": "time",
                  "type": "long"
                },
                {
                  "name": "count",
                  "type": "long"
                }
              ]
            }
          }
        }
      ]
    }
  ]

$ kafka-create-topic topic=data

$ kafka-ingest format=avro topic=data schema=${schema} timestamp=1 publish=true
{"array":[{"data":{"id":0,"price":{"int":10}},"time":5,"diff":1}]}
{"array":[{"data":{"id":1,"price":{"int":12}},"time":4,"diff":1}]}
{"com.materialize.cdc.progress":{"lower":[0],"upper":[3],"counts":[]}}
{"com.materialize.cdc.progress":{"lower":[3],"upper":[10],"counts":[{"time":4,"count":1},{"time":5,"count":1}]}}

> CREATE SOURCE data
  FROM KAFKA BROKER '${testdrive.kafka-addr}' TOPIC 'testdrive-data-${testdrive.seed}'
  FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY '${testdrive.schema-registry-url}'
  ENVELOPE MATERIALIZE

> CREATE MATERIALIZED VIEW data_view AS (SELECT * FROM data)

> SELECT * FROM data_view
0 10
1 12
