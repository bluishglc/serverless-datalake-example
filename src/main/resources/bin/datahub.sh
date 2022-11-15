#!/usr/bin/env bash

# run me on datahub server!

# if datahub provide a rest api to post the lineage yaml file, we can register metadata
# from local, the sdl deployment host, but by now, it has no such feature, so we have to
# run this scripts on datahub server.

# technically, I think detached & centralized metadata registering could be a dedicated
# maven module accompanied with etl project, i.e., sdl-metadata, it can be packaged independently
# and deployed to datahub server, then run cli to create/update metadata of the accompanied etl project

tee /tmp/lineage.yml <<EOF
---
version: 1
lineage:
  #
  # --------------------------------------------- layer: dmt, domain: tlc ----------------------------------------------
  #
  # dmt.tlc_wide_trip << [ dmt.tlc_union_trip, dmt.geo_dim_taxi_zone, dmt.pub_dim_date ]
  - entity:
      name: dmt.tlc_wide_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: dmt.tlc_union_trip
          type: dataset
          env: PROD
          platform: glue
      - entity:
          name: dmt.geo_dim_taxi_zone
          type: dataset
          env: PROD
          platform: glue
      - entity:
          name: dmt.pub_dim_date
          type: dataset
          env: PROD
          platform: glue
  # dmt.tlc_union_trip << [ dwh.tlc_yellow_trip, dwh.tlc_green_trip, dwh.tlc_fhv_trip, dwh.tlc_fhvhv_trip ]
  - entity:
      name: dmt.tlc_union_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: dwh.tlc_yellow_trip
          type: dataset
          env: PROD
          platform: glue
      - entity:
          name: dwh.tlc_green_trip
          type: dataset
          env: PROD
          platform: glue
      - entity:
          name: dwh.tlc_fhv_trip
          type: dataset
          env: PROD
          platform: glue
      - entity:
          name: dwh.tlc_fhvhv_trip
          type: dataset
          env: PROD
          platform: glue
  #
  # --------------------------------------------- layer: dmt, domain: geo ----------------------------------------------
  #
  # dmt.geo_dim_taxi_zone << [ dwh.geo_taxi_zone ]
  - entity:
      name: dmt.geo_dim_taxi_zone
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: dwh.geo_taxi_zone
          type: dataset
          env: PROD
          platform: glue
  #
  # --------------------------------------------- layer: dmt, domain: pub ----------------------------------------------
  #
  # dmt.pub_dim_date << [ stg.pub_dim_date ]
  - entity:
      name: dmt.pub_dim_date
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: stg.pub_dim_date
          type: dataset
          env: PROD
          platform: glue
  #
  # --------------------------------------------- layer: dwh, domain: tlc ----------------------------------------------
  #
  # dwh.tlc_yellow_trip << ods.tlc_yellow_trip
  - entity:
      name: dwh.tlc_yellow_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: ods.tlc_yellow_trip
          type: dataset
          env: PROD
          platform: glue
  # dwh.tlc_green_trip << [ ods.tlc_green_trip ]
  - entity:
      name: dwh.tlc_green_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: ods.tlc_green_trip
          type: dataset
          env: PROD
          platform: glue
  # dwh.tlc_fhv_trip << [ ods.tlc_fhv_trip ]
  - entity:
      name: dwh.tlc_fhv_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: ods.tlc_fhv_trip
          type: dataset
          env: PROD
          platform: glue
  # dwh.tlc_fhvhv_trip << [ ods.tlc_fhvhv_trip ]
  - entity:
      name: dwh.tlc_fhvhv_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: ods.tlc_fhvhv_trip
          type: dataset
          env: PROD
          platform: glue
  #
  # --------------------------------------------- layer: dwh, domain: geo ----------------------------------------------
  #
  # dwh.geo_taxi_zone << [ ods.geo_taxi_zone ]
  - entity:
      name: dwh.geo_taxi_zone
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: ods.geo_taxi_zone
          type: dataset
          env: PROD
          platform: glue
  #
  # --------------------------------------------- layer: ods, domain: tlc ----------------------------------------------
  #
  # ods.tlc_yellow_trip << stg.tlc_yellow_trip
  - entity:
      name: ods.tlc_yellow_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: stg.tlc_yellow_trip
          type: dataset
          env: PROD
          platform: glue
  # ods.tlc_green_trip << [ stg.tlc_green_trip ]
  - entity:
      name: ods.tlc_green_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: stg.tlc_green_trip
          type: dataset
          env: PROD
          platform: glue
  # ods.tlc_fhv_trip << [ stg.tlc_fhv_trip ]
  - entity:
      name: ods.tlc_fhv_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: stg.tlc_fhv_trip
          type: dataset
          env: PROD
          platform: glue
  # ods.tlc_fhvhv_trip << [ stg.tlc_fhvhv_trip ]
  - entity:
      name: ods.tlc_fhvhv_trip
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: stg.tlc_fhvhv_trip
          type: dataset
          env: PROD
          platform: glue
  #
  # --------------------------------------------- layer: ods, domain: geo ----------------------------------------------
  #
  # dwh.geo_taxi_zone << [ stg.geo_taxi_zone ]
  - entity:
      name: ods.geo_taxi_zone
      type: dataset
      env: PROD
      platform: glue
    upstream:
      - entity:
          name: stg.geo_taxi_zone
          type: dataset
          env: PROD
          platform: glue
EOF

tee /tmp/datahub.yml <<EOF
source:
  type: datahub-lineage-file
  config:
    # Coordinates
    file: /tmp/lineage.yml
    # Whether we want to query datahub-gms for upstream data
    preserve_upstream: False
EOF

datahub ingest -c /tmp/datahub.yml