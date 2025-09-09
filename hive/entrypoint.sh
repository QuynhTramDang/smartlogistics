#!/bin/bash
set -e

if [ "$INIT_SCHEMA" = "true" ]; then
  echo ">> Initializing Hive metastore schema..."
  /opt/hive/bin/schematool -dbType postgres -initSchema --verbose
fi

echo ">> Starting Hive Metastore service..."
exec /opt/hive/bin/hive --service metastore
