#!/bin/bash
set -e

cd osrm-data
for pbf in *.osm.pbf; do
  base="${pbf%.osm.pbf}"
  echo "  Building $base ..."
  PBF_FILE="$pbf" OSRM_BASE="$base" ALGO=mld PROFILE=/opt/car.lua \
    docker compose run --rm osrm-prep
done
