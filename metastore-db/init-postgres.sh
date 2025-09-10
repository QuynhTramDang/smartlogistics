#!/bin/bash
set -e

cp /docker-entrypoint-initdb.d/hive_pg_hba.conf /var/lib/postgresql/data/pg_hba.conf
