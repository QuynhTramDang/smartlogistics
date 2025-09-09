#!/bin/bash
set -e

# Ghi đè file cấu hình pg_hba.conf
cp /docker-entrypoint-initdb.d/hive_pg_hba.conf /var/lib/postgresql/data/pg_hba.conf
