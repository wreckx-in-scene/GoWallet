#!/bin/bash
set -e

for svc in auth user wallet payment ledger fraud notification; do
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<EOSQL
CREATE ROLE "${svc}" LOGIN PASSWORD '${svc}';
CREATE DATABASE "${svc}_db" OWNER "${svc}";
REVOKE CONNECT ON DATABASE "${svc}_db" FROM PUBLIC;
EOSQL
done