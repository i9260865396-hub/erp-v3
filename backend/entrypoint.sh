#!/bin/sh
set -e

echo "Waiting for Postgres at db:5432..."
until pg_isready -h db -p 5432 -U erp -d erp >/dev/null 2>&1; do
  sleep 1
done

echo "Postgres is ready. Starting API..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
