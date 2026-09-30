#!/bin/sh
set -e

if [ -n "${NODE_1_HOSTPORT:-}" ]; then
  export STORAGE_NODES="${NODE_1_HOSTPORT},${NODE_2_HOSTPORT},${NODE_3_HOSTPORT},${NODE_4_HOSTPORT},${NODE_5_HOSTPORT},${NODE_6_HOSTPORT}"
fi

exec uvicorn app.main:app --host 0.0.0.0 --port "${PORT:-8000}"
