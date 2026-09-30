#!/bin/sh
set -e

COORDINATOR_HOSTPORT="${COORDINATOR_HOSTPORT:-coordinator:8000}"

envsubst '${COORDINATOR_HOSTPORT}' < /etc/nginx/conf.d/default.conf.template > /etc/nginx/conf.d/default.conf

exec nginx -g "daemon off;"
