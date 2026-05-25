#!/bin/sh
# Health check script used by Dockerfile HEALTHCHECK instruction.
# Checks that the /healthz endpoint responds successfully.
exec wget -q -O /dev/null "http://localhost:${CAS_PORT:-8080}/healthz"
