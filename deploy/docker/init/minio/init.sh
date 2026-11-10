#!/bin/sh
# Idempotent: creates the private cvs bucket and the least-privilege users
# candidate-svc (put/get/delete on cvs/*) and ai-svc (get on cvs/*). The root
# credentials are used only here.
set -eu

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
mc mb --ignore-existing local/cvs
mc anonymous set none local/cvs

# setup_user <policy name> <access key> <secret key>
setup_user() {
  mc admin policy create local "$1" "/init/$1.json"
  mc admin user add local "$2" "$3"
  # attach fails when the policy is already attached; accept that on re-runs.
  mc admin policy attach local "$1" --user "$2" \
    || mc admin user info local "$2" | grep -q "$1"
}

setup_user candidate-svc "$CANDIDATE_S3_ACCESS_KEY" "$CANDIDATE_S3_SECRET_KEY"
setup_user ai-svc "$AI_S3_ACCESS_KEY" "$AI_S3_SECRET_KEY"

echo "minio ready"
