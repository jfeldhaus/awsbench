#!/usr/bin/env bash
set -euo pipefail
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROPS="${SCRIPT_DIR}/../benchman.properties"

prop() {
    grep "^${1}=" "${PROPS}" | cut -d= -f2- | tr -d ' \r'
}

# Read credentials from ~/.aws/credentials for the active profile.
# AWS_PROFILE is honoured if set; falls back to [default].
AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key)
AWS_SESSION_TOKEN=$(aws configure get aws_session_token 2>/dev/null || true)

if [[ -z "${AWS_ACCESS_KEY_ID}" || -z "${AWS_SECRET_ACCESS_KEY}" ]]; then
    echo "ERROR: could not read AWS credentials from ~/.aws/credentials" >&2
    exit 1
fi

# Read local image name and properties
ECR_REPO_NAME=$(prop benchman.ecr.repo.name)
LOCAL_IMAGE="awsbench:latest"

# Pass session token only when present (temporary credentials)
SESSION_TOKEN_ARG=""
if [[ -n "${AWS_SESSION_TOKEN}" ]]; then
    SESSION_TOKEN_ARG="-e AWS_SESSION_TOKEN=${AWS_SESSION_TOKEN}"
fi

docker run \
    -e AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID}" \
    -e AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY}" \
    ${SESSION_TOKEN_ARG} \
    "${LOCAL_IMAGE}"
