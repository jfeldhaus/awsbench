#!/usr/bin/env bash
set -euo pipefail
set -x

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROPS="${SCRIPT_DIR}/../benchman.properties"

prop() {
    grep "^${1}=" "${PROPS}" | cut -d= -f2- | tr -d ' \r'
}

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=$(prop benchman.region)
ECR_REPO_NAME=$(prop benchman.ecr.repo.name)
REPO_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO_NAME}"

aws ecr get-login-password --region "${REGION}" \
    | docker login --username AWS --password-stdin \
        "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"

LOCAL_IMAGE="awsbench:latest"   # produced by docker/build.sh
docker tag "${LOCAL_IMAGE}" "${REPO_URI}:latest"
docker push "${REPO_URI}:latest"
