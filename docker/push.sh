#!/usr/bin/env bash
set -euo pipefail
set -x



ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1
REPO_URI="200214900248.dkr.ecr.us-east-1.amazonaws.com/aws/awsbench"

aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin "${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
docker tag awsbench:latest "${REPO_URI}:latest"
docker push "${REPO_URI}:latest"


