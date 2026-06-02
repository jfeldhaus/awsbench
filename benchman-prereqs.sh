#!/usr/bin/env bash
#
# benchman-prereqs.sh
#
# Creates all AWS resources required before running the BenchMan controller.
# Safe to re-run — existing resources are detected and left unchanged.
#
# Resources created:
#   - ECR repository
#   - ECS cluster
#   - CloudWatch log group
#   - IAM role: ecsTaskExecutionRole  (ECS pulls images, writes logs)
#   - IAM role: benchman-task-role    (worker containers access SNS/SQS)
#   - iam:PassRole permission for the current AWS caller
#   - ECS task definition
#

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Configuration ──────────────────────────────────────────────────────────────

# Read values from benchman.properties
prop() {
    grep "^${1}=" "${SCRIPT_DIR}/benchman.properties" | cut -d= -f2- | tr -d ' \r'
}

REGION=$(prop benchman.region)
CLUSTER=$(prop benchman.cluster.name)
TASK_DEF=$(prop benchman.task.definition)
ECR_REPO_NAME=$(prop benchman.ecr.repo.name)

# ECS task CPU and memory (Fargate units: 1024 = 1 vCPU)
CPU=1024
MEMORY=2048

# ── Derived values ─────────────────────────────────────────────────────────────

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
IMAGE_URI="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_REPO_NAME}:latest"
TASK_ROLE="benchman-task-role"
EXEC_ROLE="ecsTaskExecutionRole"
LOG_GROUP="/ecs/${TASK_DEF}"

echo "Account  : ${ACCOUNT_ID}"
echo "Region   : ${REGION}"
echo "Cluster  : ${CLUSTER}"
echo "Task def : ${TASK_DEF}"
echo "Image    : ${IMAGE_URI}"
echo ""

# ── ECR repository ─────────────────────────────────────────────────────────────

echo "==> ECR repository: ${ECR_REPO_NAME}"
if aws ecr describe-repositories --repository-names "${ECR_REPO_NAME}" \
        --region "${REGION}" &>/dev/null; then
    echo "    Already exists."
else
    aws ecr create-repository --repository-name "${ECR_REPO_NAME}" \
        --region "${REGION}" > /dev/null
    echo "    Created."
fi

# ── ECS cluster ────────────────────────────────────────────────────────────────

echo "==> ECS cluster: ${CLUSTER}"
# create-cluster is idempotent — returns existing cluster if it already exists
aws ecs create-cluster --cluster-name "${CLUSTER}" \
    --region "${REGION}" > /dev/null
echo "    Done."

# ── CloudWatch log group ───────────────────────────────────────────────────────

echo "==> CloudWatch log group: ${LOG_GROUP}"
if aws logs describe-log-groups \
        --log-group-name-prefix "${LOG_GROUP}" \
        --query "logGroups[?logGroupName=='${LOG_GROUP}']" \
        --output text | grep -q .; then
    echo "    Already exists."
else
    aws logs create-log-group --log-group-name "${LOG_GROUP}" \
        --region "${REGION}"
    echo "    Created."
fi

# ── ECS task execution role ────────────────────────────────────────────────────
# Allows ECS to pull images from ECR and write logs to CloudWatch.

echo "==> IAM role: ${EXEC_ROLE}"
if aws iam get-role --role-name "${EXEC_ROLE}" &>/dev/null; then
    echo "    Already exists."
else
    aws iam create-role --role-name "${EXEC_ROLE}" \
        --assume-role-policy-document '{
          "Version": "2012-10-17",
          "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "ecs-tasks.amazonaws.com"},
            "Action": "sts:AssumeRole"
          }]
        }' > /dev/null
    echo "    Created."
fi
aws iam attach-role-policy --role-name "${EXEC_ROLE}" \
    --policy-arn arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy
echo "    Policy attached."

# ── BenchMan task role ─────────────────────────────────────────────────────────
# Assumed by worker containers — grants access to SNS and SQS.

echo "==> IAM role: ${TASK_ROLE}"
if aws iam get-role --role-name "${TASK_ROLE}" &>/dev/null; then
    echo "    Already exists."
else
    aws iam create-role --role-name "${TASK_ROLE}" \
        --assume-role-policy-document '{
          "Version": "2012-10-17",
          "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "ecs-tasks.amazonaws.com"},
            "Action": "sts:AssumeRole"
          }]
        }' > /dev/null
    echo "    Created."
fi
aws iam put-role-policy --role-name "${TASK_ROLE}" \
    --policy-name benchman-task-policy \
    --policy-document '{
      "Version": "2012-10-17",
      "Statement": [{
        "Effect": "Allow",
        "Action": [
          "sns:CreateTopic",
          "sns:Subscribe", "sns:Unsubscribe", "sns:Receive",
          "sqs:CreateQueue", "sqs:DeleteQueue", "sqs:SendMessage",
          "sqs:ReceiveMessage", "sqs:DeleteMessage",
          "sqs:GetQueueUrl", "sqs:GetQueueAttributes", "sqs:SetQueueAttributes"
        ],
        "Resource": "*"
      }]
    }'
echo "    Policy updated."

# ── iam:PassRole for the current caller ────────────────────────────────────────
# The process running the controller must be allowed to pass both roles to ECS.

echo "==> iam:PassRole for current caller"
CALLER_ARN=$(aws sts get-caller-identity --query Arn --output text)

PASS_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": "iam:PassRole",
    "Resource": [
      "arn:aws:iam::${ACCOUNT_ID}:role/${TASK_ROLE}",
      "arn:aws:iam::${ACCOUNT_ID}:role/${EXEC_ROLE}"
    ]
  }]
}
EOF
)

if [[ "${CALLER_ARN}" == *":user/"* ]]; then
    USER_NAME="${CALLER_ARN##*/}"
    aws iam put-user-policy --user-name "${USER_NAME}" \
        --policy-name benchman-pass-role \
        --policy-document "${PASS_POLICY}"
    echo "    Added to IAM user: ${USER_NAME}"
elif [[ "${CALLER_ARN}" == *":assumed-role/"* ]]; then
    ROLE_NAME=$(echo "${CALLER_ARN}" | cut -d/ -f2)
    aws iam put-role-policy --role-name "${ROLE_NAME}" \
        --policy-name benchman-pass-role \
        --policy-document "${PASS_POLICY}"
    echo "    Added to IAM role: ${ROLE_NAME}"
else
    echo "    WARNING: unrecognised caller ARN: ${CALLER_ARN}"
    echo "    Manually grant iam:PassRole on ${TASK_ROLE} and ${EXEC_ROLE}."
fi

# ── ECS task definition ────────────────────────────────────────────────────────

echo "==> ECS task definition: ${TASK_DEF}"

CONTAINER_DEF=$(cat <<EOF
[{
  "name": "${TASK_DEF}",
  "image": "${IMAGE_URI}",
  "essential": true,
  "logConfiguration": {
    "logDriver": "awslogs",
    "options": {
      "awslogs-group": "${LOG_GROUP}",
      "awslogs-region": "${REGION}",
      "awslogs-stream-prefix": "ecs"
    }
  }
}]
EOF
)

aws ecs register-task-definition \
    --family "${TASK_DEF}" \
    --network-mode awsvpc \
    --requires-compatibilities FARGATE \
    --cpu "${CPU}" \
    --memory "${MEMORY}" \
    --task-role-arn "arn:aws:iam::${ACCOUNT_ID}:role/${TASK_ROLE}" \
    --execution-role-arn "arn:aws:iam::${ACCOUNT_ID}:role/${EXEC_ROLE}" \
    --container-definitions "${CONTAINER_DEF}" \
    --region "${REGION}" > /dev/null
echo "    Registered."

# ──────────────────────────────────────────────────────────────────────────────

echo ""
echo "All prerequisites are in place."
echo "Build and push the Docker image (docker/build.sh && docker/push.sh),"
echo "then run the controller (./run-control.sh)."
