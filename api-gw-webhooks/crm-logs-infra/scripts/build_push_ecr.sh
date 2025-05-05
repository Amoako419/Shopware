#!/bin/bash

# === CONFIGURATION ===
AWS_REGION="eu-west-1"
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REPO_NAME="crm-logs-ecs"
IMAGE_TAG="latest"
DOCKERFILE_PATH="$(dirname $0)/../connector"

# === SCRIPT START ===
set -e

echo "🛠  Logging into ECR..."
aws ecr get-login-password --region $AWS_REGION | docker login --username AWS --password-stdin "$AWS_ACCOUNT_ID.dkr.ecr.$AWS_REGION.amazonaws.com"

# Create the repository if it doesn't exist
echo "📦  Ensuring ECR repository exists..."
aws ecr describe-repositories --repository-names $ECR_REPO_NAME --region $AWS_REGION >/dev/null 2>&1 || \
aws ecr create-repository --repository-name $ECR_REPO_NAME --region $AWS_REGION

# Build and tag the image
echo "🐳  Building Docker image..."
docker build -t $ECR_REPO_NAME:$IMAGE_TAG -f $DOCKERFILE_PATH/Dockerfile $DOCKERFILE_PATH

# Tag with full ECR path
ECR_IMAGE_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO_NAME}:${IMAGE_TAG}"
docker tag $ECR_REPO_NAME:$IMAGE_TAG $ECR_IMAGE_URI

# Push to ECR
echo "🚀  Pushing image to ECR: $ECR_IMAGE_URI"
docker push $ECR_IMAGE_URI

echo "✅  Done! Image available at: $ECR_IMAGE_URI"
