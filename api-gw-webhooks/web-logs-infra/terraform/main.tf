// ================================
// Variables
// ================================
variable "region" {
  description = "AWS region"
}

variable "account" {
  description = "AWS account ID"
}

variable "stream_name" {
  description = "Kinesis Firehose delivery stream name"
}

variable "subnet_ids" {
  description = "List of subnet IDs for ECS Fargate tasks (exactly 2 subnets required)"
  type        = list(string)
}

variable "connector_image_repo" {
  description = "ECR repository for the connector image"
  type        = string
}

variable "connector_image_name" {
  description = "Name of the connector image"
  type        = string
}

variable "security_group_ids" {
  description = "List of security group IDs for ECS Fargate tasks (at least 1 required)"
  type        = list(string)
}

locals {
  connector_image = "${var.account}.dkr.ecr.${var.region}.amazonaws.com/${var.connector_image_name}:latest"
}

// ================================
// S3 Bucket for Firehose
// ================================
resource "aws_s3_bucket" "firehose_bucket" {
  bucket = "${var.stream_name}-bucket-${var.account}"
}

# Enable ACL
resource "aws_s3_bucket_ownership_controls" "bucket_ownership" {
  bucket = aws_s3_bucket.firehose_bucket.id
  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_acl" "bucket_acl" {
  depends_on = [aws_s3_bucket_ownership_controls.bucket_ownership]
  bucket = aws_s3_bucket.firehose_bucket.id
  acl    = "private"
}

resource "aws_s3_bucket_server_side_encryption_configuration" "bucket_encryption" {
  bucket = aws_s3_bucket.firehose_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "bucket_lifecycle" {
  bucket = aws_s3_bucket.firehose_bucket.id

  rule {
    id     = "cleanup-old"
    status = "Enabled"

    filter {
      prefix = ""  # Apply to all objects
    }

    expiration {
      days = 30
    }
  }
}

// ================================
// IAM Role for Firehose
// ================================
resource "aws_iam_role" "firehose_role" {
  name = "firehose_web_delivery_role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "firehose_s3_policy" {
  name = "firehose-web-s3-access"
  role = aws_iam_role.firehose_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "s3:PutObject",
          "s3:PutObjectAcl",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.firehose_bucket.arn,
          "${aws_s3_bucket.firehose_bucket.arn}/*"
        ]
      }
    ]
  })
}

// ================================
// Kinesis Firehose Delivery Stream
// ================================
resource "aws_kinesis_firehose_delivery_stream" "web_firehose_stream" {
  name        = var.stream_name
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = aws_s3_bucket.firehose_bucket.arn
    buffering_size     = 4
    buffering_interval = 200
    prefix             = "logs/"
  }
}

// ================================
// IAM Role for API Gateway
// ================================
resource "aws_iam_role" "apigw_firehose_role" {
  name = "web-apigw-firehose-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "apigateway.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "apigw_firehose_policy" {
  name   = "web-apigw-firehose-perms"
  role   = aws_iam_role.apigw_firehose_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow",
      Action   = ["firehose:PutRecord","firehose:PutRecordBatch"],
      Resource = aws_kinesis_firehose_delivery_stream.web_firehose_stream.arn
    }]
  })
}

// ================================
// HTTP API Gateway with GET /webhook
// ================================
resource "aws_apigatewayv2_api" "http_api" {
  name          = "events-api"
  protocol_type = "HTTP"

  cors_configuration {
    allow_origins = ["*"]
    allow_methods = ["GET"]
  }
}

// ================================
// Lambda Proxy for API Gateway to Firehose
// ================================
resource "aws_iam_role" "lambda_role" {
  name = "web-api-firehose-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "lambda_firehose_policy" {
  name = "web-logs-lambda-firehose-policy"
  role = aws_iam_role.lambda_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "firehose:PutRecord",
        "firehose:PutRecordBatch"
      ]
      Resource = [aws_kinesis_firehose_delivery_stream.web_firehose_stream.arn]
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "proxy" {
  filename         = "../proxy/index.zip"
  function_name    = "web-logs-firehose-proxy"
  role            = aws_iam_role.lambda_role.arn
  handler         = "index.handler"
  runtime         = "nodejs18.x"
  
  environment {
    variables = {
      DELIVERY_STREAM = aws_kinesis_firehose_delivery_stream.web_firehose_stream.name
    }
  }
}

resource "aws_lambda_permission" "api_gw" {
  statement_id  = "AllowAPIGatewayInvoke"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.proxy.function_name
  principal     = "apigateway.amazonaws.com"
  source_arn    = "${aws_apigatewayv2_api.http_api.execution_arn}/*/*/webhook"
}

// Update API Gateway integration to use Lambda proxy
resource "aws_apigatewayv2_integration" "firehose" {
  api_id                 = aws_apigatewayv2_api.http_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = aws_lambda_function.proxy.invoke_arn
  integration_method     = "POST"  # Required for Lambda proxy integration
  payload_format_version = "2.0"
  description            = "Lambda proxy integration for Firehose"
}

resource "aws_apigatewayv2_route" "webhook" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "GET /webhook"  # This defines the public HTTP method
  target    = "integrations/${aws_apigatewayv2_integration.firehose.id}"
}

resource "aws_apigatewayv2_stage" "produ" {
  api_id      = aws_apigatewayv2_api.http_api.id
  name        = "produ"
  auto_deploy = true
}

// ================================
// ECS Fargate Connector Service
// ================================
resource "aws_ecs_cluster" "connector_cluster" {
  name = "web-traffic-connector-cluster"
}

resource "aws_iam_role" "ecs_task_execution" {
  name = "web-ecsTaskExecutionRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}
resource "aws_iam_role_policy_attachment" "ecs_exec_attach" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

resource "aws_iam_role" "ecs_task_role" {
  name = "web-logs-ecsTaskRole"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ecs-tasks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}
resource "aws_iam_role_policy" "ecs_task_firehose" {
  name   = "web-logs-ecsTaskFirehosePolicy"
  role   = aws_iam_role.ecs_task_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow",
      Action   = ["firehose:PutRecord","firehose:PutRecordBatch"],
      Resource = aws_kinesis_firehose_delivery_stream.web_firehose_stream.arn
    }]
  })
}

resource "aws_cloudwatch_log_group" "connector" {
  name              = "/ecs/web-traffic-connector"
  retention_in_days = 14
}

resource "aws_ecs_task_definition" "connector" {
  family                   = "web-traffic-connector"
  network_mode             = "awsvpc"
  requires_compatibilities = ["FARGATE"]
  cpu                      = "256"
  memory                   = "512"
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ecs_task_role.arn

  container_definitions = jsonencode([{
    name      = "connector"
    image     = local.connector_image
    essential = true
    environment = [
      { name = "SOURCE_URL",  value = "http://18.203.232.58:8000/api/web-traffic/" },
      { name = "STREAM_NAME", value = var.stream_name },
      { name = "AWS_REGION",  value = var.region }
    ]
    logConfiguration = {
      logDriver = "awslogs"
      options = {
        awslogs-group         = aws_cloudwatch_log_group.connector.name
        awslogs-region        = var.region
        awslogs-stream-prefix = "ecs"
      }
    }
  }])
}

resource "aws_ecs_service" "connector_service" {
  name            = "web-traffic-connector"
  cluster         = aws_ecs_cluster.connector_cluster.id
  task_definition = aws_ecs_task_definition.connector.arn
  desired_count   = 1
  launch_type     = "FARGATE"

  network_configuration {
    subnets          = var.subnet_ids
    security_groups  = var.security_group_ids
    assign_public_ip = true
  }

  depends_on = [aws_iam_role_policy_attachment.ecs_exec_attach]
}

// ================================
// Outputs
// ================================
output "bucket_name" {
  value       = aws_s3_bucket.firehose_bucket.id
  description = "S3 bucket where Firehose deliveries land"
}

output "delivery_stream" {
  value       = aws_kinesis_firehose_delivery_stream.web_firehose_stream.name
  description = "Kinesis Firehose delivery stream name"
}

output "webhook_url" {
  description = "GET endpoint for the webhook"
  value       = "https://${aws_apigatewayv2_api.http_api.id}.execute-api.${var.region}.amazonaws.com/${aws_apigatewayv2_stage.produ.name}/webhook"
}
