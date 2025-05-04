// ================================
// Variables
// ================================
variable "region" {
  description = "AWS region"
  default     = "eu-west-1"
}

variable "account" {
  description = "AWS account ID"
  default     = "405894843300"
}

variable "stream_name" {
  description = "Kinesis Firehose delivery stream name"
  default     = "shopware-web-logs-firehose"
}

variable "subnet_ids" {
  description = "List of subnet IDs for ECS Fargate tasks"
  type        = list(string)
}

variable "security_group_ids" {
  description = "List of security group IDs for ECS Fargate tasks"
  type        = list(string)
}

variable "connector_image" {
  description = "ECR image URI for the web-traffic connector service"
  default     = "123456789012.dkr.ecr.${var.region}.amazonaws.com/web-traffic-connector:latest"
}

// ================================
// 0. S3 Bucket for Firehose
// ================================
resource "aws_s3_bucket" "firehose_bucket" {
  bucket = "${var.stream_name}-bucket-${var.account}"
  acl    = "private"

  server_side_encryption_configuration {
    rule {
      apply_server_side_encryption_by_default {
        sse_algorithm = "AES256"
      }
    }
  }

  lifecycle_rule {
    id      = "cleanup-old"
    enabled = true
    expiration {
      days = 30
    }
  }
}

// ================================
// IAM Role for Firehose
// ================================
resource "aws_iam_role" "firehose_role" {
  name = "firehose_delivery_role"
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
  name = "firehose-s3-access"
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
resource "aws_kinesis_firehose_delivery_stream" "firehose_stream" {
  name        = var.stream_name
  destination = "s3"

  s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = aws_s3_bucket.firehose_bucket.arn
    buffer_size        = 5    # MB before flush
    buffer_interval    = 300  # seconds before flush
    compression_format = "GZIP"
    prefix             = "logs/"
  }
}

// ================================
//  Role for API Gateway
// ================================
resource "aws_iam_role" "apigw_firehose_role" {
  name = "apigw-firehose-role"
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
  name   = "apigw-firehose-perms"
  role   = aws_iam_role.apigw_firehose_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow",
      Action   = ["firehose:PutRecord","firehose:PutRecordBatch"],
      Resource = aws_kinesis_firehose_delivery_stream.firehose_stream.arn
    }]
  })
}

// ================================
// HTTP API Gateway with GET /webhook
// ================================
resource "aws_apigatewayv2_api" "http_api" {
  name          = "events-api"
  protocol_type = "HTTP"
  policy        = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = "*"
      Action    = "execute-api:Invoke"
      Resource  = "arn:aws:execute-api:${var.region}:${var.account}:${aws_apigatewayv2_api.http_api.id}/*/GET/webhook"
      Condition = { IpAddress = {"aws:SourceIp" = ["203.0.113.0/24","198.51.100.0/24"]} }
    }]
  })
}

resource "aws_apigatewayv2_integration" "firehose" {
  api_id                 = aws_apigatewayv2_api.http_api.id
  integration_type       = "AWS_PROXY"
  integration_uri        = "arn:aws:apigateway:${var.region}:firehose:action/PutRecord"
  credentials_arn        = aws_iam_role.apigw_firehose_role.arn
  payload_format_version = "1.0"

  request_templates = {
    "*/*" = <<-EOF
      #set($json = $util.toJson($input.params().querystring))
      {
        "DeliveryStreamName": "${var.stream_name}",
        "Record": {"Data": "$util.base64Encode($json)"}
      }
    EOF
  }
  request_parameters = {"integration.request.header.Content-Type" = "'application/x-amz-json-1.1'"}
}

resource "aws_apigatewayv2_route" "webhook" {
  api_id    = aws_apigatewayv2_api.http_api.id
  route_key = "GET /webhook"
  target    = "integrations/${aws_apigatewayv2_integration.firehose.id}"
}

resource "aws_apigatewayv2_stage" "prod" {
  api_id      = aws_apigatewayv2_api.http_api.id
  name        = "prod"
  auto_deploy = true
}

// ================================
// ECS Fargate Connector Service
// ================================
resource "aws_ecs_cluster" "connector_cluster" {
  name = "web-traffic-connector-cluster"
}

resource "aws_iam_role" "ecs_task_execution" {
  name = "ecsTaskExecutionRole"
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
  name = "ecsTaskRole"
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
  name   = "ecsTaskFirehosePolicy"
  role   = aws_iam_role.ecs_task_role.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow",
      Action   = ["firehose:PutRecord","firehose:PutRecordBatch"],
      Resource = aws_kinesis_firehose_delivery_stream.firehose_stream.arn
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
    image     = var.connector_image
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
  value       = aws_kinesis_firehose_delivery_stream.firehose_stream.name
  description = "Kinesis Firehose delivery stream name"
}

output "webhook_url" {
  description = "GET endpoint for the webhook"
  value       = "https://${aws_apigatewayv2_api.http_api.id}.execute-api.${var.region}.amazonaws.com/${aws_apigatewayv2_stage.prod.name}/webhook"
}
