region = "eu-west-1"
account = "405894843300"
stream_name = "crm-logs-firehose"
subnet_ids = [
  "subnet-0f1526aaff9aec36e",  # Replace with your actual subnet ID
  "subnet-0716be2d6750527e2"   # Replace with your actual subnet ID
]

security_group_ids = [
  "sg-05d6900c1579c95e2"       # Replace with your actual security group ID
]

connector_image_name = "crm-logs-ecs"