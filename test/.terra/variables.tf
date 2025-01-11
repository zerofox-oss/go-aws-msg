variable "sns_topic_name" {
  type = string
  default = "test-sns"
}

variable "sqs_topic_name" {
  type = string
  default = "test-sqs"
}

variable "iam_partition" {
  default = "aws"
}

variable "aws_region" {
  default = "us-west-2"
}
