output "sns_arn" {
  value = aws_sns_topic.sns.arn
}

output "sqs_arn" {
  value = aws_sqs_queue.sqs.arn
}

output "sqs_url" {
  value = aws_sqs_queue.sqs.id
}

# https://registry.terraform.io/providers/hashicorp/aws/latest/docs/resources/iam_access_key
# id - Access key ID.
output "key_id" {
  value  = aws_iam_access_key.test-key.id
}

# secret - Secret access key. Note that this will be written to the state file.
output "secret_key_id" {
  value  = aws_iam_access_key.test-key.secret
}