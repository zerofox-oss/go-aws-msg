data "aws_caller_identity" "current" {
}

# SNS
resource "aws_sns_topic" "sns" {
  name = var.sns_topic_name
}

# policy that allows our AWS account to publish into this topic
resource "aws_sns_topic_policy" "sns-policy" {
  arn = aws_sns_topic.sns.arn

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "AWS": "*"
      },
      "Action": [
        "SNS:Publish"
      ],
      "Resource": "${aws_sns_topic.sns.arn}",
      "Condition": {
        "StringEquals": {
          "AWS:SourceOwner": "${data.aws_caller_identity.current.account_id}"
        }
      }
    }
  ]
}
POLICY

}

# SQS
resource "aws_sqs_queue" "sqs" {
  name                       = var.sqs_topic_name
  message_retention_seconds  = 3600
  visibility_timeout_seconds = 300
  delay_seconds              = 0
  redrive_policy             = ""
}

# policy for the subscription to work: it allows the SNS topic we defined above to write into this SQS
resource "aws_sqs_queue_policy" "sqs-policy" {
  queue_url = aws_sqs_queue.sqs.id

  policy = <<POLICY
{
  "Version": "2012-10-17",
  "Id": "sqspolicy",
  "Statement": [
    {
      "Sid": "First",
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sqs:SendMessage",
      "Resource": "${aws_sqs_queue.sqs.arn}",
      "Condition": {
        "ArnEquals": {
          "aws:SourceArn": "${aws_sns_topic.sns.arn}"
        }
      }
    }
  ]
}
POLICY

}

resource "aws_sns_topic_subscription" "sns-topic-subscription" {
  topic_arn = aws_sns_topic.sns.arn
  protocol  = "sqs"
  endpoint  = aws_sqs_queue.sqs.arn

  filter_policy        = ""
  raw_message_delivery = true
}
