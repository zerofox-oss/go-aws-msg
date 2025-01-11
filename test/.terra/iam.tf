resource "aws_iam_user" "test-user" {
  name = "test"
}

resource "aws_iam_policy" "test-policy" {
  name = "yuri-sns-sqs-policy"

  policy = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Action": [
                "sns:Publish"
            ],
            "Effect": "Allow",
            "Resource": "arn:${var.iam_partition}:sns:${var.aws_region}:${data.aws_caller_identity.current.id}:test*"
        },
        {
            "Action": [
                "sqs:ChangeMessageVisibility",
                "sqs:DeleteMessage",
                "sqs:ReceiveMessage"
            ],
            "Effect": "Allow",
            "Resource": "arn:${var.iam_partition}:sqs:${var.aws_region}:${data.aws_caller_identity.current.id}:test*"
        }
  ]
}
EOF
}

resource "aws_iam_policy_attachment" "test-attachment" {
  name       = "test-policy-attachment"
  users      = [aws_iam_user.test-user.name]
  policy_arn = aws_iam_policy.test-policy.arn
}

resource "aws_iam_access_key" "test-key" {
  user = aws_iam_user.test-user.name
}