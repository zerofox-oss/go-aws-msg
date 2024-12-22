package awsinterfaces

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
)

// for mocking. Plus, the type SNSAPI interface is HUGE
type SNSPublisher interface {
	PublishWithContext(aws.Context, *sns.PublishInput, ...request.Option) (*sns.PublishOutput, error)
}

type SQSSender interface {
	SendMessageWithContext(aws.Context, *sqs.SendMessageInput, ...request.Option) (*sqs.SendMessageOutput, error)
}

type SQSReceiver interface {
	ReceiveMessage(*sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error)
	ChangeMessageVisibility(*sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error)
	DeleteMessage(*sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error)
}
