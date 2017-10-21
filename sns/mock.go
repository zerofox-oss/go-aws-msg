package sns

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
)

// mockSNSAPI satisfies the sns.snsiface interface. It handles calls to Publish
// during testing.
type mockSNSAPI struct {
	snsiface.SNSAPI

	sentParamChan chan *sns.PublishInput
	t             *testing.T
}

// Publish mocks the SNSAPI's Publish function. Instead of publishing to an SNS
// topic, it puts the input onto a channel. This allows for test assertions.
func (s *mockSNSAPI) PublishWithContext(ctx aws.Context, input *sns.PublishInput, options ...request.Option) (*sns.PublishOutput, error) {
	s.sentParamChan <- input
	return nil, nil
}
