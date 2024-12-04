package sqs

import (
	"context"
	"errors"
	"fmt"
	"github.com/yurizf/go-aws-msg-with-batching/awsinterfaces"
	"math"
	"testing"

	"github.com/aws/aws-sdk-go/service/sqs"
)

// mockSQSAPI satisfies the necessary subset of sqs.sqsiface interface.
// It handles tracking calls
// to ReceiveMessage, DeleteMessage, and others to handle test assertions.
type mockSQSAPI struct {
	awsinterfaces.SQSSender
	awsinterfaces.SQSReceiver

	Queue  []*sqs.Message
	dmChan chan struct{} // each time a message is deleted a struct is written to this channel
	rmChan chan struct{} // each time a message is requeued, a struct is wrtten to this channel
	recIdx int           // total number of messages received
	t      *testing.T
}

// DeleteMessage finds Message in SQS queue with the matching ReceiptHandle and
// marks it as removed. It will write to a channel for each message deleted.
//
// Returns an sqs.ErrCodeReceiptHandleIsInvalid if the receipt handle provided
// is invalid.
func (s *mockSQSAPI) DeleteMessage(input *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	for _, m := range s.Queue {
		if *m.ReceiptHandle == *input.ReceiptHandle {
			s.dmChan <- struct{}{}
			return &sqs.DeleteMessageOutput{}, nil
		}
	}
	return nil, errors.New(sqs.ErrCodeReceiptHandleIsInvalid)
}

// ReceiveMessage retrieves 0 or more messages (up to the maximum specified).
// If there are no more messages to return, then it will return a list of 0.
func (s *mockSQSAPI) ReceiveMessage(input *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	oldIdx := s.recIdx
	newIdx := int(math.Min(
		float64(s.recIdx+int(*input.MaxNumberOfMessages)),
		float64(len(s.Queue)),
	))
	s.recIdx = newIdx

	return &sqs.ReceiveMessageOutput{Messages: s.Queue[oldIdx:newIdx]}, nil
}

// ChangeMessageVisibility mocks the SQS functionality to force a message to
// be requeued.
func (s *mockSQSAPI) ChangeMessageVisibility(*sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	s.rmChan <- struct{}{}
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

// WaitForAllDeletes listens to dmChan until the number of writes to the channel
// is equal to the total number of messages that were queued. If the provided
// context times out then an error will be returned which includes the number
// of messages that were deleted when the context timed out.
func (s *mockSQSAPI) WaitForAllDeletes(ctx context.Context) error {
	i := 0
	for range s.dmChan {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context timeout: expected %d messages deleted, got %d", len(s.Queue), i)
		default:
			i++
			if i == len(s.Queue) {
				close(s.dmChan)
			}
		}
	}
	return nil
}

// WaitForVisibilityTimeouts listens to the rmChan until the number of writes to the
// channel is equal to the number of messages that were queued. If the context cancels
// (times out), an error will be returned to indicate failure of a test.
func (s *mockSQSAPI) WaitForVisibilityTimeouts(ctx context.Context) error {
	i := 0
	for range s.rmChan {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context timeout: expected %d messages requeued, got %d", len(s.Queue), i)
		default:
			i++
			if i == len(s.Queue) {
				close(s.rmChan)
			}
		}
	}
	return nil
}

// newMockServer constructs a sqs.Server using the passed mockSQS and concurrency.
func newMockServer(concurrency int, mockSQS *mockSQSAPI) *Server {
	serverCtx, serverCancelFunc := context.WithCancel(context.Background())
	receiverCtx, receiverCancelFunc := context.WithCancel(context.Background())

	srv := &Server{
		maxConcurrentReceives: make(chan struct{}, concurrency),
		receiverCtx:           receiverCtx,
		receiverCancelFunc:    receiverCancelFunc,
		serverCtx:             serverCtx,
		serverCancelFunc:      serverCancelFunc,
		QueueURL:              "https://myqueue.com",
		Svc:                   mockSQS,
		retryTimeout:          100,
	}

	return srv
}

// newMockSQSAPI constructs a mock that meets the sqsiface interface.
func newMockSQSAPI(messages *[]*sqs.Message, t *testing.T) *mockSQSAPI {
	return &mockSQSAPI{
		Queue:  *messages,
		dmChan: make(chan struct{}, len(*messages)),
		rmChan: make(chan struct{}, 1),
		t:      t,
	}
}
