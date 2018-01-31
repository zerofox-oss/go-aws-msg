package sqs

import (
	"bytes"
	"context"
	"log"
	"os"
	"time"

	"github.com/zerofox-oss/go-msg"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
)

// Server represents a msg.Server for receiving messages
// from an AWS SQS Queue
type Server struct {
	// AWS QueueURL
	QueueURL string
	// Concrete instance of SQSAPI
	Svc sqsiface.SQSAPI

	maxConcurrentReceives chan struct{} // The maximum number of message processing routines allowed
	retryTimeout          int64         // Visbility Timeout for a message when a receiver fails

	receiverCtx        context.Context    // context used to control the life of receivers
	receiverCancelFunc context.CancelFunc // CancelFunc for all receiver routines
	serverCtx          context.Context    // context used to control the life of the Server
	serverCancelFunc   context.CancelFunc // CancelFunc to signal the server should stop requesting messages
}

// convertToMsgAttrs creates msg.Attributes from sqs.Message.Attributes
func (s *Server) convertToMsgAttrs(awsAttrs map[string]*sqs.MessageAttributeValue) msg.Attributes {
	attr := msg.Attributes{}
	for k, v := range awsAttrs {
		attr.Set(k, *v.StringValue)
	}
	return attr
}

// Serve continuously receives messages from an SQS queue, creates a message,
// and calls Receive on `r`. Serve is blocking and will not return until
// Shutdown is called on the Server.
//
// NewServer should be used prior to running Serve.
func (s *Server) Serve(r msg.Receiver) error {

	for {
		select {

		// Shuts down the server
		case <-s.serverCtx.Done():
			close(s.maxConcurrentReceives)
			return msg.ErrServerClosed

		// Receive Messages from SQS
		default:
			resp, err := s.Svc.ReceiveMessage(&sqs.ReceiveMessageInput{
				MaxNumberOfMessages:   aws.Int64(10),
				WaitTimeSeconds:       aws.Int64(20),
				QueueUrl:              aws.String(s.QueueURL),
				MessageAttributeNames: []*string{aws.String("All")},
			})

			if err != nil {
				log.Printf("[ERROR] Could not read from SQS: %s", err.Error())
				return err
			}

			for _, m := range resp.Messages {
				if m.MessageId != nil {
					log.Printf("[TRACE] Received SQS Message: %s\n", *m.MessageId)
				}

				// Take a slot from the buffered channel
				s.maxConcurrentReceives <- struct{}{}

				go func(sqsMsg *sqs.Message) {
					defer func() {
						<-s.maxConcurrentReceives
					}()

					m := &msg.Message{
						Attributes: s.convertToMsgAttrs(sqsMsg.MessageAttributes),
						Body:       bytes.NewBufferString(*sqsMsg.Body),
					}
					err := r.Receive(s.receiverCtx, m)

					if err != nil {
						log.Printf("[ERROR] Receiver error: %s; will retry after visibility timeout", err.Error())
						s.Svc.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
							QueueUrl:          aws.String(s.QueueURL),
							ReceiptHandle:     sqsMsg.ReceiptHandle,
							VisibilityTimeout: aws.Int64(s.retryTimeout),
						})
						return
					}

					_, err = s.Svc.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      aws.String(s.QueueURL),
						ReceiptHandle: sqsMsg.ReceiptHandle,
					})

					if err != nil {
						log.Printf("[ERROR] Delete message: %s", err.Error())
					}

				}(m)
			}
		}
	}
}

var shutdownPollInterval = 500 * time.Millisecond

// Shutdown stops the receipt of new messages and waits for routines
// to complete or the passed in ctx to be canceled. msg.ErrServerClosed
// will be returned upon a clean shutdown. Otherwise, the passed ctx's
// Error will be returned.
func (s *Server) Shutdown(ctx context.Context) error {
	if ctx == nil {
		panic("context not set")
	}
	s.serverCancelFunc()

	ticker := time.NewTicker(shutdownPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			s.receiverCancelFunc()
			return ctx.Err()

		case <-ticker.C:
			if len(s.maxConcurrentReceives) == 0 {
				return msg.ErrServerClosed
			}
		}
	}
}

// NewServer creates and initializes a new Server using queueURL to a SQS queue
// `cl` represents the number of concurrent message receives (10 msgs each).
//
// AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) are assumed to be set
// as environment variables.
//
// SQS_ENDPOINT can be set as an environment variable in order to
// override the aws.Client's Configured Endpoint
func NewServer(queueURL string, cl int, retryTimeout int64) (msg.Server, error) {
	// It makes no sense to have a concurrency of less than 1.
	if cl < 1 {
		log.Printf("[WARN] Requesting concurrency of %d, this makes no sense, setting to 1\n", cl)
		cl = 1
	}

	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}
	conf := &aws.Config{
		Credentials: credentials.NewCredentials(&credentials.EnvProvider{}),
		Region:      aws.String("us-west-2"),
	}

	// http://docs.aws.amazon.com/sdk-for-go/api/aws/client/#Config
	if r := os.Getenv("AWS_REGION"); r != "" {
		conf.Region = aws.String(r)
	}

	if url := os.Getenv("SQS_ENDPOINT"); url != "" {
		conf.Endpoint = aws.String(url)
	}

	// Create an SQS Client with creds from the Environment
	svc := sqs.New(sess, conf)

	serverCtx, serverCancelFunc := context.WithCancel(context.Background())
	receiverCtx, receiverCancelFunc := context.WithCancel(context.Background())

	srv := &Server{
		Svc:                   svc,
		retryTimeout:          retryTimeout,
		QueueURL:              queueURL,
		maxConcurrentReceives: make(chan struct{}, cl),
		serverCtx:             serverCtx,
		serverCancelFunc:      serverCancelFunc,
		receiverCtx:           receiverCtx,
		receiverCancelFunc:    receiverCancelFunc,
	}
	return srv, nil
}
