package sqs

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yurizf/go-aws-msg-costs-control/awsinterfaces"
	"github.com/yurizf/go-aws-msg-costs-control/batching"
	"github.com/yurizf/go-aws-msg-costs-control/partialbase64encode"
	"github.com/yurizf/go-aws-msg-costs-control/retryer"
	msg "github.com/zerofox-oss/go-msg"
)

func init() {
	var b [8]byte

	if _, err := crand.Read(b[:]); err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}

	rand.Seed(int64(binary.LittleEndian.Uint64(b[:])))
}

// ErrThrottleServer signals that the server should sleep for the duration
// time before resuming work.
type ErrThrottleServer struct {
	Message  string
	Duration time.Duration
}

func (e ErrThrottleServer) Error() string {
	return fmt.Sprintf("error: %s - server throttled for %s", e.Message, e.Duration)
}

// Server represents a msg.Server for receiving messages
// from an AWS SQS Queue.
type Server struct {
	// AWS QueueURL
	QueueURL string
	// Concrete instance of SQSAPI
	Svc awsinterfaces.SQSReceiver

	maxConcurrentReceives chan struct{} // The maximum number of message processing routines allowed
	retryTimeout          int64         // Visbility Timeout for a message when a receiver fails
	retryJitter           int64

	receiverCtx        context.Context    // context used to control the life of receivers
	receiverCancelFunc context.CancelFunc // CancelFunc for all receiver routines
	serverCtx          context.Context    // context used to control the life of the Server
	serverCancelFunc   context.CancelFunc // CancelFunc to signal the server should stop requesting messages
	session            *session.Session   // session used to re-create `Svc` when needed
}

// DI to support mocking
var NewSQSReceiverFunc = func() (awsinterfaces.SQSReceiver, *session.Session, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, nil, err
	}

	conf := &aws.Config{
		Credentials: credentials.NewCredentials(&credentials.EnvProvider{}),
		Region:      aws.String("us-west-2"),
		Retryer: retryer.DefaultRetryer{
			Retryer: client.DefaultRetryer{NumMaxRetries: 7},
			Delay:   2 * time.Second,
		},
	}

	// http://docs.aws.amazon.com/sdk-for-go/api/aws/client/#Config
	if r := os.Getenv("AWS_REGION"); r != "" {
		conf.Region = aws.String(r)
	}

	if url := os.Getenv("SQS_ENDPOINT"); url != "" {
		conf.Endpoint = aws.String(url)
	}

	// Create an SQS Client with creds from the Environment
	return sqs.New(sess, conf), sess, nil
}

// convertToMsgAttrs creates msg.Attributes from sqs.Message.Attributes.
func (s *Server) convertToMsgAttrs(attr msg.Attributes, awsAttrs map[string]*sqs.MessageAttributeValue) {
	for k, v := range awsAttrs {
		attr.Set(k, *v.StringValue)
	}
}

// convertToAttrs creates msg.Attributes from sqs.Attributes.
func (s *Server) convertToAttrs(attr msg.Attributes, attrs map[string]*string) {
	for k, v := range attrs {
		attr.Set(k, *v)
	}
}

// convertAllAttrs
func (s *Server) convertAllAttrs(m *sqs.Message) msg.Attributes {
	// set the sqs attributes first
	// and the custom message attributes after
	// as they may override the regular attributes
	attrs := msg.Attributes{}
	s.convertToAttrs(attrs, m.Attributes)
	s.convertToMsgAttrs(attrs, m.MessageAttributes)
	return attrs
}

// Serve continuously receives messages from an SQS queue, creates a message,
// and calls Receive on `r`. Serve is blocking and will not return until
// Shutdown is called on the Server.
//
// NewServer should be used prior to running Serve.
func (s *Server) Serve(r msg.Receiver) error {
	for {
		select {
		case <-s.serverCtx.Done():
			log.Printf("[TRACE] Closing Serve chan")
			close(s.maxConcurrentReceives)

			return msg.ErrServerClosed

		default:
			resp, err := s.Svc.ReceiveMessage(&sqs.ReceiveMessageInput{
				MaxNumberOfMessages:   aws.Int64(10),
				WaitTimeSeconds:       aws.Int64(20),
				QueueUrl:              aws.String(s.QueueURL),
				AttributeNames:        []*string{aws.String("All")},
				MessageAttributeNames: []*string{aws.String("All")},
			})
			if err != nil {
				log.Printf("[ERROR] Could not read from SQS: %s", err.Error())

				return err
			}

			for _, m := range resp.Messages {
				if m.MessageId != nil {
					log.Printf("[TRACE] Received SQS Message: %s of %d bytes\n", *m.MessageId, len(*m.Body))
				}

				attrs := s.convertAllAttrs(m)
				if attrs.Get(batching.ENCODING_ATTRIBUTE_KEY) == batching.ENCODING_ATTRIBUTE_VALUE {
					err = s.serveBatch(m, &attrs, r)
					continue
				}
				// Take a slot from the buffered channel
				s.maxConcurrentReceives <- struct{}{}

				go func(sqsMsg *sqs.Message) {
					defer func() {
						<-s.maxConcurrentReceives
					}()

					attrs := s.convertAllAttrs(sqsMsg)

					m := &msg.Message{
						Attributes: attrs,
						Body:       bytes.NewBufferString(*sqsMsg.Body),
					}

					if err := r.Receive(s.receiverCtx, m); err != nil {
						log.Printf("[ERROR] Receiver error: %s; will retry after visibility timeout", err.Error())

						params := &sqs.ChangeMessageVisibilityInput{
							QueueUrl:          aws.String(s.QueueURL),
							ReceiptHandle:     sqsMsg.ReceiptHandle,
							VisibilityTimeout: aws.Int64(getVisiblityTimeout(s.retryTimeout, s.retryJitter)),
						}
						if _, err := s.Svc.ChangeMessageVisibility(params); err != nil {
							log.Printf("[ERROR] cannot change message visibility %s", err)
						}

						throttleErr, ok := err.(ErrThrottleServer)
						if ok {
							log.Printf("[TRACE] throttling received, sleeping for: %s", throttleErr.Duration.String())

							time.Sleep(throttleErr.Duration)
						}
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

func (s *Server) serveBatch(m *sqs.Message, attrs *msg.Attributes, r msg.Receiver) error {

	msgs, err := batching.DeBatch(*m.Body)
	if err != nil {
		log.Printf("[ERROR] cannot debatch message [%s]: %s\n---------------", err, *m.Body)
		return err
	}
	var sb strings.Builder
	for i, m := range msgs {
		sb.WriteString(fmt.Sprintf("| msg %d of len %d: %s", i, len(m), m[0:min(20, len(m))]))
	}

	log.Printf("[TRACE] Unpacked %d messages from the MsgID=%s batch: %s", len(msgs), *m.MessageId, sb.String())

	// delete batch from SQS right away
	_, err = s.Svc.DeleteMessage(&sqs.DeleteMessageInput{
		QueueUrl:      aws.String(s.QueueURL),
		ReceiptHandle: m.ReceiptHandle,
	})

	if err != nil {
		log.Printf("[ERROR] Delete message: %s", err.Error())
	}

	for len(msgs) > 0 {
		select {
		case <-s.serverCtx.Done():
			log.Printf("[TRACE] serveBatch: Context is done.")
			// leave s.maxConcurrentReceives chan open. The calling Serve routine will close it.
			return msg.ErrServerClosed

		default:
			failed := make([]string, 0, 128)
			result := make(chan string, len(msgs))

			goCount := 0
			for _, payload := range msgs {
				// payload is partially base64-encoded
				payload, err = partialbase64encode.Decode(payload)
				if err != nil {
					log.Printf("[ERROR] failed to decode msg %v %v", err, []rune(payload))
					continue
				}

				// Take a slot from the buffered channel
				// parallelize like unbatched messages
				s.maxConcurrentReceives <- struct{}{}
				goCount++
				go func(attrs msg.Attributes, payload string) {
					defer func() {
						<-s.maxConcurrentReceives
					}()

					m_p := &msg.Message{
						Attributes: attrs,
						Body:       bytes.NewBufferString(payload),
					}

					if err := r.Receive(s.receiverCtx, m_p); err != nil {
						log.Printf("[ERROR] Receiver error: %s; will retry", err.Error())
						throttleErr, ok := err.(ErrThrottleServer)
						if ok {
							log.Printf("[TRACE] throttling received, sleeping for: %s", throttleErr.Duration.String())
							time.Sleep(throttleErr.Duration)
						}
						result <- payload
						return
					}
					result <- ""

				}(*attrs, payload)
			}

			for i := 0; i < goCount; i++ {
				select {
				case r := <-result:
					if len(r) > 0 {
						failed = append(failed, r)
					}
				default:
				}
			}

			// reprocess failed messages
			msgs = failed
		}
	}

	return err

}

func getVisiblityTimeout(retryTimeout int64, retryJitter int64) int64 {
	if retryJitter > retryTimeout {
		panic("jitter must be less than or equal to retryTimeout")
	}

	minRetry, maxRetry := retryTimeout-retryJitter, retryTimeout+retryJitter

	return int64(rand.Intn(int(maxRetry-minRetry)+1) + int(minRetry))
}

const shutdownPollInterval = 500 * time.Millisecond

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

// Option is the signature that modifies a `Server` to set some configuration
type Option func(*Server) error

// NewServer creates and initializes a new Server using queueURL to a SQS queue
// `cl` represents the number of concurrent message receives (10 msgs each).
//
// AWS credentials (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY) are assumed to be set
// as environment variables.
//
// SQS_ENDPOINT can be set as an environment variable in order to
// override the awsinterfaces.Client's Configured Endpoint
func NewServer(queueURL string, cl int, retryTimeout int64, opts ...Option) (msg.Server, error) {
	// It makes no sense to have a concurrency of less than 1.
	if cl < 1 {
		log.Printf("[WARN] Requesting concurrency of %d, this makes no sense, setting to 1\n", cl)
		cl = 1
	}

	svc, sess, err := NewSQSReceiverFunc()
	if err != nil {
		return nil, err
	}

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
		session:               sess,
	}

	for _, opt := range opts {
		if err = opt(srv); err != nil {
			return nil, fmt.Errorf("cannot set option: %s", err)
		}
	}

	return srv, nil
}

func getConf(s *Server) (*aws.Config, error) {
	svc, ok := s.Svc.(*sqs.SQS)
	if !ok {
		return nil, errors.New("svc could not be casted to a SQS client")
	}

	return &svc.Client.Config, nil
}

// WithCustomRetryer sets a custom `Retryer` to use on the SQS client.
func WithCustomRetryer(r request.Retryer) Option {
	return func(s *Server) error {
		c, err := getConf(s)
		if err != nil {
			return err
		}

		c.Retryer = r
		s.Svc = sqs.New(s.session, c)

		return nil
	}
}

// WithRetries makes the `Server` retry on credential errors until
// `max` attempts with `delay` seconds between requests.
// This is needed in scenarios where credentials are automatically generated
// and the program starts before AWS finishes propagating them
func WithRetries(delay time.Duration, max int) Option {
	return func(s *Server) error {
		c, err := getConf(s)
		if err != nil {
			return err
		}

		c.Retryer = retryer.DefaultRetryer{
			Retryer: client.DefaultRetryer{NumMaxRetries: max},
			Delay:   delay,
		}

		s.Svc = sqs.New(s.session, c)

		return nil
	}
}

// WithRetryJitter sets a value for Jitter on the VisibilityTimeout.
// With jitter applied every message that needs to be retried will
// have a visibility timeout in the interval:
// [(visibilityTimeout - jitter), visibilityTimeout + jitter)]
func WithRetryJitter(retryJitter int64) Option {
	return func(s *Server) error {
		if retryJitter > s.retryTimeout {
			return fmt.Errorf(
				"invalid jitter: %d. Jitter must be less or equal to the retryTimeout (%d)",
				retryJitter,
				s.retryTimeout,
			)
		}

		s.retryJitter = retryJitter

		return nil
	}
}
