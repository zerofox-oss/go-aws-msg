package sns

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/yurizf/go-aws-msg-costs-control/awsinterfaces"
	"github.com/yurizf/go-aws-msg-costs-control/batching"
	"github.com/yurizf/go-aws-msg-costs-control/partialbase64encode"
	"github.com/yurizf/go-aws-msg-costs-control/retryer"
	msg "github.com/zerofox-oss/go-msg"
	b64 "github.com/zerofox-oss/go-msg/decorators/base64"
)

// DI to support mocking
var NewSNSPublisherFunc = func(sess *session.Session, cfgs ...*aws.Config) awsinterfaces.SNSPublisher {
	if len(cfgs) == 0 {
		return sns.New(sess)
	} else {
		return sns.New(sess, cfgs...)
	}
}

// Topic configures and manages SNSAPI for sns.MessageWriter.
type Topic struct {
	Svc      awsinterfaces.SNSPublisher
	TopicARN string
	session  *session.Session
	Batcher  batching.Batcher
}

func getConf(t *Topic) (*aws.Config, error) {
	switch v := t.Svc.(type) {
	case *sns.SNS:
		return &(v.Client.Config), nil
	default: // for testing
		return &aws.Config{}, nil
	}
}

// Option is the signature that modifies a `Batcher` to set some configuration
type Option func(*Topic) error

// WithCustomRetryer sets a custom `Retryer` to use on the SQS client.
func WithCustomRetryer(r request.Retryer) Option {
	return func(t *Topic) error {
		c, err := getConf(t)
		if err != nil {
			return err
		}
		c.Retryer = r
		t.Svc = NewSNSPublisherFunc(t.session, c)
		return nil
	}
}

// WithRetries makes the `Server` retry on credential errors until
// `max` attempts with `delay` seconds between requests.
// This is needed in scenarios where credentials are automatically generated
// and the program starts before AWS finishes propagating them
func WithRetries(delay time.Duration, max int) Option {
	return func(t *Topic) error {
		c, err := getConf(t)
		if err != nil {
			return err
		}
		c.Retryer = retryer.DefaultRetryer{
			Retryer: client.DefaultRetryer{NumMaxRetries: max},
			Delay:   delay,
		}
		t.Svc = NewSNSPublisherFunc(t.session, c)
		return nil
	}
}

// NewTopic returns a sns.Topic with fully configured SNSAPI.
//
// Note: SQS has limited support for unicode characters.
// - See http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/limits-messages.html
// Because we use SNS and SQS together, we recommend
// that SNS messages are base64-encoded as a best practice.
// You may use NewUnencodedTopic if you wish to ignore the encoding step.
func NewTopic(topicARN string, opts ...Option) (msg.Topic, error) {
	topic, err := NewUnencodedTopic(topicARN, opts...)
	if err != nil {
		return nil, err
	}
	return b64.Encoder(topic), nil
}

// NewPartialBASE64Topic returns a sns.Topic with fully configured SNSAPI.
//
// Note: SQS has limited support for unicode characters.
// - See http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/limits-messages.html
// Because we use SNS and SQS together, we recommend
// that SNS messages are *partially* base64-encoded: only the unicode fragments not supported by SQS are encoded.
// You may use NewUnencodedTopic if you wish to ignore the encoding step.
func NewPartialBASE64Topic(topicARN string, opts ...Option) (msg.Topic, error) {
	topic, err := NewUnencodedTopic(topicARN, opts...)
	if err != nil {
		return nil, err
	}
	return partialbase64encode.Encoder(topic), nil
}

// NewUnencodedTopic creates an concrete SNS msg.Topic
//
// Messages published by the `Batcher` returned will not
// have the body base64-encoded.
func NewUnencodedTopic(topicARN string, opts ...Option) (msg.Topic, error) {
	conf := &aws.Config{
		Credentials: credentials.NewCredentials(&credentials.EnvProvider{}),
		Region:      aws.String("us-west-2"),
	}

	// You may override AWS_REGION, SNS_ENDPOINT
	// http://docs.aws.amazon.com/sdk-for-go/api/aws/client/#Config
	if r := os.Getenv("AWS_REGION"); r != "" {
		conf.Region = aws.String(r)
	}
	if url := os.Getenv("SNS_ENDPOINT"); url != "" {
		conf.Endpoint = aws.String(url)
	}

	sess, err := session.NewSession(conf)
	if err != nil {
		return nil, err
	}

	t := &Topic{
		Svc:      NewSNSPublisherFunc(sess),
		TopicARN: topicARN,
		session:  sess,
	}

	// Default retryer
	if err = WithRetries(2*time.Second, 7)(t); err != nil {
		return nil, err
	}

	for _, opt := range opts {
		if err = opt(t); err != nil {
			return nil, fmt.Errorf("cannot set option: %s", err)
		}
	}

	return t, err
}

// NewBatchedTopic creates an concrete SNS msg.Topic
// It returns msg.Topic as opposed to BatchedTopic so that existing
// calls to NewTopic could simply replace it with NewBatchedTopic
// Messages published by the `Batcher` returned will be partially
// base64-encoded: only runes SQS does not support will be encoded.
// And these messages will be batched for transmission.
type BatchedTopic interface {
	msg.Topic
	batching.Batcher
}
type BatchedTopicS struct {
	msg.Topic
	batching.Batcher
}

func NewBatchedTopic(topicARN string, timeout ...time.Duration) (BatchedTopic, error) {
	to := batching.DEFAULT_BATCH_TIMEOUT
	if len(timeout) > 0 {
		to = timeout[0]
	}

	t, err := NewUnencodedTopic(topicARN)
	if err == nil {
		concrete := t.(*Topic)
		b, err := batching.New(topicARN, concrete.Svc, to)
		concrete.Batcher = b
		return BatchedTopicS{concrete, b}, err
	}

	return nil, err
}

// NewWriter returns a sns.MessageWriter instance for writing to
// the configured SNS Topic.
func (t *Topic) NewWriter(ctx context.Context) msg.MessageWriter {
	return &MessageWriter{
		attributes: make(map[string][]string),
		snsClient:  t.Svc,
		topicARN:   t.TopicARN,
		ctx:        ctx,
		batcher:    t.Batcher,
	}
}

// MessageWriter writes data to an output SNS batcher as configured via its
// topicARN.
type MessageWriter struct {
	msg.MessageWriter

	attributes msg.Attributes
	buf        bytes.Buffer
	closed     bool
	mux        sync.Mutex

	snsClient awsinterfaces.SNSPublisher
	topicARN  string
	ctx       context.Context

	batcher batching.Batcher
}

// Attributes returns the msg.Attributes associated with the MessageWriter.
func (w *MessageWriter) Attributes() *msg.Attributes {
	return &w.attributes
}

// Close converts the MessageWriter's Body and Attributes to sns.PublishInput
// in order to publish itself to the MessageWriter's snsClient.
//
// On the first call to Close, the MessageWriter is set to "isClosed" therefore
// blocking subsequent Close and Write calls.
func (w *MessageWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	if w.batcher != nil {
		// encoding attributes r set and encoding is done in Append.
		return w.batcher.Append(w.buf.String())
	}

	params := &sns.PublishInput{
		Message:  aws.String(w.buf.String()),
		TopicArn: aws.String(w.topicARN),
	}

	if len(*w.Attributes()) > 0 {
		params.MessageAttributes = buildSNSAttributes(w.Attributes())
	}

	log.Printf("[TRACE] writing to sns: %v", params)
	_, err := w.snsClient.PublishWithContext(w.ctx, params)
	return err
}

// Write writes data to the MessageWriter's internal buffer for aggregation
// before a .Close()
//
// After a MessageWriter's .Close() method has been called, it is no longer
// available for .Write() calls.
func (w *MessageWriter) Write(p []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(p)
}

// buildSNSAttributes converts msg.Attributes into SNS message attributes.
// uses csv encoding to use AWS's String datatype
func buildSNSAttributes(a *msg.Attributes) map[string]*sns.MessageAttributeValue {
	attrs := make(map[string]*sns.MessageAttributeValue)

	for k, v := range *a {
		attrs[k] = &sns.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(strings.Join(v, ",")),
		}
	}
	return attrs
}
