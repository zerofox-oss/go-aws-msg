package sns

import (
	"bytes"
	"context"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	b64 "github.com/zerofox-oss/go-aws-msg/base64"
	"github.com/zerofox-oss/go-msg"
)

// Topic configures and manages SNSAPI for sns.MessageWriter.
type Topic struct {
	Svc      snsiface.SNSAPI
	TopicARN string
}

// NewTopic returns a sns.Topic with fully configured SNSAPI.
//
// Note: SQS has limited support for unicode characters.
// - See http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/limits-messages.html
// Because we use SNS and SQS together, we recommended that SNS
// messages are base64-encoded as a best practice.
//
// You may use NewUnencodedTopic if you wish to ignore the encoding step.
func NewTopic(topicARN string) (msg.Topic, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

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

	return b64.Encoder(&Topic{
		Svc:      sns.New(sess, conf),
		TopicARN: topicARN,
	}), nil
}

// NewUnencodedTopic creates an concrete SNS msg.Topic
//
// Messages published by the `Topic` returned will not
// have the body base64-encoded.
func NewUnencodedTopic(topicARN string) (msg.Topic, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	conf := &aws.Config{
		Credentials: credentials.NewCredentials(&credentials.EnvProvider{}),
		Region:      aws.String("us-west-2"),
	}

	// http://docs.aws.amazon.com/sdk-for-go/api/aws/client/#Config
	if url := os.Getenv("SNS_ENDPOINT"); url != "" {
		conf.Endpoint = aws.String(url)
	}

	return &Topic{
		Svc:      sns.New(sess, conf),
		TopicARN: topicARN,
	}, nil
}

// NewWriter returns a sns.MessageWriter instance for writing to
// the configured SNS topic.
func (t *Topic) NewWriter(ctx context.Context) msg.MessageWriter {
	return &MessageWriter{
		attributes: make(map[string][]string),
		snsClient:  t.Svc,
		topicARN:   t.TopicARN,
		ctx:        ctx,
	}
}

// MessageWriter writes data to an output SNS topic as configured via its
// topicARN.
type MessageWriter struct {
	msg.MessageWriter

	attributes msg.Attributes
	buf        bytes.Buffer
	closed     bool
	mux        sync.Mutex

	snsClient snsiface.SNSAPI
	topicARN  string

	ctx context.Context
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

	if w.closed == true {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	attrs := buildSNSAttributes(w.Attributes())
	snsPublishParams := &sns.PublishInput{
		Message:           aws.String(string(w.buf.String())),
		TopicArn:          aws.String(w.topicARN),
		MessageAttributes: attrs,
	}

	log.Printf("[TRACE] writing to sns: %v", snsPublishParams)
	if _, err := w.snsClient.PublishWithContext(w.ctx, snsPublishParams); err != nil {
		return err
	}
	return nil
}

// Write writes data to the MessageWriter's internal buffer for aggregation
// before a .Close()
//
// After a MessageWriter's .Close() method has been called, it is no longer
// available for .Write() calls.
func (w *MessageWriter) Write(p []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed == true {
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
