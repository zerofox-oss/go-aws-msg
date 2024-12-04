package sqs

import (
	"bytes"
	"context"
	"github.com/yurizf/go-aws-msg-with-batching/awsinterfaces"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/yurizf/go-aws-msg-with-batching/batching"
	msg "github.com/zerofox-oss/go-msg"
)

// Topic configures and manages SQSAPI for sqs.MessageWriter
type Topic struct {
	QueueURL string
	Svc      sqsiface.SQSAPI
}

var toBatch bool

// Starts the batching engine
func BatchON() {
	batching.New(batching.SQS)
	toBatch = true
}

// BatchOFF - drains the batch queue and stops the batching engine
// Usually called on a termination signal
func BatchOFF() {
	toBatch = false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	batching.ShutDown(ctx)
}

// NewTopic returns an sqs.Topic with fully configured SQSAPI
func NewTopic(queueURL string) (msg.Topic, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	conf := &aws.Config{
		Credentials: credentials.NewCredentials(&credentials.EnvProvider{}),
	}

	// http://docs.aws.amazon.com/sdk-for-go/api/aws/client/#Config
	if r := os.Getenv("AWS_REGION"); r != "" {
		conf.Region = aws.String(r)
	}
	if url := os.Getenv("SQS_ENDPOINT"); url != "" {
		conf.Endpoint = aws.String(url)
	}

	svc := sqs.New(sess, conf)

	if toBatch {
		// defgault timeout
		batching.NewTopic(queueURL, svc, 1*time.Second)
	}

	return &Topic{
		QueueURL: queueURL,
		Svc:      svc,
	}, nil
}

// NewWriter returns a new sqs.MessageWriter
func (t *Topic) NewWriter(ctx context.Context) msg.MessageWriter {
	return &MessageWriter{
		attributes: make(map[string][]string),
		buf:        &bytes.Buffer{},
		ctx:        ctx,
		queueURL:   t.QueueURL,
		sqsClient:  t.Svc,
	}
}

// MessageWriter writes data to a SQS Queue.
type MessageWriter struct {
	msg.MessageWriter

	attributes msg.Attributes
	buf        *bytes.Buffer
	ctx        context.Context
	closed     bool
	mux        sync.Mutex

	// delaySeconds is a length of time to delay the SQS message.
	delaySeconds int64

	// sqsClient is the SQS interface
	// sqsClient sqsiface.SQSAPI
	sqsClient awsinterfaces.SQSSender

	// queueURL is the URL to the queue.
	queueURL string
}

// Attributes returns the msg.Attributes associated with the MessageWriter
func (w *MessageWriter) Attributes() *msg.Attributes {
	return &w.attributes
}

// Write writes data to the MessageWriter's internal buffer.
//
// Once a MessageWriter is closed, it cannot be used again.
func (w *MessageWriter) Write(p []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(p)
}

// Close converts it's buffered data and attributes to an SQS message
// and publishes it to a queue.
//
// Once a MessageWriter is closed, it cannot be used again.
func (w *MessageWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	params := &sqs.SendMessageInput{
		DelaySeconds: aws.Int64(w.delaySeconds),
		MessageBody:  aws.String(w.buf.String()),
		QueueUrl:     aws.String(w.queueURL),
	}

	if len(*w.Attributes()) > 0 {
		params.MessageAttributes = buildSQSAttributes(w.Attributes())
	}

	if toBatch {
		batching.SetAttributes(w.queueURL, params.MessageAttributes)
		return batching.Append(w.queueURL, w.buf.String())
	}

	log.Printf("[TRACE] writing to sqs: %v", params)
	_, err := w.sqsClient.SendMessageWithContext(w.ctx, params)
	return err
}

// SetDelay sets a delay on the Message.
// The delay must be between 0 and 900 seconds, according to the awsinterfaces sdk.
func (w *MessageWriter) SetDelay(delay time.Duration) {
	w.delaySeconds = int64(math.Min(math.Max(delay.Seconds(), 0), 900))
	if toBatch {
		batching.SetTopicTimeout(w.queueURL, time.Duration(w.delaySeconds)*time.Second)
	}
}

// buildSNSAttributes converts msg.Attributes into SQS message attributes.
// uses csv encoding to use AWS's String datatype
func buildSQSAttributes(a *msg.Attributes) map[string]*sqs.MessageAttributeValue {
	attrs := make(map[string]*sqs.MessageAttributeValue)

	for k, v := range *a {
		attrs[k] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(strings.Join(v, ",")),
		}
	}
	return attrs
}
