package sqs

import (
	"bytes"
	"log"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	msg "github.com/zerofox-oss/go-msg"
)

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

	return &Topic{
		Svc:      sqs.New(sess, conf),
		QueueURL: queueURL,
	}, nil
}

// Topic configures and manages SQSAPI for sqs.MessageWriter
type Topic struct {
	Svc      sqsiface.SQSAPI
	QueueURL string
}

// NewWriter returns a new sqs.MessageWriter
func (t *Topic) NewWriter() msg.MessageWriter {
	return &MessageWriter{
		attributes: make(map[string][]string),
		buf:        &bytes.Buffer{},
		sqsClient:  t.Svc,
		queueURL:   t.QueueURL,
	}
}

// MessageWriter writes data to a SQS Queue
type MessageWriter struct {
	msg.MessageWriter

	attributes msg.Attributes
	buf        *bytes.Buffer
	closed     bool
	mux        sync.Mutex

	sqsClient sqsiface.SQSAPI
	queueURL  string
}

// Attributes returns the msg.Attributes associated with the MessageWriter
func (w *MessageWriter) Attributes() *msg.Attributes {
	return &w.attributes
}

// Write writes data to the MessageWriter's internal buffer
//
// Once a MessageWriter is closed, it cannot be used again.
func (w *MessageWriter) Write(p []byte) (int, error) {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed == true {
		return 0, msg.ErrClosedMessageWriter
	}
	return w.buf.Write(p)
}

// Close converts it's buffered data and attributes to an SQS message
// and publishes it to a queue
//
// Once a MessageWriter is closed, it cannot be used again.
func (w *MessageWriter) Close() error {
	w.mux.Lock()
	defer w.mux.Unlock()

	if w.closed == true {
		return msg.ErrClosedMessageWriter
	}
	w.closed = true

	sqsParams := &sqs.SendMessageInput{
		MessageBody:       aws.String(w.buf.String()),
		QueueUrl:          aws.String(w.queueURL),
		MessageAttributes: buildSQSAttributes(w.Attributes()),
	}

	log.Printf("[TRACE] writing to sqs: %v", sqsParams)
	if _, err := w.sqsClient.SendMessage(sqsParams); err != nil {
		return err
	}
	return nil
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
