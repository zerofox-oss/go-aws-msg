package sns

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	msg "github.com/zerofox-oss/go-msg"
)

// Tests that msgAttributesToSNSAttributes successfully converts msg.Attributes
// to SNSAttributes.
func TestMsgAttributesToSNSAttributes_SingleListItems(t *testing.T) {
	w := &MessageWriter{
		attributes: make(map[string][]string),
	}
	w.Attributes().Set("key", "value")
	w.Attributes().Set("key2", "value2")

	// Note: the keys in expectedAttrs have a capital first letter because
	// .Attributes auto-capitalizes the first letter of the key
	expectedAttrs := make(map[string]*sns.MessageAttributeValue)
	expectedAttrs["Key"] = &sns.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String("value"),
	}
	expectedAttrs["Key2"] = &sns.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String("value2"),
	}

	computedAttrs := buildSNSAttributes(w.Attributes())

	for k := range *w.Attributes() {
		if expectedAttrs[k].String() != computedAttrs[k].String() {
			t.Errorf("Expected computedAttr to be %v, but got %v", expectedAttrs[k].String(), computedAttrs[k].String())
		}
	}
}

// Tests that MessageWriter's .Write and .Close functions return ErrClosedMessageWriter
// after .Close has been called.
func TestMessageWriter_WriteAndCloseReturnErrorAfterFirstClose(t *testing.T) {
	svc := &mockSNSAPI{
		sentParamChan: make(chan *sns.PublishInput),
		t:             t,
	}

	tpc := Topic{
		Svc:      svc,
		TopicARN: "test-arn",
	}

	mw := tpc.NewWriter(context.Background())

	go func() {
		<-svc.sentParamChan
	}()

	if _, err := mw.Write([]byte("test")); err != nil {
		t.Fatalf("could not write message %s", err)
	}
	mw.Close()

	if _, err := mw.Write([]byte("test2")); err != msg.ErrClosedMessageWriter {
		t.Errorf("Expected .Write to return %v, got %v", msg.ErrClosedMessageWriter, err)
	}

	if err := mw.Close(); err != msg.ErrClosedMessageWriter {
		t.Errorf("Expected .Close to return %v, got %v", msg.ErrClosedMessageWriter, err)
	}
}

// Tests that MessageWriter properly formats and constructs SNS publish params
// based on its Body and Attributes.
func TestMessageWriter_CloseProperlyConstructsPublishInput(t *testing.T) {
	control := make(chan struct{})
	svc := &mockSNSAPI{
		sentParamChan: make(chan *sns.PublishInput),
		t:             t,
	}

	tpc := Topic{
		Svc:      svc,
		TopicARN: "test-arn",
	}

	msg := []byte("test message")

	mw := tpc.NewWriter(context.Background())
	mw.Attributes().Set("key", "value")

	go func() {
		// Construct "expected" Attributes on unrelated MessageWriter
		// (including the additional base64)
		m2 := tpc.NewWriter(context.Background())
		m2.Attributes().Set("key", "value")
		m2.Attributes().Set("Content-Transfer-Encoding", "base64")

		expectedInput := &sns.PublishInput{
			Message:           aws.String("test message"),
			TopicArn:          aws.String(tpc.TopicARN),
			MessageAttributes: buildSNSAttributes(m2.Attributes()),
		}

		receivedInput := <-svc.sentParamChan

		// Assert on Message
		if *receivedInput.Message != *expectedInput.Message {
			t.Errorf("Expected input to be:\n%v\ngot: \n%v", *receivedInput.Message, *expectedInput.Message)
		}

		// Assert on TopicArn
		if *receivedInput.TopicArn != *expectedInput.TopicArn {
			t.Errorf("Expected input to be:\n%v\ngot: \n%v", *receivedInput.TopicArn, *expectedInput.TopicArn)
		}

		// Assert on AWS Message Attributes
		for k := range receivedInput.MessageAttributes {
			received := receivedInput.MessageAttributes[k].String()
			expected := expectedInput.MessageAttributes[k].String()
			if received != expected {
				t.Errorf("Expected computedAttr to be %v, but got %v", received, expected)
			}
		}

		control <- struct{}{}
	}()

	if _, err := mw.Write(msg); err != nil {
		t.Fatalf("could not write message %s", err)
	}
	mw.Close()

	<-control
}

type constructor func(string, ...Option) (msg.Topic, error)

func TestMessageWriter_Close_retryer(t *testing.T) {
	retries := make([]*http.Request, 0, 3)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadAll(r.Body)
		t.Logf("Request: %s\n", b)
		retries = append(retries, r)
		w.WriteHeader(403)
		fmt.Fprintln(w, `
<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">
  <Error>
    <Type>Sender</Type>
    <Code>InvalidClientTokenId</Code>
    <Message>The security token included in the request is invalid.</Message>
  </Error>
  <RequestId>590d5457-e4b6-5464-a482-071900d4c7d6</RequestId>
</ErrorResponse>`)
	}))
	defer ts.Close()

	os.Setenv("SNS_ENDPOINT", ts.URL)
	os.Setenv("AWS_ACCESS_KEY_ID", "fake")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "fake")

	defer func() {
		os.Unsetenv("SNS_ENDPOINT")
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	}()

	cases := []struct {
		name     string
		newTopic constructor
		options  []Option
		numTries int
	}{
		{"default", NewTopic, nil, 8},
		{"1 retry", NewTopic, []Option{WithRetries(0, 1)}, 2},
		{"No retries", NewTopic, []Option{WithRetries(0, 0)}, 1},
		{"UnencodedTopic default", NewUnencodedTopic, nil, 8},
		{"UnencodedTopic 1 retry", NewUnencodedTopic, []Option{WithRetries(0, 1)}, 2},
		{"UnencodedTopic No retries", NewUnencodedTopic, []Option{WithRetries(0, 0)}, 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			retries = make([]*http.Request, 0, 3)
			tpc, err := c.newTopic("arn:aws:sns:us-west-2:777777777777:test-sns", c.options...)
			if err != nil {
				t.Errorf("Server creation should not fail: %s", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			w := tpc.NewWriter(ctx)

			if _, err := w.Write([]byte("it's full of stars!")); err != nil {
				t.Fatalf("could not write message %s", err)
			}
			err = w.Close()

			if strings.Index(err.Error(), "InvalidClientTokenId: The security token included in the request is invalid") != 0 {
				t.Errorf("Expected error message to start with `InvalidClientTokenId: The security token included in the request is invalid`, was `%s`", err.Error())
			}

			t.Logf("retries: %v", retries)
			if len(retries) != c.numTries {
				t.Errorf("It should try %d times before failing, was %d", c.numTries, len(retries))
			}
		})
	}
}
