package sns

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/zerofox-oss/go-msg"
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

	mw := tpc.NewWriter()

	go func() {
		<-svc.sentParamChan
	}()

	mw.Write([]byte("test"))
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

	mw := tpc.NewWriter()
	mw.Attributes().Set("key", "value")

	go func() {
		// Construct "expected" Attributes on unrelated MessageWriter
		// (including the additional base64)
		m2 := tpc.NewWriter()
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

	mw.Write(msg)
	mw.Close()

	<-control
}
