package sqs

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yurizf/go-aws-msg-with-batching/batching"
	msg "github.com/zerofox-oss/go-msg"
)

// Failing receiver returns an error upon consumption of a Message.
type FailingReceiver struct {
	t *testing.T
}

func (r *FailingReceiver) Receive(ctx context.Context, m *msg.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return errors.New("failing recevier returned error")
	}
}

// SimpleReceiver simply returns nil upon consumption of a Message.
type SimpleReceiver struct {
	t *testing.T
}

func (r *SimpleReceiver) Receive(ctx context.Context, m *msg.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

// newSQSMessages generates a slice of n sqs.Message objects and
// returns a pointer to that slice.
func newSQSMessages(n int) *[]*sqs.Message {
	messages := make([]*sqs.Message, n)

	for i := 0; i < n; i++ {
		messages[i] = &sqs.Message{
			Body:              aws.String(fmt.Sprintf("this is a test %d", i)),
			MessageAttributes: make(map[string]*sqs.MessageAttributeValue),
			MessageId:         aws.String(fmt.Sprintf("msg%d", i)),
			ReceiptHandle:     aws.String(fmt.Sprintf("msg%d", i)),
		}
	}

	return &messages
}

// TestServer_Serve tests that an SQS server can receive messages, process
// them, and delete them from the queue successfully.
func TestServer_Serve(t *testing.T) {
	msgs := newSQSMessages(1)
	mockSQS := newMockSQSAPI(msgs, t)
	srv := newMockServer(1, mockSQS)

	go func() {
		r := &SimpleReceiver{t: t}
		if err := srv.Serve(r); err != nil {
			t.Errorf("server died %s", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := mockSQS.WaitForAllDeletes(ctx); err != nil {
		t.Errorf(err.Error())
	}
}

type BatchReceiver struct {
	t        *testing.T
	payloads []string
	mux      sync.Mutex
	received []string
}

func (r *BatchReceiver) Receive(ctx context.Context, m *msg.Message) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		buf := new(bytes.Buffer)
		buf.ReadFrom(m.Body)
		s := buf.String()
		r.mux.Lock()
		defer r.mux.Unlock()
		r.received = append(r.received, s)
		r.t.Log(fmt.Printf("batch receiver called for %s", s))
		for _, p := range r.payloads {
			if s == p {
				return nil
			}
		}
		return fmt.Errorf("payload %s is not expected", s)
	}
}

// TestServer_Serve tests that an SQS server can receive messages, process
// them, and delete them from the queue successfully.
func TestServer_ServeBatched(t *testing.T) {
	msgs := newSQSMessages(1)
	toBatch = true
	defer func() {
		toBatch = false
	}()

	(*msgs)[0].Body = aws.String(batching.Batch([]string{"12345", "文字材料"}))
	mockSQS := newMockSQSAPI(msgs, t)
	srv := newMockServer(1, mockSQS)

	r := &BatchReceiver{
		t:        t,
		payloads: []string{"12345", "文字材料"},
		received: make([]string, 0, 2),
	}

	go func() {
		if err := srv.Serve(r); err != nil { // Serve spawns go routines calling Receive func for each msg
			t.Log(fmt.Sprintf("server shut down: err %s", err))
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := mockSQS.WaitForAllDeletes(ctx); err != nil {
		t.Errorf(err.Error())
	}

	// give Recievers go routines some time to finish their job
	ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	srv.Shutdown(ctx) // shutdown shuts Server right away and stops Recive go routines after ^^ timeout

	if r.received[0] != "12345" || r.received[1] != "文字材料" {
		t.Errorf("Instead of the expected processing order got %v", r.received)
	}
}

func TestServer_Serve_retries(t *testing.T) {
	retries := make([]*http.Request, 0, 3)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := ioutil.ReadAll(r.Body)
		t.Logf("Request: %s\n", b)
		retries = append(retries, r)
		w.WriteHeader(403)
		fmt.Fprintln(w, `
			{
    			"__type": "com.amazonaws.sqs#InvalidClientTokenId",
                "message": "The security token included in the request is invalid."
			}
		`)
	}))
	defer ts.Close()

	os.Setenv("SQS_ENDPOINT", ts.URL)
	os.Setenv("AWS_ACCESS_KEY_ID", "foo")
	os.Setenv("AWS_SECRET_ACCESS_KEY", "bar")

	defer func() {
		os.Unsetenv("SQS_ENDPOINT")
		os.Unsetenv("AWS_ACCESS_KEY_ID")
		os.Unsetenv("AWS_SECRET_ACCESS_KEY")
	}()

	cases := []struct {
		name     string
		options  []Option
		numTries int
	}{
		{"default", nil, 8},
		{"1 retry", []Option{WithRetries(0, 1)}, 2},
		{"No retries", []Option{WithRetries(0, 0)}, 1},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			retries = make([]*http.Request, 0, 3)
			srv, err := NewServer(ts.URL+"/queue", 1, 1, c.options...)
			if err != nil {
				t.Errorf("Server creation should not fail: %s", err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				if err := srv.Shutdown(ctx); err != nil {
					t.Logf("server shutdown failed %s", err)
				}
				cancel()
			}()

			r := &SimpleReceiver{t: t}
			err = srv.Serve(r)
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

// TestServer_ServeConcurrency tests that an SQS server can process a lot of
// messages using many concurrent goroutines.
func TestServer_Concurrency(t *testing.T) {
	msgs := newSQSMessages(10000)
	mockSQS := newMockSQSAPI(msgs, t)
	srv := newMockServer(100, mockSQS)

	go func() {
		r := &SimpleReceiver{t: t}
		if err := srv.Serve(r); err != nil {
			t.Errorf("server died %s", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := mockSQS.WaitForAllDeletes(ctx); err != nil {
		t.Fatalf(err.Error())
	}
}

// TestServer_Serve tests that an SQS server can receive messages, process
// them, and delete them from the queue successfully.
func TestServer_ServeFailingReceiver(t *testing.T) {
	msgs := newSQSMessages(1)
	mockSQS := newMockSQSAPI(msgs, t)
	srv := newMockServer(1, mockSQS)

	go func() {
		r := &FailingReceiver{t: t}
		if err := srv.Serve(r); err != nil {
			t.Errorf("server died %s", err)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := mockSQS.WaitForVisibilityTimeouts(ctx); err != nil {
		t.Errorf(err.Error())
	}
}

// Test conversion of sqs.Message.Attributes to msg.Attributes.
func TestServer_ConvertToMsgAttrs(t *testing.T) {
	val1 := "val1"
	val2 := "val2"
	str := "Attribute Test"
	awsMsg := &sqs.Message{
		Body: &str,
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"key1": {StringValue: &val1},
			"key2": {StringValue: &val2},
		},
	}
	serv := &Server{}
	attrs := msg.Attributes{}
	serv.convertToMsgAttrs(attrs, awsMsg.MessageAttributes)
	if attrs.Get("key1") != val1 {
		t.Error("key1 does not match")
	}
	if attrs.Get("key2") != val2 {
		t.Error("key2 does not match")
	}
}

// Test conversion of sqs.Message.Attributes to msg.Attributes.
func TestServer_ConvertToAttrs(t *testing.T) {
	val1 := "val1"
	val2 := "val2"
	str := "Attribute Test"
	awsMsg := &sqs.Message{
		Body: &str,
		Attributes: map[string]*string{
			"key1": &val1,
			"key2": &val2,
		},
	}
	serv := &Server{}
	attrs := msg.Attributes{}
	serv.convertToAttrs(attrs, awsMsg.Attributes)
	if attrs.Get("key1") != val1 {
		t.Error("key1 does not match")
	}
	if attrs.Get("key2") != val2 {
		t.Error("key2 does not match")
	}
}

// Tests that ErrServerClosed when all go routines finish before the context
// cancels.
func TestServer_ShutdownClean(t *testing.T) {
	msgs := newSQSMessages(10)
	srv := newMockServer(1, newMockSQSAPI(msgs, t))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	go func() {
		r := &SimpleReceiver{t: t}
		if err := srv.Serve(r); err != msg.ErrServerClosed {
			t.Logf("server died %s", err)
		}
	}()

	err := srv.Shutdown(ctx)
	if err != msg.ErrServerClosed {
		t.Errorf("Expected ErrServerClosed, got %v", err)
	}
}

// Tests that srv.Shutdown() shuts down when the passed context is
// canceled (in this case, by timeout).
func TestServer_ShutdownHard(t *testing.T) {
	msgs := newSQSMessages(100)
	srv := newMockServer(1, newMockSQSAPI(msgs, t))
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	go func() {
		r := &SimpleReceiver{t: t}
		if err := srv.Serve(r); err != msg.ErrServerClosed {
			t.Logf("server died %s", err)
		}
	}()

	err := srv.Shutdown(ctx)
	if err != context.DeadlineExceeded {
		t.Errorf("Expected context.DeadlineExceeded, got %v", err)
	}
}

func TestWithRetryJitter_SetsValidJitter(t *testing.T) {
	jitter := 10
	msgs := newSQSMessages(0)
	srv := newMockServer(1, newMockSQSAPI(msgs, t))
	optionFunc := WithRetryJitter(int64(jitter))
	err := optionFunc(srv)
	if err != nil {
		t.Errorf("Unexpected error %s", err)
	}
	if srv.retryJitter != int64(jitter) {
		t.Errorf("Expected retryJitter to be %d", jitter)
	}
}

func TestWithRetryJitter_ErrorOnInvalidJitter(t *testing.T) {
	jitter := 1000
	msgs := newSQSMessages(0)
	srv := newMockServer(1, newMockSQSAPI(msgs, t))
	optionFunc := WithRetryJitter(int64(jitter))
	err := optionFunc(srv)
	if err == nil {
		t.Errorf("Expected error, received nil")
	}
	if !strings.HasPrefix(err.Error(), "invalid jitter:") {
		t.Errorf("expected error to start with 'invalid jitter:', error is '%s'", err)
	}
}

func TestGetVisiblityTimeout_NoJitter(t *testing.T) {
	var retryTimeout int64 = 100
	var jitter int64 = 0
	val := getVisiblityTimeout(retryTimeout, jitter)
	if val < (retryTimeout-jitter) || val > (retryTimeout+jitter) {
		t.Errorf("val should be in the interval %d±%d", retryTimeout, jitter)
	}
}

func TestGetVisiblityTimeout_ValidJitter(t *testing.T) {
	var retryTimeout int64 = 100
	var jitter int64 = 10
	val := getVisiblityTimeout(retryTimeout, jitter)
	if val < (retryTimeout-jitter) || val > (retryTimeout+jitter) {
		t.Errorf("val should be in the interval %d±%d", retryTimeout, jitter)
	}
}

func TestGetVisiblityTimeout_InvalidJitter(t *testing.T) {
	var retryTimeout int64 = 100
	var jitter int64 = 1000
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("Expected panic")
		}
	}()
	val := getVisiblityTimeout(retryTimeout, jitter)
	if val < (retryTimeout-jitter) || val > (retryTimeout+jitter) {
		t.Errorf("val should be in the interval %d±%d", retryTimeout, jitter)
	}
}
