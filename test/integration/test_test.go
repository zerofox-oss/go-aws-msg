package integration

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yurizf/go-aws-msg-costs-control/awsinterfaces"
	"github.com/yurizf/go-aws-msg-costs-control/batching"
	sns2 "github.com/yurizf/go-aws-msg-costs-control/sns"
	sqs2 "github.com/yurizf/go-aws-msg-costs-control/sqs"
	"github.com/yurizf/go-aws-msg-costs-control/test/common"
	"github.com/zerofox-oss/go-msg"
	"io"
	"strings"
	"sync"
	// "sync/atomic"
	"testing"
	"time"
)

const TOPICS_NUM = 3
const MSG_PER_TOPIC = 1000
const TOPIC_TIMEOUT_TO_DRAIN_BATCH_QUEUE = 30 * time.Second
const SQS_SRV_SHUTDOWN_TIMEOUT = 10 * time.Second

type singletonQueue struct {
	mux          sync.Mutex
	first        int
	msgs         []string
	receivedMsgs map[string]string
	deletedMsgs  map[string]string
}

type mockSNSSQS struct {
	sess  *session.Session
	queue *singletonQueue
}

var theQueue singletonQueue = singletonQueue{
	msgs:         make([]string, 0, 2048),
	receivedMsgs: make(map[string]string),
	deletedMsgs:  make(map[string]string),
}

func (m *mockSNSSQS) PublishWithContext(ctx aws.Context, in *sns.PublishInput, opts ...request.Option) (*sns.PublishOutput, error) {

	m.queue.mux.Lock()
	defer m.queue.mux.Unlock()
	m.queue.msgs = append(m.queue.msgs, *in.Message)
	return nil, nil
}

func (m *mockSNSSQS) ReceiveMessage(in *sqs.ReceiveMessageInput) (*sqs.ReceiveMessageOutput, error) {
	m.queue.mux.Lock()
	defer m.queue.mux.Unlock()

	l64 := *in.MaxNumberOfMessages
	if l64 > int64(len(m.queue.msgs)) {
		l64 = int64(len(m.queue.msgs))
	}
	max := int(l64)

	out := sqs.ReceiveMessageOutput{
		Messages: make([]*sqs.Message, max),
	}

	atrrValue := batching.ENCODING_ATTRIBUTE_VALUE
	for i, v := range m.queue.msgs {
		msgID := fmt.Sprintf("%d", m.queue.first+i)
		s := v // assigning &v makes all output msgs the same.
		out.Messages[i] = &sqs.Message{
			MessageId:     &msgID,
			Body:          &s,
			Attributes:    map[string]*string{batching.ENCODING_ATTRIBUTE_KEY: &atrrValue},
			ReceiptHandle: &msgID,
		}
		m.queue.receivedMsgs[msgID] = v
		if i == max-1 {
			break
		}
	}
	m.queue.first = m.queue.first + max
	m.queue.msgs = m.queue.msgs[max:]

	return &out, nil
}

func (m *mockSNSSQS) ChangeMessageVisibility(in *sqs.ChangeMessageVisibilityInput) (*sqs.ChangeMessageVisibilityOutput, error) {
	return &sqs.ChangeMessageVisibilityOutput{}, nil
}

func (m *mockSNSSQS) DeleteMessage(in *sqs.DeleteMessageInput) (*sqs.DeleteMessageOutput, error) {
	m.queue.mux.Lock()
	defer m.queue.mux.Unlock()
	m.queue.deletedMsgs[*in.ReceiptHandle] = m.queue.receivedMsgs[*in.ReceiptHandle]
	delete(m.queue.receivedMsgs, *in.ReceiptHandle)
	return &sqs.DeleteMessageOutput{}, nil
}

type stats struct {
	mux   sync.Mutex
	count int
	msgs  map[int]string
	logs  []string
}

var trace = stats{
	count: 0,
	msgs:  make(map[int]string),
	logs:  make([]string, 0, 10),
}

func (m *stats) increment(str string) {
	m.mux.Lock()
	m.count++
	m.msgs[m.count] = str
	m.mux.Unlock()
}

func (m *stats) log(str string) {
	m.mux.Lock()
	m.logs = append(m.logs, str)
	m.mux.Unlock()
}

func TestBoth(t *testing.T) {

	sns2.NewSNSPublisherFunc = func(sess *session.Session, cfgs ...*aws.Config) awsinterfaces.SNSPublisher {
		return &mockSNSSQS{
			sess:  sess,
			queue: &theQueue,
		}
	}

	sqs2.NewSQSReceiverFunc = func() (awsinterfaces.SQSReceiver, *session.Session, error) {
		sess, err := session.NewSession()
		if err != nil {
			return nil, nil, err
		}
		return &mockSNSSQS{
			sess:  sess,
			queue: &theQueue,
		}, sess, nil
	}

	// senders
	var wg sync.WaitGroup
	for i := 0; i < TOPICS_NUM; i++ {
		wg.Add(1)
		go func(j int) {
			defer func(j int) {
				wg.Done()
				t.Logf("exiting topic %d sender", j)
			}(j)
			topic, err := sns2.NewBatchedTopic("blah")
			if err != nil {
				t.Errorf("cannot create batched topic blah: %v", err)
				return
			}
			topic.DebugON()
			ctx := context.Background()
			for k := 0; k < MSG_PER_TOPIC; k++ {
				str := common.RandString(100, 15000)
				str = fmt.Sprintf("msg-%d:%s:%s", k, topic.ID(), str)
				// t.Logf("sending length %d: %s", len(str), str[:30])
				w := topic.NewWriter(ctx)
				_, err := w.Write([]byte(str))
				if err != nil {
					t.Errorf("[ERROR] %s Failed to write %d bytes into the msg writer: %s", topic.ID(), len(str), err)
					return
				}
				err = w.Close()
				if err != nil {
					t.Errorf("closing writer/publishing to SNS: %v", err)
				}
			}

			ctxt, cancel := context.WithTimeout(ctx, TOPIC_TIMEOUT_TO_DRAIN_BATCH_QUEUE)
			defer cancel()
			topic.ShutDown(ctxt)
		}(i)
	}

	// just wait ill everything has been sent
	wg.Wait()

	// receiver:
	sqsSrv, err := sqs2.NewServer("blah", 10, int64(30))
	if err != nil {
		t.Errorf("creating Server: %s", err)
		return
	}

	receiverFunc := msg.ReceiverFunc(
		func(ctx context.Context, message *msg.Message) error {
			data, _ := io.ReadAll(message.Body)
			str := string(data)
			// encoding := message.Attributes.Get(batching.ENCODING_ATTRIBUTE_KEY)
			// t.Logf("%d: received length %d encoded as %s: %s", goID, len(str), encoding, str[:30])
			if !strings.HasPrefix(str, "msg-") {
				t.Errorf("[ERROR] unrecohnized message %s", str)
			}

			trace.increment(str[:30])
			return nil
		},
	)

	// start SQS msg server. It's a blocking call
	go func() {
		err = sqsSrv.Serve(receiverFunc)
		if !errors.Is(err, msg.ErrServerClosed) {
			t.Logf("[ERROR] Server closed with an error: %s\n", err)
		}
		t.Logf("existing go func with Serve...")
	}()

	// stop condition: the queue is empty for 15 seconds
	ticker := time.NewTicker(5 * time.Second)
	countZeros := 0

checkQueue:
	for {
		msgsInFlight := len(theQueue.msgs)
		select {
		case <-ticker.C:
			t.Logf("%d of 15 seconds elapsed while waiting in ticker: number of messages in the queue is %d", countZeros*5, msgsInFlight)
			if msgsInFlight == 0 {
				countZeros++
				if countZeros > 3 {
					t.Logf("shutting down sqsSrv with the timeout %s", SQS_SRV_SHUTDOWN_TIMEOUT)
					ctxt, cancel := context.WithTimeout(context.Background(), SQS_SRV_SHUTDOWN_TIMEOUT)
					defer cancel()
					ticker.Stop()
					sqsSrv.Shutdown(ctxt)
					break checkQueue
				}
			}
		case <-time.After(1 * time.Second):
			t.Logf("tick-tok. still %d messages to receive", msgsInFlight)
			if msgsInFlight > 0 {
				countZeros = 0
			}
		}
	}

	t.Logf("******** RECEIVED MESSAGES %d **********", trace.count)
	if trace.count != TOPICS_NUM*MSG_PER_TOPIC {
		t.Logf("%d %v", trace.count, trace.msgs)
		t.Errorf("expected number of received messages %d not equal to actual %d", trace.count, TOPICS_NUM*MSG_PER_TOPIC)
	}
}
