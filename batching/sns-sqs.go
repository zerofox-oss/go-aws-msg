package batching

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	aws2 "github.com/yurizf/go-aws-msg-with-batching/awsinterfaces"
	"log/slog"
	"sync"
	"time"
)

const LARGEST_MSG_LENGTH int = 262144
const SNS = "sns"
const SQS = "sqs"

type msg struct {
	placed  time.Time
	payload string
}

func (m msg) byteLen() int {
	return batchLen([]string{m.payload})
}

// &{false {0 0} <nil> map[] map[] map[] map[] <nil>}
type control struct {
	queueType     string
	mux           map[string]*sync.Mutex
	timeouts      map[string]time.Duration
	snsClients    map[string]aws2.SNSPublisher
	snsAttributes map[string]map[string]*sns.MessageAttributeValue
	sqsClients    map[string]aws2.SQSSender
	sqsAttributes map[string]map[string]*sqs.MessageAttributeValue

	messages   map[string][]msg
	byteLength map[string]int
	resend     map[string][]string

	concurrency map[string]chan struct{}

	batcherCtx        context.Context    // context used to control the life of batcher engine
	batcherCancelFunc context.CancelFunc // CancelFunc for all receiver routines
}

var ctl control

// SetTopicTimeout - updates the timeout used to fire batched messages for a topic
// NewTopic should have been called for the topic prior to this call
func SetTopicTimeout(topicARN string, timeout time.Duration) {
	mux := ctl.mux[topicARN]
	mux.Lock()
	defer mux.Unlock()

	ctl.timeouts[topicARN] = timeout
}

// SetAttributes - sets a single attributes set for ALL queued msgs of a topic.
// NewTopic should have been called for the topic prior to this call
func SetAttributes(topicARN string, attrs any) {
	mux := ctl.mux[topicARN]
	mux.Lock()
	defer mux.Unlock()

	switch attrs.(type) {
	case map[string]*sns.MessageAttributeValue:
		ctl.snsAttributes[topicARN] = attrs.(map[string]*sns.MessageAttributeValue)
	case map[string]*sqs.MessageAttributeValue:
		ctl.sqsAttributes[topicARN] = attrs.(map[string]*sqs.MessageAttributeValue)
	}
}

// Append - batch analogue of "send". Adds the payload to the current batch
func Append(topicARN string, payload string) error {
	if len(payload) > LARGEST_MSG_LENGTH {
		return fmt.Errorf("message is too long: %d", len(payload))
	}

	newMsg := msg{placed: time.Now(), payload: payload}

	byteLength := ctl.byteLength[topicARN]

	slog.Debug(fmt.Sprintf("appending %d to %d", newMsg.byteLen(), byteLength))

	if byteLength+newMsg.byteLen() > LARGEST_MSG_LENGTH {
		slog.Debug(fmt.Sprintf("reached max SNS msg size, sending %d bytes", byteLength))
		// mux is locked
		err := sendMessages(ctl.batcherCtx, topicARN, byteLength)
		if err != nil {
			slog.Error(fmt.Sprintf("Error sending msg to %s: %v", topicARN, err))
		}
	}

	ctl.messages[topicARN] = append(ctl.messages[topicARN], newMsg)
	ctl.byteLength[topicARN] = ctl.byteLength[topicARN] + newMsg.byteLen()

	return nil
}

func sendMessages(parentCtx context.Context, topicARN string, expectedByteLength int) error {
	messages, ok := ctl.messages[topicARN]
	if !ok {
		return fmt.Errorf("No such topic: %s", topicARN)
	}

	// since [topicArn] maps have not been locked, it's possible that
	// another thread sendMessages and reset the msg slice.
	if ctl.byteLength[topicARN] < expectedByteLength {
		return nil
	}

	mux := ctl.mux[topicARN]
	mux.Lock()

	payloads := make([]string, len(messages))
	for i, msg := range messages {
		payloads[i] = msg.payload
	}

	ctl.messages[topicARN] = make([]msg, 0, 128)
	ctl.byteLength[topicARN] = 0

	mux.Unlock()

	payload := Batch(payloads)

	err := send(parentCtx, topicARN, payload)

	if err != nil {
		mux := ctl.mux[topicARN]
		mux.Lock()
		ctl.resend[topicARN] = append(ctl.resend[topicARN], payload)
		mux.Unlock()
	}

	return err
}

func send(parentCtx context.Context, topicARN string, payload string) error {
	ctx, cancel := context.WithTimeout(parentCtx, 500*time.Millisecond)
	defer cancel()

	var err error = nil
	switch ctl.queueType {
	case SNS:
		params := &sns.PublishInput{
			Message:  aws.String(payload),
			TopicArn: aws.String(topicARN),
		}
		attrs, ok := ctl.snsAttributes[topicARN]
		if ok && len(attrs) > 0 {
			params.MessageAttributes = attrs
		}

		for i := 0; i < 3; i++ {
			_, err = ctl.snsClients[topicARN].PublishWithContext(ctx, params)
			if err != nil {
				slog.Error("error while sending message to sns: " + topicARN + ": " + err.Error())
				time.Sleep(time.Duration(int64((i+1)*100) * int64(time.Millisecond)))
				continue
			}
			break
		}
	case SQS:
		params := &sqs.SendMessageInput{
			MessageBody: aws.String(payload),
			QueueUrl:    aws.String(topicARN),
		}
		attrs, ok := ctl.sqsAttributes[topicARN]
		if ok && len(attrs) > 0 {
			params.MessageAttributes = attrs
		}

		for i := 0; i < 3; i++ {
			_, err = ctl.sqsClients[topicARN].SendMessageWithContext(ctx, params)
			if err != nil {
				slog.Error("error while sending message to sqs: " + topicARN + ": " + err.Error())
				time.Sleep(time.Duration(int64((i+1)*100) * int64(time.Millisecond)))
				continue
			}
			break
		}
	}

	return err
}

// NewTopic creates and initializes the batching the engine data structures for a specific c sns/sqs.Topic
//
// It accepts the topic ARN,
// an SNSPublisher or SQSSender interface instance (implemented as AWS SNS or SQS clients).
// and the timeout value for this topic: upon its expiration the batch will be sendMessages to the topic
// generics with unions referencing interfaces with methods are not currently supported. Hence, any and type assertions.
// https://github.com/golang/go/issues/45346#issuecomment-862505803
func NewTopic(topicARN string, p any, timeout time.Duration, concurrency ...int) error {
	_, ok := ctl.mux[topicARN]
	if ok {
		return nil // topic was already created by another thread. Leave it as is
	}

	var m sync.Mutex
	ctl.mux[topicARN] = &m
	ctl.timeouts[topicARN] = timeout
	ctl.messages[topicARN] = make([]msg, 0, 128)
	ctl.resend[topicARN] = make([]string, 0, 128)
	ctl.byteLength[topicARN] = 0
	if len(concurrency) == 0 {
		ctl.concurrency[topicARN] = make(chan struct{}, 10)
	} else {
		ctl.concurrency[topicARN] = make(chan struct{}, concurrency[0])
	}

	switch v := p.(type) {
	case aws2.SNSPublisher:
		ctl.snsClients[topicARN] = v
		ctl.snsAttributes[topicARN] = make(map[string]*sns.MessageAttributeValue)
	case aws2.SQSSender:
		ctl.sqsClients[topicARN] = v
		ctl.sqsAttributes[topicARN] = make(map[string]*sqs.MessageAttributeValue)
	default:
		return errors.New("Invalid client of unexpected type passed")
	}

	return nil
}

type HighWaterMark struct {
	TimeStamp time.Time
	Number    int
	Length    int
	Topic     string
}

type HWMStats struct {
	MessagesTotalHWM HighWaterMark
	FailedTotalHWM   HighWaterMark
	MessagesHWM      map[string]HighWaterMark
	FailedHWM        map[string]HighWaterMark
}

var HWMStatsSingleton HWMStats

// New creates and initializes a batching engine using the type string that can have values "sns" or "sqs".
// depending on which client the application needs.
//
// It spawns a go routine that sends message batches if the waiting period(timeout) for extra messages
// has expired.
func New(queueType string) error {
	// init stats
	HWMStatsSingleton.MessagesHWM = make(map[string]HighWaterMark)
	HWMStatsSingleton.FailedHWM = make(map[string]HighWaterMark)

	ctl = control{
		queueType:   queueType,
		mux:         make(map[string]*sync.Mutex),
		timeouts:    make(map[string]time.Duration),
		messages:    make(map[string][]msg),
		byteLength:  make(map[string]int),
		resend:      make(map[string][]string),
		concurrency: make(map[string]chan struct{}), // unbuffered
	}
	ctl.batcherCtx, ctl.batcherCancelFunc = context.WithCancel(context.Background())

	switch queueType {
	case SNS:
		ctl.snsClients = make(map[string]aws2.SNSPublisher)
		ctl.snsAttributes = make(map[string]map[string]*sns.MessageAttributeValue)
	case SQS:
		ctl.sqsClients = make(map[string]aws2.SQSSender)
		ctl.sqsAttributes = make(map[string]map[string]*sqs.MessageAttributeValue)
	default:
		slog.Error("invalid queueType passed")
		return fmt.Errorf("invalid queueType passed %s", queueType)
	}

	go func() {
		for {
			select {
			case <-ctl.batcherCtx.Done():
				slog.Info("batcher is shutting down")
				for _, v := range ctl.concurrency {
					close(v)
				}
				return
			case <-time.After(100 * time.Millisecond):

				totalMsgHWM := HighWaterMark{}
				totalMsgHWM.TimeStamp = time.Now()
				totalHWM := HighWaterMark{}
				totalHWM.TimeStamp = time.Now()

				for k, v := range ctl.resend {

					ctl.concurrency[k] <- struct{}{}

					tmpHWM := HighWaterMark{}
					tmpHWM.TimeStamp = time.Now()
					tmpHWM.Number = len(v)

					totalHWM.Number = totalHWM.Number + len(v)

					// Loop variables captured by 'func' literals in 'go' statements might have unexpected values
					go func(k string, v []string) {
						defer func() {
							<-ctl.concurrency[k]
						}()

						tmp := make([]string, 0, len(v))
						for _, r := range v {
							err := send(ctl.batcherCtx, k, r)
							if err != nil {
								tmp = append(tmp, r)
							}
						}

						ctl.resend[k] = tmp
					}(k, v)

					hwm, ok := HWMStatsSingleton.FailedHWM[k]
					if !ok || hwm.Number < tmpHWM.Number {
						HWMStatsSingleton.FailedHWM[k] = tmpHWM
					}

				}

				if HWMStatsSingleton.FailedTotalHWM.Number < totalHWM.Number {
					HWMStatsSingleton.FailedTotalHWM = totalHWM
				}

				totalHWM = HighWaterMark{}
				totalHWM.TimeStamp = time.Now()

				for k, v := range ctl.messages {
					tmpHWM := HighWaterMark{}
					tmpHWM.TimeStamp = time.Now()
					tmpHWM.Number = len(v)
					tmpHWM.Length = ctl.byteLength[k]
					totalHWM.Number = totalHWM.Number + len(v)
					totalHWM.Length = totalHWM.Length + ctl.byteLength[k]

					if len(v) > 0 && time.Now().Sub(v[0].placed) > ctl.timeouts[k] {

						ctl.concurrency[k] <- struct{}{}

						slog.Debug(fmt.Sprintf("sending a Batch of %d on timeout to topic %s", len(ctl.messages[k]), k))
						go func(ctx context.Context, topic string, length int) {
							// free concurrency slot for the topic
							defer func() {
								<-ctl.concurrency[topic]
							}()

							sendMessages(ctx, topic, length)

						}(ctl.batcherCtx, k, ctl.byteLength[k])

					}

					hwm, ok := HWMStatsSingleton.MessagesHWM[k]
					if !ok || tmpHWM.Number > hwm.Number || tmpHWM.Length > hwm.Length {
						HWMStatsSingleton.MessagesHWM[k] = tmpHWM
					}
				}

				if totalHWM.Length > HWMStatsSingleton.FailedTotalHWM.Length || totalHWM.Number > HWMStatsSingleton.FailedTotalHWM.Number {
					HWMStatsSingleton.MessagesTotalHWM = totalHWM
				}
			}
		}
	}()

	return nil
}

// Shutdown stops the batching engine and stops its go routine
// by calling cancel on the batcher context.
// It expects a context with a timeout to be passed to delay the shutdown
// so that all already accumulated messages could be sent.
func ShutDown(ctx context.Context) error {
	if ctx == nil {
		panic("context not set in shutdown batcher")
	}
	slog.Info("shutting down batcher...")

	for {
		select {
		case <-ctx.Done():
			ctl.batcherCancelFunc()
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			return ctx.Err()
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

type Stats struct {
	HWM           HWMStats
	CurrentFailed map[string]int
	CurrentQueued map[string]int
}

// Stats - returns batching stats that can be used in Promethius or other metrics
func GetStats() *Stats {
	stats := Stats{}
	stats.HWM = HWMStatsSingleton

	stats.CurrentFailed = make(map[string]int)
	for k, v := range ctl.resend {
		stats.CurrentFailed[k] = len(v)
	}

	stats.CurrentQueued = make(map[string]int)
	for k, v := range ctl.messages {
		stats.CurrentQueued[k] = len(v)
	}

	return &stats
}
