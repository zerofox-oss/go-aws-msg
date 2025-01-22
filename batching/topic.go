package batching

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yurizf/go-aws-msg-costs-control/awsinterfaces"
	"github.com/yurizf/go-aws-msg-costs-control/partialbase64encode"
	sliceondisk "github.com/yurizf/slice-on-disk"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
// The maximum [message size]  is 262,144 bytes (256 KiB)
// https://docs.aws.amazon.com/sns/latest/api/API_MessageAttributeValue.html#
// All parts of the message attribute, including name, type,
// and value, are included in the message size restriction, which is currently 256 KB (262,144 bytes)

const LENGTH_OF_256K int = 262144

const SNS = "sns"
const SQS = "sqs"
const SEND_TIMEOUT = 3 * time.Second

const ENCODING_ATTRIBUTE_KEY = "Content-Transfer-Encoding"
const ENCODING_ATTRIBUTE_VALUE = "partially-base64-batch"

var MAX_MSG_LENGTH int = LENGTH_OF_256K - len(ENCODING_ATTRIBUTE_KEY) - len(ENCODING_ATTRIBUTE_VALUE) - FRAGMENT_HEADER_LENGTH

const DEFAULT_BATCH_TIMEOUT = 2 * time.Second

// it might be gob encoded. So, the fields are public
type msg struct {
	Placed  time.Time
	Payload string
}

var id = struct {
	mux sync.Mutex
	id  int64
}{}

func createID() string {
	id.mux.Lock()
	defer id.mux.Unlock()
	id.id++
	return fmt.Sprintf("topic-%d", id.id)
}

// BatchedTopic is an extension of the generic Batcher interface
// Multiple goroutines may invoke method on a Batcher simultaneously.
type Batcher interface {
	Append(payload string) error
	SetAttributes(attrs any)
	SetTopicTimeout(timeout time.Duration)

	ShutDown(ctx context.Context, drain ...bool) error

	ID() string
	DebugON()
	DebugOFF()
	Stats() (int, int, int)
}

type TopicStruct struct {
	queueType     string
	arnOrUrl      string
	id            string
	mux           sync.Mutex
	timeout       time.Duration
	snsClient     awsinterfaces.SNSPublisher
	snsAttributes map[string]*sns.MessageAttributeValue
	sqsClient     awsinterfaces.SQSSender
	sqsAttributes map[string]*sqs.MessageAttributeValue

	batch    []msg
	overflow sliceondisk.Slicer[msg]
	//batcchStrings that failed to be sent
	resend []string

	concurrency chan struct{}

	batcherCtx        context.Context    // context used to Batcher the life of batcher engine
	batcherCancelFunc context.CancelFunc // CancelFunc for the batch engine go routines

	wg sync.WaitGroup

	batchLength int

	debug                bool
	draining             bool
	preparedMsgCount     int64
	preparedBatchesCount int64
	sentMsgCount         int64
	sentBatchesCount     int64
}

func (t *TopicStruct) ID() string {
	return t.id
}

// SetTopicTimeout - updates the timeout used to fire batched messages for a topic
// New should have been called for the topic prior to this call
func (t *TopicStruct) SetTopicTimeout(timeout time.Duration) {
	t.timeout = timeout
}

// SetAttributes - sets a single attributes set for ALL queued msgs of a topic.
// New should have been called for the topic prior to this call
func (t *TopicStruct) SetAttributes(attrs any) {

	t.mux.Lock()
	defer t.mux.Unlock()

	switch attrs.(type) {
	case map[string]*sns.MessageAttributeValue:
		t.snsAttributes = attrs.(map[string]*sns.MessageAttributeValue)
	case map[string]*sqs.MessageAttributeValue:
		t.sqsAttributes = attrs.(map[string]*sqs.MessageAttributeValue)
	}
}

func (t *TopicStruct) tryToAppend(m msg) bool {
	toAdd := fragmentLen(m.Payload) // + length of the whole batch
	if t.batchLength+toAdd > MAX_MSG_LENGTH {
		return false
	}

	t.batch = append(t.batch, m)
	t.batchLength = t.batchLength + toAdd
	return true
}

// Append - batch analogue of "send". Adds the Payload to the current batch
// Payload must be already partially base64 encoded!
func (t *TopicStruct) Append(payload string) error {
	payload = partialbase64encode.Encode(payload)
	switch {
	case len(payload) > MAX_MSG_LENGTH:
		return fmt.Errorf("message is too long: %d", len(payload))
	case len(payload) == 0:
		return fmt.Errorf("message is empty")
	}

	m := msg{time.Now(), payload}
	t.mux.Lock()
	defer t.mux.Unlock()
	if !t.tryToAppend(m) {
		return t.overflow.Append(m)
	}

	// don't send from here. It's cleaner to send from one place: engine
	return nil
}

func (t *TopicStruct) send(payload string) error {
	// from the tests, 500*time.Millisecond timout seems to be insufficient on messages 100K+ in size
	ctx, cancel := context.WithTimeout(context.Background(), SEND_TIMEOUT)
	defer cancel()

	var err error = nil
	switch t.queueType {
	case SNS:
		t.snsAttributes[ENCODING_ATTRIBUTE_KEY] = &sns.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(strings.Join([]string{ENCODING_ATTRIBUTE_VALUE}, ",")),
		}

		params := &sns.PublishInput{
			Message:  aws.String(payload),
			TopicArn: aws.String(t.arnOrUrl),
		}

		if len(t.snsAttributes) > 0 {
			params.MessageAttributes = t.snsAttributes
		} else {
			// sanity check: this should never happen:
			return errors.New("expected content transfer attribute is missing")
		}

		for i := 0; i < 3; i++ {
			_, err = t.snsClient.PublishWithContext(ctx, params)

			if err != nil {
				log.Printf("[ERROR] %s: error sending message of %d bytes with timeout %s to sns %s: %s", t.id, len(payload), 3*time.Second, t.arnOrUrl, err.Error())
				time.Sleep(time.Duration(int64((i+1)*100) * int64(time.Millisecond)))
				continue
			}
			break
		}
	case SQS:
		t.sqsAttributes[ENCODING_ATTRIBUTE_KEY] = &sqs.MessageAttributeValue{
			DataType:    aws.String("String"),
			StringValue: aws.String(strings.Join([]string{ENCODING_ATTRIBUTE_VALUE}, ",")),
		}

		params := &sqs.SendMessageInput{
			MessageBody: aws.String(payload),
			QueueUrl:    aws.String(t.arnOrUrl),
		}

		if len(t.sqsAttributes) > 0 {
			params.MessageAttributes = t.sqsAttributes
		}

		for i := 0; i < 3; i++ {
			_, err = t.sqsClient.SendMessageWithContext(ctx, params)
			if err != nil {
				log.Printf("[ERROR] %s: error sending message of %d bytes to sqs %s: %s", t.id, len(payload), t.arnOrUrl, err.Error())
				time.Sleep(time.Duration(int64((i+1)*100) * int64(time.Millisecond)))
				continue
			}
			break
		}
	}

	return err
}

func (t *TopicStruct) DebugON() {
	t.debug = true
}

func (t *TopicStruct) DebugOFF() {
	t.debug = false
}

// New creates and initializes the batching the engine data structures for a specific c sns/sqs.Topic
//
// It accepts the topic ARN,
// an SNSPublisher or SQSSender interface instance (implemented as AWS SNS or SQS clients).
// and the timeout value for this topic: upon its expiration the batch will be sendMessages to the topic
// generics with unions referencing interfaces with methods are not currently supported. Hence, any and type assertions.
// https://github.com/golang/go/issues/45346#issuecomment-862505803
func New(topicARN string, p any, timeout time.Duration, concurrency ...int) (Batcher, error) {

	overflow, err := sliceondisk.New(make([]msg, 0, 512), os.TempDir())
	if err != nil {
		return nil, err
	}

	topic := TopicStruct{
		timeout:  timeout,
		arnOrUrl: topicARN,

		batch: make([]msg, 0, 128),
		// overflow: make([]msg, 0, 128),
		overflow: overflow,
		resend:   make([]string, 0, 128),
	}

	if len(concurrency) == 0 {
		topic.concurrency = make(chan struct{}, 10)
	} else {
		topic.concurrency = make(chan struct{}, concurrency[0])
	}

	switch v := p.(type) {
	case awsinterfaces.SNSPublisher:
		topic.snsClient = v
		topic.snsAttributes = make(map[string]*sns.MessageAttributeValue)
		topic.queueType = SNS
	case awsinterfaces.SQSSender:
		topic.sqsClient = v
		topic.sqsAttributes = make(map[string]*sqs.MessageAttributeValue)
		topic.queueType = SQS
	default:
		return nil, errors.New("Invalid client of unexpected type passed")
	}

	topic.batcherCtx, topic.batcherCancelFunc = context.WithCancel(context.Background())

	topic.id = createID()

	// this go routine is the sending engine for this topic
	// So, if the topic is used by multiple threads, only one instance of this routine runs.
	// if each thread created own topic for this arn/url, they won't collide.
	// either way it works
	log.Printf("[INFO] created batcher %s. Starting its batching engine...", topic.id)
	topic.wg.Add(1)
	go func() {
		defer topic.wg.Done()
		for {
			select {
			case <-topic.batcherCtx.Done():
				log.Printf("[INFO] %s: batching engine is shutting down. queued Payload length is %d; overflow %d; resend %d", topic.id, len(topic.batch), topic.overflow.Len(), len(topic.resend))
				close(topic.concurrency)
				return

			case <-time.After(100 * time.Millisecond):

				// first, resend failed on send msgs
				// these are batches, not messages: ready to send bytes.
				if len(topic.resend) > 0 {

					tmp := make([]string, 0, 128)
					for _, v := range topic.resend {
						// concurrency limit how many threads will hit SNS/SQS endpoint simultaneously
						topic.concurrency <- struct{}{}
						topic.wg.Add(1)
						go func(s string) {
							defer func() {
								<-topic.concurrency
							}()
							defer topic.wg.Done()

							log.Printf("[DEBUG] %s: resending failed %d bytes to %s", topic.id, len(s), topic.arnOrUrl)
							if err := topic.send(s); err != nil {
								log.Printf("[DEBUG] %s: failed to resend %d bytes to %s: %s", topic.id, len(s), topic.arnOrUrl, err.Error())
								topic.mux.Lock()
								tmp = append(tmp, s)
								topic.mux.Unlock()
							}
						}(v)
					}
					topic.resend = tmp
				}

				if len(topic.batch) > 0 && time.Now().Sub(topic.batch[0].Placed) > topic.timeout {
					log.Printf("[TRACE] sending batch of size %d", len(topic.batch))
					s := topic.buildPayload()

					topic.concurrency <- struct{}{}
					// make it a go routine to unblock top level select
					// even tho we spawn only one go routine, we limit concurrency b/c we are in the loop
					topic.wg.Add(1)
					go func(payload string) {
						defer func() {
							<-topic.concurrency
						}()
						defer topic.wg.Done()

						err := topic.send(payload)

						topic.mux.Lock()
						defer topic.mux.Unlock()

						if err != nil {
							topic.resend = append(topic.resend, s)
						} else {
							topic.sentMsgCount = topic.preparedMsgCount
							topic.sentBatchesCount++
						}
					}(s)
				}
			}
		}
	}()

	return &topic, nil
}

func (t *TopicStruct) buildPayload() string {
	t.mux.Lock()
	defer t.mux.Unlock()

	t.preparedBatchesCount++
	t.preparedMsgCount += int64(len(t.batch))

	var sb strings.Builder
	for _, m := range t.batch {
		sb.WriteString(partialbase64encode.PrefixWithLength(m.Payload))
	}

	t.batch = make([]msg, 0, 128)
	t.batchLength = 0
	t.processOverflow()

	return sb.String()
}

func (t *TopicStruct) processOverflow() int {
	copied := 0
	if t.overflow.Len() > 0 {
		for i := 0; i < t.overflow.Len(); i++ {
			o, err := t.overflow.Get(i)
			if err != nil {
				log.Printf("[ERROR] retreiving %d overflow element: %s", i, err.Error())
				return copied
			}
			if t.tryToAppend(o) {
				copied++
				continue
			}
			break
		}
		// t.overflow = shiftLeft(t.overflow, copied)
		err := t.overflow.Delete(0, copied)
		if err != nil {
			log.Printf("[ERROR] deleting %d from overflow of length %d: %s", copied, t.overflow.Len(), err.Error())
		}
	}
	return copied
}

// Shutdown stops the batching engine and stops its go routine
// by calling cancel on the batcher context.
// It expects a context with a timeout to be passed to delay the shutdown
// so that all already accumulated messages could be sent.
func (t *TopicStruct) ShutDown(ctx context.Context, drain ...bool) error {
	if ctx == nil {
		panic("context not set in shutdown batcher")
	}

	if len(drain) > 0 {
		t.draining = drain[0]
	}

	// wait till all generated have been sent
	ticker := time.NewTicker(15 * time.Second)
	keepGoing := true
	cnt := 0
	for keepGoing {
		select {
		case <-ticker.C: //blocking
			cnt++
			if cnt < 240 {
				log.Printf("[TRACE]. up to 1 hour of wait for all batches to be sent expired")
				if len(t.batch) > 0 || t.overflow.Len() > 0 || len(t.resend) > 0 {
					log.Printf("[ERROR].  %s is not done yet: batched:%d, overflow:%d, resend:%dd", t.id, len(t.batch), t.overflow.Len(), len(t.resend))
				} else {
					log.Printf("[INFO].  %s is done", t.id)
					keepGoing = false
				}
			} else {
				log.Printf("[INFO].  %s draining for 1 hour. exiting", t.id)
				keepGoing = false
			}
		}
	}

	t.overflow.Cleanup()

	deadline, ok := ctx.Deadline()
	log.Printf("[INFO] %s *********************debug is %t ******************", t.id, t.debug)
	if ok {
		log.Printf("[INFO] %s: *** shutting down topic's batcher...in %s", t.id, deadline.Sub(time.Now()))
	} else {
		log.Printf("[INFO] %s: *** shutting down topic's batcher...", t.id)
	}

	for {
		select {
		case <-ctx.Done():
			t.batcherCancelFunc()

			log.Printf("[INFO] %s: *** waiting for topic's go routines to finish....", t.id)
			t.wg.Wait()
			log.Printf("[INFO] %s **** cost savings (after deadline): (prepared/sent) messages: %d/%d, batches: %d/%d", t.id, t.preparedMsgCount, t.sentMsgCount, t.preparedBatchesCount, t.sentBatchesCount)
			log.Printf("[INFO] %s: *** topic's go routines finished: in batch:%d, in overflow:%d, in resend:%d", t.id, len(t.batch), t.overflow.Len(), len(t.resend))
			log.Printf("[INFO] %s ***************************************", t.id)
			return ctx.Err()
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

func (t *TopicStruct) Stats() (int, int, int) {
	return len(t.batch), t.overflow.Len(), len(t.resend)
}
