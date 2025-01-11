package batching

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	aws2 "github.com/yurizf/go-aws-msg-costs-control/awsinterfaces"
	"testing"
	"time"
)

type mockPublisher struct {
	p func(aws.Context, *sns.PublishInput, ...request.Option) (*sns.PublishOutput, error)
}

func (m mockPublisher) PublishWithContext(ctx aws.Context, input *sns.PublishInput, opts ...request.Option) (*sns.PublishOutput, error) {
	return m.p(ctx, input, opts...)
}

func Test_SNS(t *testing.T) {

	payloads := []string{
		"12345",
		"书、杂志等中区别",
		"于图片的）正文",
		"文字材料",
		"абвгдежзиклмн",
	}

	type args struct {
		publisher aws2.SNSPublisher
		payloads  []string
		topic     string
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "test-publish-on-timeout",
			args: args{
				payloads: payloads,
				publisher: mockPublisher{
					p: func(ctx aws.Context, input *sns.PublishInput, _ ...request.Option) (*sns.PublishOutput, error) {
						msgs, err := DeBatch(*input.Message)
						if err != nil {
							t.Fatalf("Failed to debatch the payload %v", err)
						}
						if len(msgs) != 5 {
							t.Fatalf("Wrong number of messages. Expected 5. Got %d", len(msgs))
						}
						if msgs[0] != "12345" {
							t.Fatalf("Unexpected msgs[0] %s", msgs[0])
						}
						if msgs[1] != "书、杂志等中区别" {
							t.Fatalf("Unexpected msgs[0] %s", msgs[0])
						}

						return nil, nil
					},
				},
			},
			wantErr: false,
		},

		{
			name: "test-publish-on-reaching-250K-SNS-msh-limit",
			args: args{
				payloads: payloads,
				publisher: mockPublisher{
					p: func(ctx aws.Context, input *sns.PublishInput, _ ...request.Option) (*sns.PublishOutput, error) {
						switch len(*input.Message) {
						case 262125, 61875:
							t.Log(fmt.Sprintf("Expected buffer length received %d", len(*input.Message)))
						default:
							t.Fatalf("Expected buffer size of 262125. Got %d", len(*input.Message))
						}

						return nil, nil
					},
				},
			},
			wantErr: false,
		},
	}

	//for _, tt := range tests {

	//}
	tt := tests[0]
	t.Run(tt.name, func(t *testing.T) {

		t.Log(tt.name)
		topic, err := New("fake-topic", tt.args.publisher, 1*time.Second)
		if err != nil {
			t.Errorf("could not create topic: %s", err)
		}

		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			topic.ShutDown(ctx)
			defer cancel()
		}()

		//SetTopicTimeout("fake-topic", 1*time.Second)

		for _, msg := range tt.args.payloads {
			if err = topic.Append(msg); err != nil {
				t.Errorf("could not append msg %s to topic: %s", msg, err)
			}
		}

		// time.Sleep(3 * time.Second)

	})

	tt = tests[1]
	t.Run(tt.name, func(t *testing.T) {
		t.Log(tt.name)

		topic, err := New("fake-topic-1", tt.args.publisher, 30*time.Second)
		if err != nil {
			t.Errorf("could not create topic: %s", err)
		}
		defer func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			topic.ShutDown(ctx)
			defer cancel()
		}()
		//SetTopicTimeout("fake-topic-1", 30*time.Second)

		for i := 0; i < 3000; i = i + 1 {
			for _, msg := range tt.args.payloads {
				topic.Append(msg)
			}
		}

		// wait till the remainder of msgs is sent on timeout reached
		// time.Sleep(40 * time.Second)
		t.Log(tt.name)
	})

}
