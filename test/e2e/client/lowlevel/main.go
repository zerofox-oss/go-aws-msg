package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yurizf/go-aws-msg-costs-control/batching"
	"github.com/yurizf/go-aws-msg-costs-control/partialbase64encode"
	"github.com/yurizf/go-aws-msg-costs-control/test/common"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const insertGeneratedSQL = "INSERT INTO client (l, md5, msg) VALUES ($1,$2, $3)"
const insertFailedSQL = "INSERT INTO too_long (olen, original, elen, encoded) VALUES ($1,$2,$3,$4)"

var config = struct {
	numberOfMessages   int
	numberOfGoRoutines int
	minLen             int
	maxLen             int
	topicARN           string
	doDB               bool
	dbURL              string
	awsConfig          *aws.Config
}{1000, 10, 100, 150000, "", false, "", nil}

func main() {
	if r := os.Getenv("TOTAL_MESSAGES"); r != "" {
		config.numberOfMessages, _ = strconv.Atoi(r)
	}

	if r := os.Getenv("CONCURRENCY"); r != "" {
		config.numberOfGoRoutines, _ = strconv.Atoi(r)
	}

	if r := os.Getenv("MIN_MSG_LEN"); r != "" {
		config.minLen, _ = strconv.Atoi(r)
	}

	if r := os.Getenv("MAX_MSG_LEN"); r != "" {
		config.maxLen, _ = strconv.Atoi(r)
	}

	if r := os.Getenv("TOPIC_ARN"); r != "" {
		config.topicARN = r
	}

	if r := os.Getenv("DO_DB"); r != "" && r != "0" && strings.ToLower(r) != "no" {
		config.doDB = true

		if r := os.Getenv("DB_URL"); r != "" {
			config.dbURL = r
		}
	}

	config.awsConfig = &aws.Config{
		Credentials: credentials.NewCredentials(&credentials.EnvProvider{}),
		Region:      aws.String("us-west-2"),
	}

	fmt.Println("Configuration", config)

	var pgConn *pgxpool.Pool
	var err error
	var dbCtx context.Context
	if config.doDB {
		dbCtx = context.Background()
		pgConn, err = pgxpool.New(dbCtx, config.dbURL)
		if err != nil {
			log.Printf("[ERROR] failed to connect to %s: %s", config.dbURL, err)
			return
		}
		defer pgConn.Close()
	}

	ch := make(chan string)
	var wg sync.WaitGroup
	var totalCount atomic.Int32
	var allTopics = make(map[string]batching.Batcher)

	for i := 0; i < config.numberOfGoRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			sess, err := session.NewSession(config.awsConfig)
			if err != nil {
				log.Printf("[ERROR]  creating session: %s", err)
				return
			}

			batcher, err := batching.New(config.topicARN, sns.New(sess), 2*time.Second)
			if err != nil {
				log.Printf("[ERROR]  creating batcher %s: %s", config.topicARN, err)
				return
			}
			allTopics[batcher.ID()] = batcher

			if config.doDB {
				batcher.DebugON()
			}

			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			for {
				select {
				case msg, ok := <-ch:
					if !ok {
						log.Printf("[INFO] %s in main: Channel is closed. Shutting down the topic ", batcher.ID())
						ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)

						if err := batcher.ShutDown(ctx, true); err != nil {
							log.Printf("[ERROR] %s calling shutdown func: %s", batcher.ID(), err)
						}

						cancel()
						return // channel closed
					}

					select {
					case <-ticker.C:
						log.Printf("[TRACE] %s: queuing %dth msg of %d bytes(%d runes): %s", batcher.ID(), totalCount.Load(), len(msg), utf8.RuneCountInString(msg), msg[:20])
					default:
						break
					}

					if config.doDB {
						_, err := pgConn.Exec(dbCtx, insertGeneratedSQL, len(msg), common.MD5(msg), string([]rune(msg)[:40]))
						if err != nil {
							log.Printf("[ERROR] writing %d bytes to the database: %s", len(msg), err)

						}
					}

					err := batcher.Append(msg)
					if err != nil {
						log.Printf("[ERROR] %s Failed to append %d bytes to the batch: %s", batcher.ID(), len(msg), err)
						if config.doDB {
							e := partialbase64encode.Encode(msg)
							pgConn.Exec(dbCtx, insertFailedSQL, len(msg), msg, len(e), e)
						}

						return
					}

					totalCount.Add(1)
				default:
				}
			}

		}()
	}

	log.Printf("[INFO] %d messages to be generated by %d threads", config.numberOfMessages+1, config.numberOfGoRoutines)
	ch <- common.HAIRY_MSG
	for i := 0; i < config.numberOfMessages; i++ {
		var msg string
		// for unicode strings length in bytes > length in characters/runes.
		for {
			msg = common.RandString(config.minLen, config.maxLen)
			msg = fmt.Sprintf("msg %d:%s", i, msg)
			if len(msg) <= batching.MAX_MSG_LENGTH {
				break
			}
		}

		ch <- msg
	}

	log.Printf("closing the message feeding chan.....")
	close(ch)
	wg.Wait() // this should take care of draining batcher via ShutDown call
	log.Printf("all go routines finished")
}
