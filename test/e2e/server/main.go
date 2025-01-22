package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yurizf/go-aws-msg-costs-control/sqs"
	"github.com/zerofox-oss/go-msg"
	"io"
	"log"
	"os"
	"strconv"
	"sync/atomic"

	// "runtime"
	"strings"
	// "sync"
	"time"
)

func MD5(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func substr(s string) string {
	if len(s) > 20 {
		return s[:20]
	}
	return s
}

func main() {
	var topic_url string
	// https://betterstack.com/community/guides/logging/logging-in-go/
	if r := os.Getenv("TOPIC_URL"); r != "" {
		log.Printf("TOPIC_URL is %s", r)
		topic_url = r
	}

	var db_url string
	do_db := false
	if r := os.Getenv("DO_DB"); r != "" && r != "0" && strings.ToLower(r) != "no" {
		do_db = true

		if r := os.Getenv("DB_URL"); r != "" {
			log.Printf("DB_URL is %s", r)
			db_url = r
		}
	}

	var expectedMsgsNum int
	if r := os.Getenv("TOTAL_MESSAGES"); r != "" {
		expectedMsgsNum, _ = strconv.Atoi(r)
	}
	expectedMsgsNum++ // add hairy msg

	sqsSrv, err := sqs.NewServer(topic_url, 10, int64(30))
	if err != nil {
		log.Printf(fmt.Sprintf("[ERROR] creating Server: %s", err))
		return
	}

	var dbCtx context.Context
	var pgConn *pgxpool.Pool
	if do_db {
		dbCtx = context.Background()
		pgConn, err = pgxpool.New(dbCtx, db_url)
		if err != nil {
			log.Printf(fmt.Sprintf("[ERROR] connecting to %: %s", db_url, err))
			return
		}
		defer pgConn.Close()
	}

	var processedMessages atomic.Int32
	insertSQL := "INSERT INTO server (l, md5, msg) VALUES ($1, $2, $3)"

	receiverFunc := msg.ReceiverFunc(
		func(ctx context.Context, m *msg.Message) error {

			data, _ := io.ReadAll(m.Body)
			str := string(data)
			log.Printf("[TRACE] receiver obtained msg of %d bytes: %s", len(str), substr(str))
			if do_db {
				_, err := pgConn.Exec(dbCtx, insertSQL, len(str), MD5(str), string([]rune(str)[:40]))
				if err != nil {
					log.Printf("[ERROR] writing %d bytes to the database: %s", len(str), err)
				}
			}
			processedMessages.Add(1)
			return nil
		})

	// watcher thread. In the local integration test we did the opposite:
	// ran sqsSrv.Serve within a go routine and watched from the main thread
	// here we do the opposite to demonstrate you can do it both ways.
	// as long as you have a data point to go by

	go func() {
		ticker := time.NewTicker(15 * time.Second)
		unchanged := 0
		initialCount := processedMessages.Load()

		for {
			current := processedMessages.Load()
			select {
			case <-ticker.C:
				log.Printf("%d * 15 seconds elapsed without new messages: %d messages received", unchanged, processedMessages.Load())
				if current > 0 && initialCount == current {
					unchanged++
					if current == int32(expectedMsgsNum) || unchanged > 4 { // 60 secs
						log.Printf("no new messages within 3min. Shutting down sqsSrv with the timeout %s", time.Duration(1*time.Second))
						ctxt, cancel := context.WithTimeout(context.Background(), time.Duration(1*time.Second))
						defer cancel()
						ticker.Stop()
						sqsSrv.Shutdown(ctxt)
						return
					}
				}
			case <-time.After(1 * time.Second):
				log.Printf("tick-tok.  %d messages processed", processedMessages.Load())
				if current > initialCount {
					initialCount = current
					unchanged = 0
				}
			}
		}
	}()

	log.Printf("Starting to serve...")
	// blocking call
	err = sqsSrv.Serve(receiverFunc)
	if !errors.Is(err, msg.ErrServerClosed) {
		log.Printf("[ERROR] Server closed with an error: %s", err)
	}

	log.Printf("[TRACE] Server closed. Running diagnostics SQLs now...")

	var cnt int

	if do_db {
		sqls := []string{"SELECT COUNT(1) FROM client",
			"SELECT COUNT(1) FROM server",
			`SELECT client.l, client.md5, client.msg
		 	 FROM client
		 	 EXCEPT    
		 	 SELECT server.l,server.md5, server.msg
         	 FROM server`,
			"SELECT count(1) FROM client INNER JOIN server ON server.md5=client.md5",
		}

		row := pgConn.QueryRow(dbCtx, sqls[0])
		switch err := row.Scan(&cnt); err {
		case nil:
			log.Printf("[TRACE] ****** Total number of client records from PG is %d *******\n", cnt)
		default:
			log.Printf("[ERROR] ****** error retrieveing Total number of client records from PG is %s *******\n", err)
		}

		clientCnt := cnt
		row = pgConn.QueryRow(dbCtx, sqls[1])
		switch err := row.Scan(&cnt); err {
		case nil:
			log.Printf("[TRACE] ****** Total number of server records from PG is %d *******\n", cnt)
		default:
			log.Printf("[ERROR] ****** error retrieveing Total number of server records from PG is %s *******\n", err)
		}
		serverCnt := cnt

		var l int
		var hash string
		var m string

		rows, err := pgConn.Query(dbCtx, sqls[2])
		if err != nil {
			log.Printf("[ERROR] failed to execute sql %s: %s", sqls[2], err)
			return
		}
		defer rows.Close()
		log.Printf("[TRACE] ***** SQL Results: client messages that were not found in the server table")
		cnt = 0
		for rows.Next() {
			err := rows.Scan(&l, &hash, &m)
			if err != nil {
				log.Printf("[ERROR] failed to scan in values: %s", err)
				return
			}
			cnt++
			log.Printf("[TRACE] %d %s %s", l, hash, m)
		}

		log.Printf("[TRACE] ***** SQL Results: count of client messages that were not found in the server table is %d", cnt)
		if cnt > 0 || clientCnt != serverCnt || clientCnt != expectedMsgsNum || serverCnt != expectedMsgsNum {
			log.Printf("[ERROR] record numbers mismatch expected=%d,  client tbl=%d, server tbl=%d", expectedMsgsNum, clientCnt, serverCnt)
		}

		row = pgConn.QueryRow(dbCtx, sqls[3])
		switch err := row.Scan(&cnt); err {
		case nil:
			if cnt != expectedMsgsNum {
				log.Printf("[ERROR] ****** number of matching records %d not equal to expected %d", cnt, expectedMsgsNum)
			} else {
				log.Printf("[INFO] **** number of matching records match the expected %d", cnt)
			}
		default:
			log.Printf("[ERROR] ****** error retrieveing number of matching records %s *******\n", err)
		}
	}

	received := processedMessages.Load()
	log.Printf("[TRACE] SQS read and processed Total unbatched messages: %d", received)
	if received != int32(expectedMsgsNum) {
		log.Printf("[ERROR] expected %d unbatched messages, got %d", expectedMsgsNum, received)
	}
}
