# go-aws-msg

[![Go Reference](https://pkg.go.dev/badge/github.com/zerofox-oss/go-aws-msg.svg)](https://pkg.go.dev/github.com/zerofox-oss/go-aws-msg)
![lint](https://github.com/zerofox-oss/go-aws-msg/actions/workflows/golangci-lint.yml/badge.svg)
![tests](https://github.com/zerofox-oss/go-aws-msg/actions/workflows/tests.yml/badge.svg)

**This is a fork of https://github.com/zerofox-oss/go-aws-msg with added message [batching](#Batching) for cost control
This post claims that they managed reduce SQS costs by 90% by batching the messages
https://www.moengage.com/blog/reduce-sqs-cost/

**AWS Pub/Sub Primitives for Go**

This library contains
basic primitives for
building pub/sub systems
using AWS's
[SNS (Simple Notification Service)][SNS]
and [SQS (Simple Queue Service)][SQS].
It is inspired by [go-msg][].

SNS is specifically designed
to be a fully managed
[Pub/Sub messaging service][SNS-PubSub]
which supports a number of
different subscription types,
such as HTTP endpoints
or SQS queues.

At ZeroFOX, we use SNS for
publishing all of our data.
Teams are able to tap into
existing data streams
for new features or
data analysis.
SQS is our primary subscription protocol
because most of our data processing
backend is queued (for obvious benefits).
Though Lambda and HTTP
are not uncommon.
For us, SNS and SQS form the backbone
of our architecture,
what we call our data pipelines.

## How it works

Most of the basic theory behind
the primitives can be found
in [go-msg][].
This library contains the Topics
and Servers which interact
with SNS and SQS.

[go-msg]: https://github.com/zerofox-oss/go-msg
[SNS]: https://aws.amazon.com/documentation/sns/
[SNS-PubSub]: https://aws.amazon.com/sns/#SNSpubsub
[SQS]: https://aws.amazon.com/documentation/sqs/

## Batching

A package 'batching' has been added. On the client side it would spawn a go routine that
runs a 'batching engine'. The message won't be immediately send to SNS/SQS but will be put
on the queue instead via batching.Append call. The actual send occurs either when Append
sees that the queued messages with this extra payload exceed 250K in total or
when the batching engine detects a topic timeout that is specified in batching.NewTopic call.
The higher level packages sqs and sns were added functions BatchON and BatchOFF to turn batching on and off.
In theory, turning batching on and off should be possible without restart. 
The server side that reads SQS messages has a call sqs.BatchServer() that switches the 
package to reading/parsing/processing batches of messages instead of single ones.
Obviously, client and server need to be in sync when it comes to batching.
