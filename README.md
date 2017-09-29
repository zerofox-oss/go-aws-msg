# go-aws-msg

[![GoDoc](https://godoc.org/github.com/zerofox-oss/go-aws-msg?status.svg)](http://godoc.org/github.com/zerofox-oss/go-aws-msg)
[![Build Status](https://travis-ci.org/zerofox-oss/go-aws-msg.svg?branch=master)](https://travis-ci.org/zerofox-oss/go-aws-msg)

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
