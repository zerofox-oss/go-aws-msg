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

### Packing Multiple Messages into a Batch

When multiple messages are batched together into a single one, each of them is simply prefixed by 
the 4 bytes that symbolically expresses the message length in bytes as a base 62 integer.
Say we have these 3 messages to batch:

```shell
[]string{"ABC", strings.Repeat("杂志等中区别", 1000), strings.Repeat("志", 200000)}
```


The batch will look like below, without the blanks put there for readability

```shell
0003ABC 04Gk杂志等...18000 bytes(6000 runes)  2w5q志志志志...600000 bytes(20K runes) 
```

The batched messages have the following attribute: value set

```shell
"Content-Transfer-Encoding": "partially-base64-batch"
```

### Base-64 encoding of Messages and Optimization made in this area

[SQS has limited support of Unicode](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html)
Some Unicode ranges are not supported. Because of that the original library primary option
is to Base-64 encode whole messages. This is very wasteful since it 
inflates even ASCII data. 
For example, 'ABCDEF12345' becomes 'QUJDREVGMTIzNDUK': extra 6 chars
18 bytes of '杂志等中区别' (SQS supported characters) becomes 28 bytes of '5p2C5b+X562J5Lit5Yy65YirCgo='.
This is problematic because an SNS/SQS has a size limit of 250K

Batching uses encoding that base-64 encodes only what's necessary: 
the msg subsequences that are not supported by SQS. And this is the only encoding
batching supports. It's implemented in the sqsencode package. 
So partially base64 encoded message will look like this:

```bash
<sqs supported sequence>U+10FFFF<4 bytes of base64 encoded subsequence length><base64 encoded subsequence>
```

if the original message contains U+10FFFF, it is duplicated.

There is an option to use this encoding without batching: sns.NewPartialBASE64Topic call.
But if the topic is batched - sns.NewBatchedTopic - the above is the only encoding
used internally. 

This encoding sets the following message attribute: value

```shell
"Content-Transfer-Encoding": "partially-base64"
```

### Tests results

The tests I ran show significant potential cost savings. 
Here are the results from sending and receiving 5,000 randomly generated strings with lengths between 100 to 120,000 unicode chars,
including non SQS supported ones. With 5 parallel batchers (threads), here are the results

```bash
topic-5 **** cost savings (after deadline): (prepared/sent) messages: 1033/1033, batches: 575/575
topic-1 **** cost savings (after deadline): (prepared/sent) messages: 973/973, batches: 534/534
topic-4 **** cost savings (after deadline): (prepared/sent) messages: 1003/1003, batches: 551/551
topic-2 **** cost savings (after deadline): (prepared/sent) messages: 1013/1013, batches: 557/557
topic-3 **** cost savings (after deadline): (prepared/sent) messages: 979/979, batches: 520/520
```

As you can see, it should result in ~50% costs reduction. 
It's far from 90% savings claimed by the quoted article but still very significant.
I suppose if you send/receive short messages and your message to batch ratio is like 10 to 1, you can realistically get those 90%






