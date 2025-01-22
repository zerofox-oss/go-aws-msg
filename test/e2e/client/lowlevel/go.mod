module github.com/yurizf/go-aws-msg-costs-control/test/e2e/client/lowlevel

go 1.21

// require github.com/yurizf/go-aws-msg-with-batching

require github.com/yurizf/go-aws-msg-costs-control v0.0.0

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20240606120523-5a60cdf6a761 // indirect
	github.com/jackc/puddle/v2 v2.2.2 // indirect
	github.com/yurizf/slice-on-disk v0.2.0 // indirect
	golang.org/x/crypto v0.31.0 // indirect
	golang.org/x/sync v0.10.0 // indirect
	golang.org/x/text v0.21.0 // indirect
)

replace github.com/yurizf/go-aws-msg-costs-control v0.0.0 => ./../../../..

require (
	github.com/aws/aws-sdk-go v1.55.5 // indirect
	github.com/jackc/pgx/v5 v5.7.2
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/zerofox-oss/go-msg v0.1.4 // indirect
)
