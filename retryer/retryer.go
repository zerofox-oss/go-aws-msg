package retryer

import (
	"time"

	"github.com/aws/aws-sdk-go/aws/request"
)

// DefaultRetryer implements an AWS `request.Retryer` that has a custom delay
// for credential errors (403 statuscode).
// This is needed in order to wait for credentials to be valid for SQS requests
// due to AWS "eventually consistent" credentials:
// https://docs.aws.amazon.com/IAM/latest/UserGuide/troubleshoot_general.html
type DefaultRetryer struct {
	request.Retryer
	Delay time.Duration
}

// RetryRules returns the delay for the next request to be made
func (r DefaultRetryer) RetryRules(req *request.Request) time.Duration {
	if req.HTTPResponse.StatusCode == 403 {
		return r.Delay
	}
	return r.Retryer.RetryRules(req)
}

// ShouldRetry determines if the passed request should be retried
func (r DefaultRetryer) ShouldRetry(req *request.Request) bool {
	if req.HTTPResponse.StatusCode == 403 {
		return true
	}
	return r.Retryer.ShouldRetry(req)
}
