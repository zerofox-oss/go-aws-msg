package batching

import (
	"fmt"
	"github.com/yurizf/go-aws-msg-costs-control/partialbase64encode"
	"math/big"
	"strings"
)

const FRAGMENT_HEADER_LENGTH = 4

func fragmentLen(fragment string) int {
	return len(fragment) + FRAGMENT_HEADER_LENGTH
}

// Batch - builds a single string out of a slice of strings
// that concatenates slice elements prefixed by 4 bytes of length
func Batch(payloads []string) string {
	var sb strings.Builder

	for _, msg := range payloads {
		sb.WriteString(partialbase64encode.PrefixWithLength(msg))
	}

	return sb.String()
}

// DeBatch - decodes a string that was produced by a call to Batch
func DeBatch(batch string) ([]string, error) {

	i := 0
	ret := make([]string, 0, 100)

	for i < len(batch) {
		length, ok := new(big.Int).SetString(batch[i:i+4], 62)
		if !ok {
			return ret, fmt.Errorf("Failed to parse length %s", batch[i:i+4])
		}
		i = i + 4
		ret = append(ret, string(batch[i:i+int(length.Uint64())]))
		i = i + int(length.Uint64())
	}

	return ret, nil
}
