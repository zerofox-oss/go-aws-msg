package batching

import (
	"fmt"
	"math/big"
	"strings"
)

// base 62 300000 is 1g2I
// 4 bytes of length prefix each msg in a Batch

func pad(base62int string) string {
	return strings.Repeat("0", 4-len(base62int)) + base62int
}

func batchLen(payloads []string) int {
	totalLen := 0
	for _, msg := range payloads {
		totalLen = totalLen + 4 + len(msg)
	}
	return totalLen
}

// Batch - builds a single string out of a slice of strings
// that concatenates slice elements prefixed by 4 bytes of length
func Batch(payloads []string) string {
	var sb strings.Builder

	for _, msg := range payloads {
		sb.WriteString(pad(big.NewInt(int64(len(msg))).Text(62)))
		sb.WriteString(msg)
	}

	return sb.String()
}

// Debatch - decodes a string that was produced by a call to Batch
func Debatch(batch string) ([]string, error) {

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
