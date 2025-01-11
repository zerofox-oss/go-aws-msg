package partialbase64encode

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"strings"
)

// https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html
// A message can include only XML, JSON, and unformatted text.
// The following Unicode characters are allowed:
// #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF
// https://symbl.cc/en/unicode-table
// #x20 (32) - ASCII space  --  #xD7FF (55295)  - not defined; Closest: ퟻ D7FB:  Hangul Jongseong Phieuph-Thieuth
// xE000 (57344) -"Private Use Area, not defined"  -- xFFFD (65533) � - Replacement Character
// #x10000 (65536) - 𐀀 Linear B Syllable B008 A   -- x10FFFF (1114111) - undefined. Closest:   10FF6 - Elymaic Ligature Zayin-yodh. Unicode can support a maximum of 1,114,112 code points:
//Any characters not included in this list are rejected.
// For more information, see the W3C specification for characters.
//
// Yes, Go strings are natively UTF-8 encoded.
// var t rune = 'Ї';var sb strings.Builder; n, err := sb.WriteRune(t): n==2 and len(sb.String()) == 2
// t = 65536 //#x10000 (𐀀): n == 4 and len(sb.String()) == 4

var LOW = []rune{9, 10, 13, 32, 57344, 65536}
var HI = []rune{9, 10, 13, 55295, 65533, 1114111}
var MARKER rune = 1114111

func supported(r rune) bool {
	for i := 0; i < len(LOW); i++ {
		if r >= LOW[i] && r <= HI[i] {
			return true
		}
	}
	return false
}

func encodeIllegalRunes(runes []rune, sb *strings.Builder) {
	s := base64.StdEncoding.EncodeToString([]byte(string(runes)))
	s = PrefixWithLength(s)
	sb.WriteRune(MARKER)
	sb.WriteString(s)
}

func Encode(in string) string {
	asRunes := []rune(in)
	var sb strings.Builder
	start := 0
	inUnsupported := false
	for i, r := range asRunes {
		//escape MARKER by doubling it
		if r == MARKER {
			// crazy case when marker terminates an illegal substring
			if inUnsupported {
				encodeIllegalRunes(asRunes[start:i], &sb)
			} else {
				sb.WriteString(string(asRunes[start:i]))
			}
			sb.WriteRune(MARKER)
			sb.WriteRune(MARKER)
			start = i + 1
			continue
		}

		// base64 Encode unsupported chars, prefix the encoded string with MARKER + 4 bytes of length
		if supported(r) && inUnsupported {
			encodeIllegalRunes(asRunes[start:i], &sb)
			start = i
			inUnsupported = false
			continue
		}

		// hitting an unsupported chart after good ones
		if !inUnsupported && !supported(r) {
			inUnsupported = true
			sb.WriteString(string(asRunes[start:i]))
			start = i
			continue
		}
	}

	// take care of the tail
	switch inUnsupported {
	case true:
		s := base64.StdEncoding.EncodeToString([]byte(string(asRunes[start:])))
		s = PrefixWithLength(s)
		sb.WriteRune(MARKER)
		sb.WriteString(s)
	default:
		sb.WriteString(string(asRunes[start:]))
	}
	return sb.String()
}

func Decode(in string) (string, error) {
	asRunes := []rune(in)
	var sb strings.Builder
	start := 0
	for i := 0; i < len(asRunes); i++ {
		r := asRunes[i]
		if r == MARKER {
			//write everything up to but excluding the marker
			if start < i {
				sb.WriteString(string(asRunes[start:i]))
				start = i
			}

			// check for escaped marker
			if i < len(asRunes)-1 && asRunes[i+1] == MARKER {
				sb.WriteString(string(asRunes[start : i+1]))
				start = i + 2
				i = i + 1 // loop increment will add another 1
				continue
			}

			// uuencoded string follows
			length, ok := new(big.Int).SetString(string(asRunes[i+1:i+5]), 62)
			if !ok {
				return "", fmt.Errorf("Failed to parse length %v", asRunes[i+1:i+5])
			}
			lInt := int(length.Uint64())
			decoded, err := base64.StdEncoding.DecodeString(string(asRunes[i+5 : i+5+lInt]))
			if err != nil {
				return "", fmt.Errorf("Failed to Decode length %d string %v: %v", lInt, asRunes[i+5:i+5+lInt], err)
			}
			sb.WriteString(string(decoded))
			start = i + 4 + lInt + 1
			i = i + 4 + lInt // loop increment will add another 1
			continue
		}
	}

	sb.WriteString(string(asRunes[start:]))
	return sb.String(), nil
}

// base 62 300000 is 1g2I
// 4 bytes of length prefix each msg in a Batch
func PrefixWithLength(payload string) string {
	length := big.NewInt(int64(len(payload))).Text(62)
	return strings.Repeat("0", 4-len(length)) + length + payload
}
