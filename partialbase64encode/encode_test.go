package partialbase64encode

import (
	"fmt"
	"testing"
)

var ILLEGAL = []rune{0, 1, 2, 3}

func Test_encode(t *testing.T) {
	s := string(ILLEGAL[:2])
	s = s + "𐀀𐀀"
	s = s + string(MARKER)
	s = s + string(ILLEGAL[:3])
	s = s + "杂志等中区别" + string(MARKER) + "A" + string(MARKER) + string(ILLEGAL[:3]) + string(MARKER) + "012"

	fmt.Println(s, len(s), []rune(s))
	encodedS := Encode(s)
	fmt.Println(encodedS, []rune(encodedS))
	d, e := Decode(encodedS)
	fmt.Println(d, len(d), e)
	if s != d {
		t.Errorf("decoded string %s not equal the excpected %s", d, s)
	}
}
