package common

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"strconv"
)

// msg with unicode chars SQS does not support
var unsupported1 string = string(rune(5)) + string(rune(6)) + string(rune(7))
var unsupported2 string = string(rune(2)) + string(rune(1)) + string(rune(3))
var marker string = string(rune(1114111))
var HAIRY_MSG string = "hairy_msg: " + unsupported2 +
	"𐀀𐀀" + marker + unsupported1 +
	"杂志等中区别" + marker + "A" + marker + marker + unsupported1 +
	marker + unsupported2 + marker + marker + "987"

const symbols = "ЮРА НАХШИНabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_-/+?!@#$%^&*()[]世界象形字形声嶝山登(ē)"

var allRunesString = unsupported1 + unsupported2 + marker + symbols

var allRunes []rune = []rune(allRunesString)

func MD5(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func RandString(minLen int, maxLen int) string {
	// inclusive
	n := rand.Intn(maxLen-minLen+1) + minLen
	b := make([]rune, n)
	for i := range b {
		b[i] = allRunes[rand.Int63()%int64(len(allRunes))]
	}
	// Encode Unicode code points as UTF-8
	// Invalid code points converted to Unicode replacement character (U+FFFD). Should not be any
	return string(b)
}

func DumpGoRoutines(id string) {
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	fmt.Printf("%s: number of go routines is: %d\n", id, runtime.NumGoroutine())
	fmt.Println(string(buf[:n]))
}

var (
	goroutinePrefix = []byte("goroutine ")
	errBadStack     = errors.New("invalid runtime.Stack output")
)

func GoID() (int, error) {
	buf := make([]byte, 32)
	n := runtime.Stack(buf, false)
	buf = buf[:n]
	// goroutine 1 [running]: ...

	buf, ok := bytes.CutPrefix(buf, goroutinePrefix)
	if !ok {
		return 0, errBadStack
	}

	i := bytes.IndexByte(buf, ' ')
	if i < 0 {
		return 0, errBadStack
	}

	return strconv.Atoi(string(buf[:i]))
}
