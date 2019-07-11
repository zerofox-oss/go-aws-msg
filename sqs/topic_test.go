package sqs

import (
	"testing"
	"time"
)

func TestSetDelay(t *testing.T) {
	durations := map[time.Duration]int64{
		60 * time.Second: 60,

		// round down to nearest second
		3602 * time.Millisecond: 3,

		// durations must be between 0 and 900
		1200 * time.Second: 900,
		-100 * time.Second: 0,
	}

	for d, seconds := range durations {
		t.Run(d.String(), func(t *testing.T) {
			w := &MessageWriter{}
			w.SetDelay(d)

			if w.delaySeconds != seconds {
				t.Errorf("expected delay to be set to %d, got %d", seconds, w.delaySeconds)
			}
		})
	}
}
