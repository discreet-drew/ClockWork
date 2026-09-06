package backoff

import (
	"math"
	"math/rand"
	"time"
)

func Exponential(base, max time.Duration) func(attempt int) time.Duration {
	return func(attempt int) time.Duration {
		if attempt < 1 {
			attempt = 1
		}
		d := float64(base) * math.Pow(2, float64(attempt-1))
		if d > float64(max) {
			d = float64(max)
		}
		jitterFactor := 0.8 + rand.Float64()*0.4 
		return time.Duration(d * jitterFactor)
	}
}
