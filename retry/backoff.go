package retry

import (
	"math"
	"time"
)

// Backoff interface defines contract for backoff strategies
type Backoff interface {
	Next() time.Duration
}

type exponentialBackoff struct {
	exponentFactor float64
	initialTimeout float64
	maxTimeout     float64
	retryCounter   int64
}

// NewExponentialBackoff returns an instance of ExponentialBackoff
func NewExponentialBackoff(initialTimeout, maxTimeout time.Duration, exponentFactor float64) Backoff {
	return &exponentialBackoff{
		exponentFactor: exponentFactor,
		initialTimeout: float64(initialTimeout / time.Second),
		maxTimeout:     float64(maxTimeout / time.Second),
	}
}

func (eb *exponentialBackoff) Next() time.Duration {
	eb.retryCounter++
	return time.Duration(math.Min(eb.initialTimeout+math.Pow(eb.exponentFactor, float64(eb.retryCounter)), eb.maxTimeout)) * time.Second
}
