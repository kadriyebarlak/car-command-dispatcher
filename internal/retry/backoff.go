package retry

import (
	"math"
	"math/rand/v2"
	"time"
)

// Backoff returns a randomized delay for the given attempt number using
// exponential backoff with full jitter.
//
// The exponential ceiling is base * 2^attempt, capped at cap.
// The returned delay is a random duration in [0, ceiling] — this is "full jitter".
func Backoff(attempt int, base, cap time.Duration) time.Duration {
	// Guard against negative attempts.
	if attempt < 0 {
		attempt = 0
	}

	// exponential = base * 2^attempt
	// We compute 2^attempt as a float to avoid integer overflow on large attempts.
	multiplier := math.Pow(2, float64(attempt))
	exponentialFloat := float64(base) * multiplier

	if exponentialFloat > float64(cap) {
		exponentialFloat = float64(cap)
	}
	exponential := time.Duration(exponentialFloat)

	// Cap the exponential ceiling. Also catches overflow: if the multiplication
	// overflowed into a negative or huge value, the cap brings it back in range.
	if exponential <= 0 {
		exponential = cap
	}

	// Full jitter: a random duration in [0, exponential].
	// rand.Int64N(n) returns [0, n); +1 lets the result include the full ceiling.
	return time.Duration(rand.Int64N(int64(exponential) + 1))
}
