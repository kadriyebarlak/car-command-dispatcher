package retry

import (
	"math"
	"testing"
	"time"
)

func TestBackoff_WithinBounds(t *testing.T) {
	base := 1 * time.Second
	cap := 30 * time.Second

	tests := []struct {
		name    string
		attempt int
	}{
		{"attempt 0", 0},
		{"attempt 1", 1},
		{"attempt 2", 2},
		{"attempt 3", 3},
		{"large attempt capped", 40},
		{"negative attempt treated as 0", -5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// the expected ceiling for this attempt, capped
			normalized := tt.attempt
			if normalized < 0 {
				normalized = 0
			}
			ceiling := time.Duration(float64(base) * math.Pow(2, float64(normalized)))
			if ceiling > cap || ceiling <= 0 {
				ceiling = cap
			}

			// run many times — randomness means we must check the bound repeatedly
			for i := 0; i < 1000; i++ {
				d := Backoff(tt.attempt, base, cap)

				if d < 0 {
					t.Fatalf("delay is negative: %v", d)
				}
				if d > ceiling {
					t.Fatalf("delay %v exceeds ceiling %v", d, ceiling)
				}
				if d > cap {
					t.Fatalf("delay %v exceeds cap %v", d, cap)
				}
			}
		})
	}
}

func TestBackoff_ProducesVariety(t *testing.T) {
	// full jitter should produce different values across calls — confirm it is
	// not returning a constant. Collect distinct values over many runs.
	base := 1 * time.Second
	cap := 30 * time.Second

	seen := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		seen[Backoff(3, base, cap)] = true
	}

	if len(seen) < 10 {
		t.Fatalf("expected varied delays from jitter, got only %d distinct values", len(seen))
	}
}
