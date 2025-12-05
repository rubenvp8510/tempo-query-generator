package generator

import (
	"math/rand"
	"time"

	"github.com/rubenvp8510/tempo-query-generator/internal/config"
)

// SelectTimeBucket selects a time bucket based on weighted random selection
// Only buckets where data could exist (ageEnd <= elapsed time) are considered
func SelectTimeBucket(buckets []config.TimeBucket, testStartTime time.Time) *config.TimeBucket {
	elapsed := time.Since(testStartTime)

	// Filter to only buckets where data could exist
	var eligible []config.TimeBucket
	for _, bucket := range buckets {
		if bucket.AgeEnd <= elapsed {
			eligible = append(eligible, bucket)
		}
	}

	// If no buckets are eligible yet, return nil
	if len(eligible) == 0 {
		return nil
	}

	// Weighted selection from eligible buckets only
	totalWeight := 0
	for _, bucket := range eligible {
		totalWeight += bucket.Weight
	}

	r := rand.Intn(totalWeight)
	cumulative := 0
	for i := range eligible {
		cumulative += eligible[i].Weight
		if r < cumulative {
			return &eligible[i]
		}
	}
	return &eligible[0]
}
