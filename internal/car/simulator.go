package car

import (
	"context"
	"fmt"
	"log"
	"math/rand/v2"

	"github.com/kadriyebarlak/car-command-dispatcher/internal/domain"
)

type CarSimulator struct {
	offlineRate float64 // e.g. 0.3 = 30% chance the car is offline
}

func NewCarSimulator(offlineRate float64) *CarSimulator {
	return &CarSimulator{
		offlineRate: offlineRate,
	}
}

func (c *CarSimulator) Send(ctx context.Context, command domain.RemoteCommand) error {
	if rand.Float64() < c.offlineRate {
		return fmt.Errorf("car %s is offline", command.CarID)
	}
	log.Printf("car %s executed command %s", command.CarID, command.Type)
	return nil
}

var _ domain.Car = (*CarSimulator)(nil)
