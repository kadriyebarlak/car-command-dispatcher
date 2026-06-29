package domain

import "time"

type CommandType string

const (
	CommandStartClimate  CommandType = "START_CLIMATE"
	CommandStopClimate   CommandType = "STOP_CLIMATE"
	CommandStartCharging CommandType = "START_CHARGING"
)

type CommandStatus string

const (
	CommandStatusPending      CommandStatus = "PENDING"
	CommandStatusPublished    CommandStatus = "PUBLISHED"
	CommandStatusSent         CommandStatus = "SENT"
	CommandStatusAcknowledged CommandStatus = "ACKNOWLEDGED"
	CommandStatusFailed       CommandStatus = "FAILED"
	CommandStatusDead         CommandStatus = "DEAD"
)

type RemoteCommand struct {
	ID            string
	CarID         string
	Type          CommandType
	Payload       string
	Status        CommandStatus
	RetryCount    int
	LastAttemptAt *time.Time
}
