package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-playground/validator/v10"
	"github.com/google/uuid"
)

var (
	validate = validator.New()
)

type Message struct {
	// ID is the unique identifier of the job
	ID uuid.UUID `json:"id" validate:"required,uuid"`

	// Type is the type of the job. It should be the value of job.Type.String
	Type string `json:"type" validate:"required"`

	// Payload is the data to be passed to the job
	Payload string `json:"payload" validate:"-"`

	// RetryCount is the number of times the job has been retried
	RetryCount int `json:"retry_count" validate:"-"`

	// Caller is the information about the caller of the job
	Caller string `json:"caller" validate:"required"`

	// CreatedAt is the time the job was created
	CreatedAt time.Time `json:"created_at" validate:"required"`
}

// Retry increments the RetryCount of the job
func (m *Message) Retry() {
	m.RetryCount++
}

// SetCaller sets the Caller of the job
func (m *Message) SetCaller(caller string) {
	m.Caller = caller
}

// NewMessage creates a new Message
func NewMessage(ctx context.Context, t string, payload any) (Message, error) {
	id := uuid.New()
	bytes, err := json.Marshal(payload)
	if err != nil {
		return Message{}, fmt.Errorf("marshalling payload failed: %w", err)
	}
	if err := validate.StructCtx(ctx, payload); err != nil {
		return Message{}, fmt.Errorf("validation failed: %v", err)
	}
	return Message{
		ID:        id,
		Type:      t,
		Payload:   string(bytes),
		CreatedAt: time.Now(),
	}, nil
}
