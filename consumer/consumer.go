package consumer

import (
	"context"
	"encoding/json"
	"fmt"
	"math"

	sqsLib "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/go-playground/validator/v10"

	"github.com/mickamy/go-sqs-worker/job"
	"github.com/mickamy/go-sqs-worker/sqs"
	"github.com/mickamy/go-sqs-worker/worker"
)

var (
	validate = validator.New()
)

// Config is a configuration of the Consumer
type Config struct {
	// UseDLQ is a flag to enable DLQ
	UseDLQ bool

	// WorkerQueueURL is the URL of the worker queue
	WorkerQueueURL string

	// DeadLetterQueueURL is the URL of the dead letter queue
	// This is required if UseDLQ is true
	DeadLetterQueueURL string

	// MaxRetry is the maximum number of retries (default 5)
	MaxRetry int

	// BaseDelay is the initial delay time (default 30)
	BaseDelay float64

	// MaxDelay is the maximum delay time (default 3600)
	MaxDelay float64

	// WaitTimeSeconds is the wait time for long polling (default 20)
	WaitTimeSeconds int
}

func newConfig(c Config) Config {
	if c.MaxRetry == 0 {
		c.MaxRetry = 5
	}
	if c.BaseDelay == 0 {
		c.BaseDelay = 30
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = 3600
	}
	return c
}

// OnProcessFunc is a function to handle the output of the processing
type OnProcessFunc func(output ProcessingOutput)

type Consumer struct {
	config      Config
	sqsClient   sqs.Client
	getJobFunc  job.GetFunc
	onErrorFunc OnProcessFunc
}

// New creates a new Consumer
func New(config Config, client *sqsLib.Client, getJobFunc job.GetFunc, onProcessFunc OnProcessFunc) (*Consumer, error) {
	return newConsumer(config, sqs.New(client), getJobFunc, onProcessFunc)
}

func newConsumer(config Config, client sqs.Client, getJobFunc job.GetFunc, onProcessFunc OnProcessFunc) (*Consumer, error) {
	if getJobFunc == nil {
		return nil, fmt.Errorf("getJobFunc is required")
	}
	return &Consumer{
		config:      newConfig(config),
		sqsClient:   client,
		getJobFunc:  getJobFunc,
		onErrorFunc: onProcessFunc,
	}, nil
}

// Consume consumes messages from the worker queue
func (c *Consumer) Consume(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			m, cleanUp, err := c.sqsClient.Dequeue(ctx, c.config.DeadLetterQueueURL, c.config.WaitTimeSeconds)
			if err != nil {
				// continue processing if dequeue failed
				continue
			}
			if m == nil {
				// continue processing if message is empty
				continue
			}

			// clean up before processing to avoid duplicate processing
			if err := cleanUp(ctx); err != nil {
				continue
			}

			output := c.process(ctx, *m)
			if output.Error != nil && c.onErrorFunc != nil {
				c.onErrorFunc(output)
			}
		}
	}
}

// process processes a message
func (c *Consumer) process(ctx context.Context, s string) (out ProcessingOutput) {
	defer func() {
		if r := recover(); r != nil {
			out = fatalJobProcessingOutput(fmt.Errorf("panic occurred while processing message: %v", r))
		}
	}()

	msg, err := parse(ctx, s)
	if err != nil {
		if dlqErr := c.sendToDLQ(ctx, msg); dlqErr != nil {
			return fatalJobProcessingOutput(
				fmt.Errorf("failed to unmarshal message and send to DLQ: %w", dlqErr),
			).WithMessage(msg)
		}
		return nonFatalJobProcessingOutput(
			fmt.Errorf("failed to unmarshal message; sent to DLQ successfully: %s", err),
		).WithMessage(msg)
	}

	j, err := c.getJobFunc(msg.Type)
	if err != nil {
		if dlqErr := c.sendToDLQ(ctx, msg); dlqErr != nil {
			return fatalJobProcessingOutput(
				fmt.Errorf("failed to get job and send to DLQ. id=[%s], Type=[%s], Caller=[%s]: %w", msg.ID, msg.Type, msg.Caller, dlqErr),
			).WithMessage(msg)
		}
		return nonFatalJobProcessingOutput(
			fmt.Errorf("failed to get job; sent to DLQ successfully. id=[%s] type=[%s] caller=[%s]: %w", msg.ID, msg.Type, msg.Caller, err),
		).WithMessage(msg)
	}

	return c.execute(ctx, j, msg)
}

// execute executes a job and returns the JobProcessingOutput
func (c *Consumer) execute(ctx context.Context, j job.Job, msg worker.Message) ProcessingOutput {
	if err := j.Execute(ctx, msg.Payload); err != nil {
		if msg.RetryCount < c.config.MaxRetry {
			if retryErr := c.retry(ctx, msg); retryErr != nil {
				return fatalJobProcessingOutput(
					fmt.Errorf("failed to execute job and retry. id=[%s] type=[%s] payload=[%s] caller=[%s]: %w", msg.ID, msg.Type, msg.Payload, msg.Caller, retryErr),
				).WithMessage(msg)
			}
			return nonFatalJobProcessingOutput(
				fmt.Errorf("failed to execute job; retried successfully. id=[%s] type=[%s] caller=[%s]: %w", msg.ID, msg.Type, msg.Caller, err),
			).WithMessage(msg)
		}

		if dlqErr := c.sendToDLQ(ctx, msg); dlqErr != nil {
			return fatalJobProcessingOutput(
				fmt.Errorf("max retry attempts reached; failed to send to DLQ. id=[%s] type=[%s] payload=[%s] caller=[%s]: %w", msg.ID, msg.Type, msg.Payload, msg.Caller, dlqErr),
			).WithMessage(msg)
		}
		return nonFatalJobProcessingOutput(
			fmt.Errorf("max retry attempts exceeded; sent to DLQ successfully. id=[%s] type=[%s] caller=[%s]: %w", msg.ID, msg.Type, msg.Caller, err),
		).WithMessage(msg)
	}
	return ProcessingOutput{
		Message: msg,
	}
}

// retry retries a job with exponential backoff
func (c *Consumer) retry(ctx context.Context, msg worker.Message) error {
	msg.Retry()
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal message on retry: %w", err)
	}
	if enqueueErr := c.sqsClient.EnqueueWithDelay(ctx, c.config.WorkerQueueURL, string(bytes), c.calculateBackoff(msg.RetryCount)); enqueueErr != nil {
		return fmt.Errorf("faild to enqueue on retry: %w", enqueueErr)
	}
	return nil
}

// sendToDLQ sends a message to the dead letter queue
func (c *Consumer) sendToDLQ(ctx context.Context, msg worker.Message) error {
	if !c.config.UseDLQ {
		return nil
	}
	bytes, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshalling message on sending to DLQ: %w", err)
	}
	if err := c.sqsClient.Enqueue(ctx, c.config.DeadLetterQueueURL, string(bytes)); err != nil {
		return fmt.Errorf("failed to enqueue message on sending to DLQ: %w", err)
	}
	return nil
}

// calculateBackOff calculates exponential backoff
func (c *Consumer) calculateBackoff(retries int) int {
	delay := c.config.BaseDelay * math.Pow(2, float64(retries-1))
	return int(math.Min(delay, c.config.MaxDelay))
}

// parse parses a message to a worker.Message
func parse(ctx context.Context, s string) (worker.Message, error) {
	var msg worker.Message
	if err := json.Unmarshal([]byte(s), &msg); err != nil {
		return worker.Message{}, fmt.Errorf("failed to unmarshalling message: %w", err)
	}
	if err := validate.StructCtx(ctx, msg); err != nil {
		return worker.Message{}, fmt.Errorf("failed to validate message: %s", err)
	}
	return msg, nil
}
