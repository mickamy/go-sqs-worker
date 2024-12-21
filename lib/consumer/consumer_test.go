package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"

	"github.com/mickamy/go-sqs-worker/job"
	"github.com/mickamy/go-sqs-worker/sqs/mock_sqs"
	"github.com/mickamy/go-sqs-worker/worker"
)

type jobType string

const (
	testJobType jobType = "TestJob"
)

func getJob(s string) (job.Job, error) {
	if s == string(testJobType) {
		return testJob{}, nil
	}
	return nil, errors.New("job not found")
}

type testJob struct {
	ExecuteFunc func() error
}

func (j testJob) Execute(ctx context.Context, payloadStr string) error {
	if j.ExecuteFunc != nil {
		return j.ExecuteFunc()
	}
	return nil
}

func must[T any](val T, err error) T {
	if err != nil {
		panic(err)
	}
	return val
}

func caller() string {
	pcs := [13]uintptr{}
	length := runtime.Callers(1, pcs[:])
	frames := runtime.CallersFrames(pcs[:length])
	frame, _ := frames.Next()
	return fmt.Sprintf("%s:%d", frame.Function, frame.Line)
}

var (
	testJobError = errors.New("test job execution failed")
	cfg          = Config{
		UseDLQ:             true,
		WorkerQueueURL:     "http://localhost:4566/000000000000/worker-queue",
		DeadLetterQueueURL: "https://localhost:4566/000000000000/dead-letter-queue",
	}
)

func TestConsumer_process(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name           string
		arrangeMessage func(msg *worker.Message)
		arrange        func(client *mock_sqs.MockClient)
		assert         func(t *testing.T, got ProcessingOutput)
	}{
		{
			name: "successful processing",
			arrange: func(client *mock_sqs.MockClient) {
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.NotEmpty(t, got.Message)
				assert.NoError(t, got.Error)
				assert.Equal(t, false, got.Fatal)
			},
		},
		{
			name: "failed to unmarshal message and sent to DLQ successfully",
			arrangeMessage: func(msg *worker.Message) {
				msg.Type = ""
			},
			arrange: func(client *mock_sqs.MockClient) {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(nil)
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.Empty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to unmarshal message; sent to DLQ successfully: failed to validate message")
				assert.Equal(t, false, got.Fatal)
			},
		},
		{
			name: "failed to unmarshal message and send to DLQ",
			arrangeMessage: func(msg *worker.Message) {
				msg.Type = ""
			},
			arrange: func(client *mock_sqs.MockClient) {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(fmt.Errorf("enqueue failed"))
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.Empty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to unmarshal message and send to DLQ: failed to enqueue message on sending to DLQ")
				assert.Equal(t, true, got.Fatal)
			},
		},
		{
			name: "failed to get job and sent to DLQ successfully",
			arrangeMessage: func(msg *worker.Message) {
				msg.Type = "NonExistingJob"
			},
			arrange: func(client *mock_sqs.MockClient) {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(nil)
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to get job; sent to DLQ successfully.")
				assert.Equal(t, false, got.Fatal)
			},
		},
		{
			name: "failed to get job and send to DLQ",
			arrangeMessage: func(msg *worker.Message) {
				msg.Type = "NonExisting"
			},
			arrange: func(client *mock_sqs.MockClient) {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(fmt.Errorf("enqueue failed"))
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to get job and send to DLQ.")
				assert.Equal(t, true, got.Fatal)
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			msg := worker.Message{
				ID:         must(uuid.NewUUID()),
				Type:       string(testJobType),
				Caller:     caller(),
				CreatedAt:  time.Now(),
				RetryCount: 0,
			}
			if tc.arrangeMessage != nil {
				tc.arrangeMessage(&msg)
			}

			sqsMock := mock_sqs.NewMockClient(ctrl)
			tc.arrange(sqsMock)

			bytes, err := json.Marshal(msg)
			assert.NoError(t, err)

			// act
			sut, err := newConsumer(cfg, sqsMock, getJob, nil)
			got := sut.process(context.Background(), string(bytes))

			// assert
			tc.assert(t, got)
		})
	}
}

func TestConsumer_execute(t *testing.T) {
	t.Parallel()

	tcs := []struct {
		name           string
		arrangeMessage func(msg *worker.Message)
		arrange        func(client *mock_sqs.MockClient) testJob
		assert         func(t *testing.T, got ProcessingOutput)
	}{
		{
			name: "successful execution",
			arrange: func(client *mock_sqs.MockClient) testJob {
				return testJob{
					ExecuteFunc: func() error {
						return nil
					},
				}
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.NotEmpty(t, got.Message)
				assert.NoError(t, got.Error)
				assert.Equal(t, false, got.Fatal)
			},
		},
		{
			name: "failed to execute job and retried successfully",
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().EnqueueWithDelay(gomock.Any(), gomock.Eq(cfg.WorkerQueueURL), gomock.Any(), gomock.Any()).Return(nil)
				return testJob{
					ExecuteFunc: func() error {
						return testJobError
					},
				}
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to execute job; retried successfully.")
				assert.Equal(t, false, got.Fatal)
			},
		},
		{
			name: "failed to execute job and retry",
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().EnqueueWithDelay(gomock.Any(), gomock.Eq(cfg.WorkerQueueURL), gomock.Any(), gomock.Any()).Return(fmt.Errorf("enqueue failed"))
				return testJob{
					ExecuteFunc: func() error {
						return testJobError
					},
				}
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "failed to execute job and retry.")
				assert.Equal(t, true, got.Fatal)
			},
		},
		{
			name: "max retry attempts exceeded and sent to DLQ successfully",
			arrangeMessage: func(msg *worker.Message) {
				msg.RetryCount = 5
			},
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(nil)
				return testJob{
					ExecuteFunc: func() error {
						return testJobError
					},
				}
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "max retry attempts exceeded; sent to DLQ successfully.")
				assert.Equal(t, false, got.Fatal)
			},
		},
		{
			name: "max retry attempts reached and failed to send to DLQ",
			arrangeMessage: func(msg *worker.Message) {
				msg.RetryCount = 5
			},
			arrange: func(client *mock_sqs.MockClient) testJob {
				client.EXPECT().Enqueue(gomock.Any(), gomock.Eq(cfg.DeadLetterQueueURL), gomock.Any()).Return(fmt.Errorf("DLQ enqueue failed"))
				return testJob{
					ExecuteFunc: func() error {
						return testJobError
					},
				}
			},
			assert: func(t *testing.T, got ProcessingOutput) {
				assert.NotEmpty(t, got.Message)
				assert.ErrorContains(t, got.Error, "max retry attempts reached; failed to send to DLQ.")
				assert.Equal(t, true, got.Fatal)
			},
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// arrange
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			msg := worker.Message{
				ID:        must(uuid.NewUUID()),
				Type:      string(testJobType),
				Caller:    caller(),
				CreatedAt: time.Now(),
			}
			if tc.arrangeMessage != nil {
				tc.arrangeMessage(&msg)
			}

			sqsMock := mock_sqs.NewMockClient(ctrl)
			testJob := tc.arrange(sqsMock)

			// act
			sut, err := newConsumer(cfg, sqsMock, getJob, nil)
			assert.NoError(t, err)
			out := sut.execute(context.Background(), testJob, msg)

			// assert
			tc.assert(t, out)
		})
	}
}
