package consumer

import (
	"github.com/mickamy/go-sqs-worker/worker"
)

type ProcessingOutput struct {
	Message worker.Message
	Error   error
	Fatal   bool
}

func (o ProcessingOutput) FatalError() error {
	if o.Fatal {
		return o.Error
	}
	return nil
}

func (o ProcessingOutput) NonFatalError() error {
	if o.Fatal {
		return nil
	}
	return o.Error
}

func (o ProcessingOutput) WithMessage(m worker.Message) ProcessingOutput {
	return ProcessingOutput{
		Message: m,
		Error:   o.Error,
		Fatal:   o.Fatal,
	}
}

func nonFatalJobProcessingOutput(err error) ProcessingOutput {
	return ProcessingOutput{Error: err}
}

func fatalJobProcessingOutput(err error) ProcessingOutput {
	return ProcessingOutput{Error: err, Fatal: true}
}
