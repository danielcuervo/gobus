package bus

import "context"

type Consumer interface {
	Consume(message Message, c context.Context) error
	Channel() chan Message
}

type EmptyConsumer struct {
}

func (e *EmptyConsumer) Consume(message Message) error {
	return nil
}
