package bus

import "fmt"

type Publisher interface {
	Publish(message Message) error
}

type PublishableMessage interface {
	IsPublishable() bool
}

type PublisherMiddleware struct {
	isFirst        bool
	publisher      Publisher
	nextMiddleware BusMiddleware
}

func NewPublisherMiddleware(nextMiddleware BusMiddleware, publisher Publisher) *PublisherMiddleware {
	return &PublisherMiddleware{
		publisher:      publisher,
		nextMiddleware: nextMiddleware,
	}
}

func (a PublisherMiddleware) Handle(message Message) error {
	publishableMessage, ok := message.(PublishableMessage)
	if !ok {
		return a.nextMiddleware.Handle(message)
	}

	if !publishableMessage.IsPublishable() {
		return a.nextMiddleware.Handle(message)
	}

	err := a.publisher.Publish(message)

	if err != nil {
		return fmt.Errorf("There was an error when publishing %#v", message)
	}

	return err
}
