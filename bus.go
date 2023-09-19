package bus

import (
	"context"
	"fmt"
)

type Bus struct {
	middlewareChain BusMiddleware
	consumer        Consumer
}

type Message interface {
	Name() string
}

func NewBus(middlewareChain BusMiddleware) *Bus {
	return &Bus{middlewareChain: middlewareChain}
}

func (bus Bus) Handle(message Message) error {
	return bus.middlewareChain.Handle(message)
}

func (bus Bus) Consume(message Message) error {
	messageChannel := bus.consumer.Channel()
	c, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		defer cancel()
		err := bus.consumer.Consume(message, c)
		if err != nil {
			_ = fmt.Errorf("Error when starting consumer for message: %s", message.Name())
		}
	}()

loop:
	for {
		select {
		case m := <-messageChannel:
			go func() {
				err := bus.Handle(m)
				if err != nil {
					_ = fmt.Errorf("Error handling message: %s, with data %#v", m.Name(), m)
				}
			}()
		case <-c.Done():
			cancel()
			break loop
		}
	}

	return nil
}
