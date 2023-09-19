package bus

import "fmt"

type MessageSubscriber interface {
	SubscribedMessage() Message
	Consume(message Message) error
}

type MessageSubscriberMiddleware struct {
	nextMiddleware BusMiddleware
	dispatcher     Dispatcher
}

func NewMessageSubscriberMiddleware(dispatcher Dispatcher) MessageSubscriberMiddleware {
	return MessageSubscriberMiddleware{
		dispatcher: dispatcher,
	}
}

func (m MessageSubscriberMiddleware) Handle(message Message) error {
	err := m.dispatcher.Dispatch(message)
	if err != nil {
		return fmt.Errorf("there not any CommandHandler associate to Command %s", message)
	}

	return m.nextMiddleware.Handle(message)
}

type Dispatcher interface {
	Dispatch(message Message) error
	Subscribe(messageName string, subscriber MessageSubscriber)
}

type LocalDispatcher struct {
	subscribers map[string][]MessageSubscriber
}

func NewLocalDispatcher(subscribers ...MessageSubscriber) *LocalDispatcher {
	localDispatcher := &LocalDispatcher{}
	for _, s := range subscribers {
		localDispatcher.Subscribe(s.SubscribedMessage().Name(), s)
	}

	return localDispatcher
}

func (localDispatcher *LocalDispatcher) Dispatch(message Message) error {
	topic := message.Name()

	subscribers, ok := localDispatcher.subscribers[topic]
	if !ok {
		return fmt.Errorf("There are not subscribers for Event %s", topic)
	}

	var lastErr error

	for _, subscriber := range subscribers {
		s := subscriber
		go func() {
			err := s.Consume(message)
			if err != nil {
				_ = fmt.Errorf(
					"There was an error when consuming %s with subscriber %#v",
					message.Name(),
					s,
				)
			}
		}()
	}

	return lastErr
}

func (localDispatcher *LocalDispatcher) Subscribe(messageName string, subscriber MessageSubscriber) {
	localDispatcher.subscribers[messageName] = append(localDispatcher.subscribers[messageName], subscriber)
}
