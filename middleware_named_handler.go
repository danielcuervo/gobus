package bus

import "fmt"

type Handler interface {
	Message() Message
	Handle(command Message) error
}

type NamedHandlerMiddleware struct {
	nextMiddleware BusMiddleware
	handlersMap    map[string]Handler
}

func NewNamedHandlerMiddleware(nextMiddleware BusMiddleware, handlers ...Handler) *NamedHandlerMiddleware {
	handlersMap := map[string]Handler{}

	for _, h := range handlers {
		handlersMap[h.Message().Name()] = h
	}
	return &NamedHandlerMiddleware{
		nextMiddleware: nextMiddleware,
		handlersMap:    handlersMap,
	}
}

func (n NamedHandlerMiddleware) Handle(message Message) error {
	messageName := message.Name()
	messageHandler, ok := n.handlersMap[messageName]
	if !ok {
		return fmt.Errorf("there not any CommandHandler associate to Command %s", messageName)
	}
	err := messageHandler.Handle(message)
	if err != nil {
		return fmt.Errorf(err.Error())
	}

	return n.nextMiddleware.Handle(message)
}
