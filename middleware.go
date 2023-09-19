package bus

type BusMiddleware interface {
	Handle(message Message) error
}

type EmptyMiddleware struct {
}

func NewEmptyMiddleware() *EmptyMiddleware {
	return &EmptyMiddleware{}
}

func (em *EmptyMiddleware) Handle(command Message) error {
	return nil
}
