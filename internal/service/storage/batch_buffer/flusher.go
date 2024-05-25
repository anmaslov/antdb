package batch_buffer

import "context"

type Flusher[T any] interface {
	Flush(context.Context, Buffer[T])
}
