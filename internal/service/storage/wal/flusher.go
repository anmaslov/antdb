package wal

import (
	"context"
)

type Flusher interface {
	Flush(context.Context, *buffer)
}
