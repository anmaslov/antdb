package replication

import "context"

type Replication interface {
	Start(context.Context) error
	IsMaster() bool
}
