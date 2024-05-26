package wal

import "antdb/internal/service/compute"

type Unit struct {
	command   compute.Command
	arguments []string
}

type UnitData struct {
	Unit    *Unit
	ErrChan chan error
}

func NewUnit(command compute.Command, arguments []string) *Unit {
	return &Unit{
		command:   command,
		arguments: arguments,
	}
}
