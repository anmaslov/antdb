package compute

import (
	"context"
	"go.uber.org/zap"
)

type Compute struct {
	parser   parser
	analyzer analyzer
	logger   *zap.Logger
}

type parser interface {
	Parse(string) ([]string, error)
}

type analyzer interface {
	Analyze([]string) (*Query, error)
}

func NewCompute(parser parser, analyzer analyzer, logger *zap.Logger) *Compute {
	return &Compute{
		parser:   parser,
		analyzer: analyzer,
		logger:   logger,
	}
}

func (d *Compute) HandleQuery(_ context.Context, queryStr string) (*Query, error) {
	tokens, err := d.parser.Parse(queryStr)
	if err != nil {
		return nil, err
	}

	query, err := d.analyzer.Analyze(tokens)
	if err != nil {
		return nil, err
	}

	return query, nil
}
