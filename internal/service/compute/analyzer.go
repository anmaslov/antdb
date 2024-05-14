package compute

import (
	"go.uber.org/zap"
)

type Analyzer struct {
	logger *zap.Logger
}

func NewAnalyzer(logger *zap.Logger) *Analyzer {
	return &Analyzer{
		logger: logger,
	}
}

func (a *Analyzer) Analyze(tokens []string) (*Query, error) {
	logAnalyzer := a.logger.With(zap.Any("tokens", tokens))

	if len(tokens) == 0 {
		logAnalyzer.Debug("empty query")
		return nil, errInvalidCommand
	}

	command, err := mapCommand(tokens[0])
	if err != nil {
		logAnalyzer.Debug("invalid command", zap.Error(err))
		return nil, errInvalidCommand
	}

	if len(tokens) != queryMap[command] {
		logAnalyzer.Debug("invalid query attributes")
		return nil, errInvalidArguments
	}

	return NewQuery(command, tokens[1:]), nil
}
