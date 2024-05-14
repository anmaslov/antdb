package compute

import (
	"errors"
	"strings"
)

var (
	ErrParse = errors.New("can't parse query")

	ErrInvalidSymbol = errors.New("invalid symbol")
)

const (
	initialState = iota
	latterFoundState
	spaceFoundState
)

type Parser struct {
	state int
	buff  strings.Builder
}

func NewParser() *Parser {
	return &Parser{}
}

func (p *Parser) Parse(query string) ([]string, error) {
	var tokens []string
	p.state = initialState
	p.buff.Reset()

	for i := 0; i < len(query); i++ {
		symbol := query[i]

		switch p.state {
		case initialState:
			if isSpaceSymbol(symbol) {
				return nil, ErrParse
			}
			if !isLetter(symbol) {
				return nil, ErrInvalidSymbol
			}
			p.buff.WriteByte(symbol)
			p.state = latterFoundState

		case latterFoundState:
			if isSpaceSymbol(query[i]) {
				tokens = append(tokens, p.buff.String())
				p.buff.Reset()
				p.state = spaceFoundState
				break
			}
			if !isLetter(query[i]) {
				return nil, ErrInvalidSymbol
			}
			p.buff.WriteByte(query[i])

		case spaceFoundState:
			if isSpaceSymbol(query[i]) {
				continue
			}
			if !isLetter(symbol) {
				return nil, ErrInvalidSymbol
			}
			p.buff.WriteByte(symbol)
			p.state = latterFoundState
		}
	}

	if p.state == latterFoundState {
		tokens = append(tokens, p.buff.String())
		p.buff.Reset()
	}

	return tokens, nil
}

func isSpaceSymbol(symbol byte) bool {
	return symbol == '\t' || symbol == '\n' || symbol == ' '
}

func isLetter(symbol byte) bool {
	return (symbol >= 'a' && symbol <= 'z') ||
		(symbol >= 'A' && symbol <= 'Z') ||
		(symbol >= '0' && symbol <= '9') ||
		(symbol == '*') ||
		(symbol == '/') ||
		(symbol == '_')
}
