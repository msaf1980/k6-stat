package dbs

import (
	"errors"
	"net"
	"net/http"
	"net/url"
)

type QueryError struct {
	code    int
	wrapped error
	query   string
}

func NewQueryError(wrapErr error, code int, query string) *QueryError {
	if code == 0 {
		code = http.StatusInternalServerError
		if uErr, ok := wrapErr.(*url.Error); ok {
			if _, ok := uErr.Err.(*net.OpError); ok {
				code = http.StatusServiceUnavailable
			}
		}
	}
	return &QueryError{code: code, wrapped: wrapErr, query: query}
}

func (e *QueryError) Error() string {
	return e.wrapped.Error()
}

func (e *QueryError) Wrapped() error {
	return e.wrapped
}

func (e *QueryError) Query() string {
	return e.query
}

func (e *QueryError) Code() int {
	return e.code
}

var (
	InvalidFrom  = NewQueryError(errors.New("invalid from"), http.StatusBadRequest, "")
	InvalidUntil = NewQueryError(errors.New("invalid until"), http.StatusBadRequest, "")
)
