package kafka

import "errors"

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }
func (e *permanentError) Unwrap() error { return e.err }

// Permanent marks err as non-retryable: the consumer sends the record to the
// DLQ immediately.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) is Permanent.
func IsPermanent(err error) bool {
	var p *permanentError
	return errors.As(err, &p)
}
