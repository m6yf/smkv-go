package smkv

import (
	"errors"
	"fmt"
)

var (
	ErrUnauthorized    = errors.New("smkv: authentication or permission denied")
	ErrCapacity        = errors.New("smkv: capacity limit; mutation rejected")
	ErrWrongOwner      = errors.New("smkv: wrong partition owner")
	ErrNotFound        = errors.New("smkv: key not found")
	ErrBusy            = errors.New("smkv: server busy")
	ErrInvalidArgument = errors.New("smkv: invalid argument")
	ErrInvalidRequest  = errors.New("smkv: server rejected request")
	ErrStorage         = errors.New("smkv: storage error")
	ErrUnavailable     = errors.New("smkv: server unavailable")
	ErrTimedOut        = errors.New("smkv: server execution timed out")
	ErrClosed          = errors.New("smkv: client closed")
	ErrProtocol        = errors.New("smkv: invalid protocol response")
	ErrOutcomeUnknown  = errors.New("smkv: mutation outcome unknown")
	// ErrConflict: a check-and-set found another version, or a record where
	// none was expected; nothing was written.
	ErrConflict = errors.New("smkv: version conflict")
)

// Status is a response status from the SMKV v1 wire protocol.
type Status uint8

const (
	StatusOK Status = iota
	StatusNotFound
	StatusBusy
	StatusInvalidRequest
	StatusStorageError
	StatusUnavailable
	StatusTimedOut
	StatusWrongOwner
	StatusCapacity
	StatusUnauthorized
	StatusConflict
)

// ServerError preserves the server's status and diagnostic message.
// Use errors.Is for a sentinel, or errors.As to inspect these fields.
type ServerError struct {
	Status  Status
	Message string
}

func (e *ServerError) Error() string {
	return fmt.Sprintf("smkv: server status %d: %s", e.Status, e.Message)
}

func (e *ServerError) Unwrap() error {
	switch e.Status {
	case StatusUnauthorized:
		return ErrUnauthorized
	case StatusCapacity:
		return ErrCapacity
	case StatusWrongOwner:
		return ErrWrongOwner
	case StatusNotFound:
		return ErrNotFound
	case StatusBusy:
		return ErrBusy
	case StatusInvalidRequest:
		return ErrInvalidRequest
	case StatusStorageError:
		return ErrStorage
	case StatusUnavailable:
		return ErrUnavailable
	case StatusTimedOut:
		return ErrTimedOut
	case StatusConflict:
		return ErrConflict
	default:
		return nil
	}
}

// OpError describes a failed operation. An unknown outcome applies only to
// mutations and does not assert that the mutation actually completed.
type OpError struct {
	Op             string
	Err            error
	OutcomeUnknown bool
}

func (e *OpError) Error() string {
	if e.OutcomeUnknown {
		return fmt.Sprintf("smkv %s: %v (mutation may have completed)", e.Op, e.Err)
	}
	return fmt.Sprintf("smkv %s: %v", e.Op, e.Err)
}

func (e *OpError) Unwrap() error { return e.Err }

func (e *OpError) Is(target error) bool {
	return target == ErrOutcomeUnknown && e.OutcomeUnknown
}
