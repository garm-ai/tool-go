package toolbind_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/garm-ai/tool-go/toolbind"
)

// The message is the error, so a handler that returns one and a handler that
// wraps one read the same in a log.
func TestACodedErrorReadsAsItsMessage(t *testing.T) {
	err := error(toolbind.CodedError{Code: "404", Message: "no such account"})
	if err.Error() != "no such account" {
		t.Errorf("Error() = %q, want the message", err.Error())
	}
}

// errors.As is how the runtime finds it, so it has to survive wrapping — a
// handler that adds context with %w is the ordinary case, not an exception.
func TestACodedErrorSurvivesWrapping(t *testing.T) {
	wrapped := fmt.Errorf("looking up the account: %w",
		toolbind.CodedError{Code: "404", Message: "no such account"})

	var coded toolbind.CodedError
	if !errors.As(wrapped, &coded) {
		t.Fatal("a wrapped CodedError was not found; a handler that added context " +
			"would silently lose its code")
	}
	if coded.Code != "404" || coded.Message != "no such account" {
		t.Errorf("got %+v, want 404/no such account", coded)
	}
}

// A plain error is not one of these, and must not be mistaken for one — the
// zero value's empty code would reach the wire as a reply with no code at all.
func TestAPlainErrorIsNotACodedError(t *testing.T) {
	var coded toolbind.CodedError
	if errors.As(errors.New("the database is down"), &coded) {
		t.Error("a plain error matched CodedError")
	}
}
