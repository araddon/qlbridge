package gentypes

import (
	"fmt"
)

// MissingFieldErrors are returned when a segment can't be evaluated due to a
// referenced field missing from a schema.
type MissingFieldError struct {
	Field string
}

// MissingField creates a new MissingFieldError for the given field.
func MissingField(field string) *MissingFieldError {
	return &MissingFieldError{field}
}

func (m *MissingFieldError) Reason() string { return m.Error() }
func (m *MissingFieldError) Status() int    { return 400 }

func (m *MissingFieldError) Error() string {
	return fmt.Sprintf("missing field %s", m.Field)
}
