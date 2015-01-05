package qlbdriver

import "fmt"

var (
	ErrNotSupported   = fmt.Errorf("QLB: Not supported")
	ErrNotImplemented = fmt.Errorf("QLB: Not implemented")
	ErrUnknownCommand = fmt.Errorf("QLB: Unknown Command")
	ErrInternalError  = fmt.Errorf("QLB: Internal Error")
)
