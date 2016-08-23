package kingsmoot

import "fmt"

type ErrorCode int

const (
	InvalidArgument ErrorCode = 1
	KeyNotFound     ErrorCode = 2
	KeyExists       ErrorCode = 3
	CompareFailed   ErrorCode = 4
	DataStoreError  ErrorCode = 5
	Timeout         ErrorCode = 6
)

type Error interface {
	error
	Code() ErrorCode
	Message() string
	Cause() error
}

type InvalidArgumentError struct {
	code     ErrorCode
	Name     string
	Value    string
	Expected string
	cause    error
}

func (iae *InvalidArgumentError) Code() ErrorCode {
	return iae.code
}
func (iae *InvalidArgumentError) Cause() error {
	return iae.cause
}
func (iae *InvalidArgumentError) Message() string {
	if iae.Cause() == nil {
		return fmt.Sprintf("Value of argument %v passed [%v] is invalid, expected %v",
			iae.Name,
			iae.Value,
			iae.Expected)
	}
	return fmt.Sprintf("Value of argument %v passed [%v] is invalid, expected %v, Cause:%v",
		iae.Name,
		iae.Value,
		iae.Expected,
		iae.Cause())
}
func (iae *InvalidArgumentError) Error() string {
	return fmt.Sprintf("%v:%v",
		iae.Code(),
		iae.Message())
}

type OpError struct {
	code  ErrorCode
	op    string
	cause error
}

func (ce *OpError) Code() ErrorCode {
	return ce.code
}
func (ce *OpError) Cause() error {
	return ce.cause
}
func (ce *OpError) Message() string {
	return fmt.Sprintf("Failed during %v due to %v", ce.op, ce.Cause())
}
func (ce *OpError) Error() string {
	return fmt.Sprintf("%v:%v", ce.Code(), ce.Message())
}
