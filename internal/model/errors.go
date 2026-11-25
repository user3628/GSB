package model

import "errors"

var (
	ErrZeroDenominator   = errors.New("fixed point division by zero")
	ErrUnknownField      = errors.New("unknown fingerprint field")
	ErrInvalidTransition = errors.New("illegal alert state transition")
)
