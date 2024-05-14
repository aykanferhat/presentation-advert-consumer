package util

import "unsafe"

func ToByte(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

func ToPtr[T any](t T) *T {
	return &t
}
