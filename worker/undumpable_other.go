//go:build !linux

package worker

// MarkUndumpable does nothing where the kernel is not Linux, which has no
// /proc for a program to read the worker's environment through.
func MarkUndumpable() error { return nil }
