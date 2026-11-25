package worker

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// MarkUndumpable turns off the process's dumpable flag. The kernel then gives
// its /proc files to root, so a program it starts, running as the same user,
// cannot read its environment or memory, where its client secret and token
// live. ssh-agent guards its keys the same way.
func MarkUndumpable() error {
	if err := unix.Prctl(unix.PR_SET_DUMPABLE, 0, 0, 0, 0); err != nil {
		return fmt.Errorf("mark the worker undumpable: %w", err)
	}
	return nil
}
