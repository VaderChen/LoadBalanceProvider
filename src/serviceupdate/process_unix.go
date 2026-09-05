//go:build !windows

package serviceupdate

import (
	"os/exec"
	"syscall"
)

func detachUpdateProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
}
