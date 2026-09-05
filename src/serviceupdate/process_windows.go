package serviceupdate

import "os/exec"

// Windows 安裝版透過 MSI 升級，不會執行 Unix 自我更新程序。
func detachUpdateProcess(command *exec.Cmd) {}
