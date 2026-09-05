package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

func main() {
	executable, err := os.Executable()
	if err == nil {
		start := filepath.Join(filepath.Dir(executable), "..", "Resources", "start.command")
		err = exec.Command("/usr/bin/open", "-a", "Terminal", start).Run()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "無法開啟服務視窗:", err)
		os.Exit(1)
	}
}
