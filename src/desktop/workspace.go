package desktop

import (
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
)

// Prepare 將安裝資源與可寫資料分開；既有 agent.properties 與 data 不在複製範圍。
func Prepare() (func(), error) {
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return nil, err
	}
	if runtime.GOOS == "windows" {
		base = os.Getenv("LOCALAPPDATA")
		if base == "" {
			return nil, fmt.Errorf("缺少 LOCALAPPDATA")
		}
	}
	root := filepath.Join(base, "LoadBalanceProvider")
	if info, err := os.Lstat(root); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("資料目錄不可為符號連結: %s", root)
	} else if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	lock, err := lockWorkspace(filepath.Join(root, ".desktop.lock"))
	if err != nil {
		return nil, fmt.Errorf("無法鎖定資料目錄，請確認沒有另一個安裝版正在執行: %w", err)
	}
	cleanup := func() { _ = lock.Close() }
	payload := filepath.Join(filepath.Dir(executable), "payload")
	for _, name := range []string{"website", "questBank", "agent.sample.properties", "build-info.json", "BUILD_INFO.txt"} {
		if err := copyResource(filepath.Join(payload, name), filepath.Join(root, name)); err != nil {
			cleanup()
			return nil, err
		}
	}
	if err := os.Chdir(root); err != nil {
		cleanup()
		return nil, err
	}
	if err := os.Setenv("LBP_DESKTOP_MODE", "1"); err != nil {
		cleanup()
		return nil, err
	}
	fmt.Println("資料目錄:", root)
	fmt.Println("請使用瀏覽器開啟設定的 HTTP 位址；首次安裝預設為 http://localhost:10081")
	fmt.Println("停止服務請在此視窗按 Ctrl+C；升級前請先停止服務。")
	return cleanup, nil
}

func copyResource(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.Type()&os.ModeSymlink != 0 {
			return fmt.Errorf("資源不可包含符號連結: %s", path)
		}
		if info, err := os.Lstat(target); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("資料路徑不可為符號連結: %s", target)
		} else if err != nil && !os.IsNotExist(err) {
			return err
		}
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("不支援的資源類型: %s", path)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.CreateTemp(filepath.Dir(target), ".resource-*")
		if err != nil {
			return err
		}
		defer os.Remove(output.Name())
		_, copyErr := io.Copy(output, input)
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		return os.Rename(output.Name(), target)
	})
}
