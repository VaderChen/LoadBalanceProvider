package main

import (
	"debug/buildinfo"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const appName = "LoadBalanceProvider"

type target struct{ os, arch, directory string }

func main() {
	version := flag.String("version", "", "版本：1.YY.MMDD build HHmm")
	targets := flag.String("targets", "darwin/arm64,windows/amd64", "逗號分隔的目標平台")
	noBuild := flag.Bool("no-build", false, "沿用該版本的安裝資源")
	flag.Parse()
	if err := stage(*version, *targets, *noBuild); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func parseVersion(version string) (time.Time, error) {
	if !regexp.MustCompile(`^1\.[0-9]{2}\.[0-9]{4} build [0-9]{4}$`).MatchString(version) {
		return time.Time{}, fmt.Errorf("版本格式必須為 1.YY.MMDD build HHmm")
	}
	return time.Parse("06.0102 build 1504", strings.TrimPrefix(version, "1."))
}

func stage(version, rawTargets string, noBuild bool) error {
	date, err := parseVersion(version)
	if err != nil {
		return err
	}
	var targets []target
	seen := map[string]bool{}
	for _, raw := range strings.Split(rawTargets, ",") {
		raw = strings.TrimSpace(raw)
		parts := strings.Split(raw, "/")
		if len(parts) != 2 || (parts[0] != "darwin" && parts[0] != "windows") || (parts[1] != "arm64" && parts[1] != "amd64") {
			return fmt.Errorf("不支援的安裝目標: %s", raw)
		}
		if seen[raw] {
			continue
		}
		seen[raw] = true
		directory := strings.ReplaceAll(raw, "/", "-")
		directory = strings.ReplaceAll(directory, "darwin-", "macos-")
		targets = append(targets, target{parts[0], parts[1], directory})
	}
	root, err := filepath.Abs(".")
	if err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(root, "src/cmd/loadbalanceprovider/main.go")); err != nil {
		return fmt.Errorf("請在 LoadBalanceProvider 專案根目錄執行: %w", err)
	}
	dist := filepath.Join(root, "dist")
	if info, err := os.Lstat(dist); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("dist 不可為符號連結")
	}
	if err := os.MkdirAll(dist, 0755); err != nil {
		return err
	}
	release := filepath.Join(dist, strings.Replace(version, " build ", "-build-", 1))
	if noBuild {
		if info, err := os.Lstat(release); err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("找不到該版本的安裝資源: %s", release)
		}
		for _, t := range targets {
			platform := filepath.Join(release, t.directory)
			binary := binaryPath(platform, t)
			if err := verifyBinary(binary, t); err != nil {
				return err
			}
			if t.os == "darwin" {
				if err := verifyBinary(filepath.Join(platform, appName+".app", "Contents", "MacOS", appName), t); err != nil {
					return err
				}
			}
			data, err := os.ReadFile(filepath.Join(filepath.Dir(binary), "payload", "build-info.json"))
			if err != nil {
				return err
			}
			var metadata struct{ Service, Version string }
			if json.Unmarshal(data, &metadata) != nil || metadata.Service != appName || metadata.Version != version {
				return fmt.Errorf("快取資源的產品或版本不符: %s", platform)
			}
			if err := validateCachedPlatform(platform); err != nil {
				return err
			}
			if t.os == "windows" {
				if err := buildMSI(filepath.Join(release, t.directory), version, date, t); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if _, err := os.Lstat(release); err == nil {
		return fmt.Errorf("版本已存在，請指定新版本或使用 --no-build: %s", release)
	} else if !os.IsNotExist(err) {
		return err
	}
	work, err := os.MkdirTemp(dist, ".installer-build-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	for _, t := range targets {
		platform := filepath.Join(work, t.directory)
		binary := binaryPath(platform, t)
		if err := os.MkdirAll(filepath.Dir(binary), 0755); err != nil {
			return err
		}
		fmt.Printf("建置 %s/%s\n", t.os, t.arch)
		if err := buildExecutable("./src/cmd/loadbalanceprovider", binary, t); err != nil {
			return err
		}
		payload := filepath.Join(filepath.Dir(binary), "payload")
		if err := os.MkdirAll(payload, 0755); err != nil {
			return err
		}
		for _, name := range []string{"website", "questBank", "agent.sample.properties", "README.md", "DEPLOY.md", "install.md"} {
			if err := copyTree(filepath.Join(root, name), filepath.Join(payload, name)); err != nil {
				return err
			}
		}
		metadata, err := json.MarshalIndent(map[string]interface{}{"service": appName, "version": version, "build_time": time.Now().UTC().Format(time.RFC3339), "targets": []string{t.os + "/" + t.arch}}, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(payload, "build-info.json"), metadata, 0644); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(payload, "BUILD_INFO.txt"), []byte("app="+appName+"\nversion="+version+"\ninstaller=true\n"), 0644); err != nil {
			return err
		}
		if t.os == "darwin" {
			if err := macBundle(platform, date, t); err != nil {
				return err
			}
		} else if err := buildMSI(platform, version, date, t); err != nil {
			return err
		}
	}
	return os.Rename(work, release)
}

func validateCachedPlatform(platform string) error {
	return filepath.WalkDir(platform, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Type()&os.ModeSymlink != 0 || (!entry.IsDir() && !entry.Type().IsRegular()) {
			return fmt.Errorf("安裝資源含不支援的檔案類型: %s", path)
		}
		name := strings.ToLower(entry.Name())
		if name == "cmd" || name == "data" || name == "usage" || name == "agent.properties" || strings.HasSuffix(name, ".bak") || strings.HasSuffix(name, ".log") {
			return fmt.Errorf("安裝資源含禁止封裝的檔案: %s", path)
		}
		return nil
	})
}

func binaryPath(platform string, t target) string {
	if t.os == "darwin" {
		return filepath.Join(platform, appName+".app", "Contents", "Resources", appName)
	}
	return filepath.Join(platform, "program", appName+".exe")
}

func verifyBinary(path string, t target) error {
	info, err := buildinfo.ReadFile(path)
	if err != nil {
		return err
	}
	settings := map[string]string{}
	for _, s := range info.Settings {
		settings[s.Key] = s.Value
	}
	if settings["GOOS"] != t.os || settings["GOARCH"] != t.arch || settings["-trimpath"] != "true" {
		return fmt.Errorf("執行檔平台不符或缺少 trimpath: %s", path)
	}
	return nil
}

func copyTree(source, destination string) error {
	return filepath.WalkDir(source, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.Name() == ".DS_Store" || strings.HasSuffix(entry.Name(), ".bak") || strings.HasSuffix(entry.Name(), ".log") || strings.HasPrefix(entry.Name(), ".") {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0755)
		}
		if !entry.Type().IsRegular() {
			return fmt.Errorf("拒絕封裝非一般檔案: %s", path)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		_, err = io.Copy(output, input)
		closeErr := output.Close()
		if err != nil {
			return err
		}
		return closeErr
	})
}

func run(command *exec.Cmd) error {
	command.Stdout, command.Stderr = os.Stdout, os.Stderr
	return command.Run()
}

func buildExecutable(packagePath, output string, t target) error {
	command := exec.Command("go", "build", "-buildvcs=false", "-trimpath", "-ldflags=-s -w", "-o", output, packagePath)
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		if key != "GOOS" && key != "GOARCH" && key != "CGO_ENABLED" {
			command.Env = append(command.Env, value)
		}
	}
	command.Env = append(command.Env, "GOOS="+t.os, "GOARCH="+t.arch, "CGO_ENABLED=0")
	return run(command)
}

func macBundle(platform string, date time.Time, t target) error {
	contents := filepath.Join(platform, appName+".app", "Contents")
	if err := os.MkdirAll(filepath.Join(contents, "MacOS"), 0755); err != nil {
		return err
	}
	plist := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
<key>CFBundleIdentifier</key><string>com.vaderchen.loadbalanceprovider</string>
<key>CFBundleName</key><string>LoadBalanceProvider</string>
<key>CFBundleExecutable</key><string>LoadBalanceProvider</string>
<key>CFBundlePackageType</key><string>APPL</string>
<key>CFBundleShortVersionString</key><string>1.%d.%d</string>
<key>CFBundleVersion</key><string>%d.%d.%d</string>
<key>LSMinimumSystemVersion</key><string>12.0</string>
<key>LSUIElement</key><true/>
</dict></plist>
`, date.Year()%100, int(date.Month())*100+date.Day(), date.Year()%100, int(date.Month())*100+date.Day(), date.Hour()*100+date.Minute())
	if err := os.WriteFile(filepath.Join(contents, "Info.plist"), []byte(plist), 0644); err != nil {
		return err
	}
	// 服務以 Terminal 前景執行，保留 Ctrl+C 正常關閉與診斷輸出。
	if err := buildExecutable("./src/cmd/desktoplauncher", filepath.Join(contents, "MacOS", appName), t); err != nil {
		return err
	}
	start := "#!/bin/bash\nset -euo pipefail\ncd \"$(dirname \"$0\")\"\nexec ./LoadBalanceProvider --desktop\n"
	return os.WriteFile(filepath.Join(contents, "Resources", "start.command"), []byte(start), 0755)
}
