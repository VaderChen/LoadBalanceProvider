package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

func xmlText(value string) string {
	var b bytes.Buffer
	_ = xml.EscapeText(&b, []byte(value))
	return b.String()
}

func installerID(prefix, relative string) string {
	digest := sha256.Sum256([]byte(filepath.ToSlash(relative)))
	return fmt.Sprintf("%s%x", prefix, digest[:12])
}

func productGUID(version string, t target) string {
	digest := sha256.Sum256([]byte("LoadBalanceProvider:" + version + ":" + t.os + "/" + t.arch))
	digest[6] = (digest[6] & 0x0f) | 0x50
	digest[8] = (digest[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", digest[:4], digest[4:6], digest[6:8], digest[8:10], digest[10:16])
}

func buildMSI(platform, version string, date time.Time, t target) error {
	tool := os.Getenv("LBP_WIXL")
	if tool == "" {
		tool = "wixl"
	}
	tool, err := exec.LookPath(tool)
	if err != nil {
		return fmt.Errorf("MSI 需要 msitools 的 wixl，可使用 LBP_WIXL 指定路徑: %w", err)
	}
	var directories, refs strings.Builder
	program := filepath.Join(platform, "program")
	err = filepath.WalkDir(program, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == program {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 || (!entry.IsDir() && !entry.Type().IsRegular()) {
			return fmt.Errorf("拒絕封裝非一般檔案: %s", path)
		}
		relative, err := filepath.Rel(program, path)
		if err != nil {
			return err
		}
		parent := filepath.Dir(relative)
		parentID := "INSTALLFOLDER"
		if parent != "." {
			parentID = installerID("D", parent)
		}
		if entry.IsDir() {
			fmt.Fprintf(&directories, "<DirectoryRef Id=\"%s\"><Directory Id=\"%s\" Name=\"%s\" /></DirectoryRef>\n", parentID, installerID("D", relative), xmlText(entry.Name()))
			return nil
		}
		id := installerID("C", relative)
		shortcut := ""
		if relative == appName+".exe" {
			shortcut = `<Shortcut Id="StartMenuShortcut" Directory="ProgramMenuFolder" Name="LoadBalanceProvider" WorkingDirectory="INSTALLFOLDER" Arguments="--desktop" Advertise="yes" />`
		}
		fmt.Fprintf(&directories, "<DirectoryRef Id=\"%s\"><Component Id=\"%s\" Guid=\"*\" Win64=\"yes\"><File Id=\"%s\" Name=\"%s\" Source=\"%s\" KeyPath=\"yes\">%s</File></Component></DirectoryRef>\n", parentID, id, installerID("F", relative), xmlText(entry.Name()), xmlText(path), shortcut)
		fmt.Fprintf(&refs, "<ComponentRef Id=\"%s\" />\n", id)
		return nil
	})
	if err != nil {
		return err
	}
	upgrade := "CE2359CF-45CB-4A97-9B11-11B6DE8286D6"
	if t.arch == "arm64" {
		upgrade = "BB9FF983-DB06-4E03-A373-9706C24F4A26"
	}
	// MSI 比較前三段版本；把當日分鐘納入第三段，讓同日新版也能正確升級。
	msiVersion := fmt.Sprintf("%d.%d.%d", date.Year()%100, date.Month(), date.Day()*1440+date.Hour()*60+date.Minute())
	source := fmt.Sprintf(`<?xml version="1.0" encoding="UTF-8"?>
<Wix xmlns="http://schemas.microsoft.com/wix/2006/wi">
<Product Id="%s" Name="LoadBalanceProvider" Manufacturer="VaderChen" Language="1033" Version="%s" UpgradeCode="%s">
<Package InstallerVersion="500" Compressed="yes" InstallScope="perMachine" Description="LoadBalanceProvider %s" />
<MajorUpgrade DowngradeErrorMessage="A newer version of LoadBalanceProvider is already installed." />
<MediaTemplate EmbedCab="yes" />
<Directory Id="TARGETDIR" Name="SourceDir">
<Directory Id="ProgramFiles64Folder"><Directory Id="INSTALLFOLDER" Name="LoadBalanceProvider" /></Directory>
<Directory Id="ProgramMenuFolder" />
</Directory>
%s
<Feature Id="ProductFeature" Title="LoadBalanceProvider" Level="1">%s</Feature>
</Product></Wix>
`, productGUID(version, t), msiVersion, upgrade, xmlText(version), directories.String(), refs.String())
	work, err := os.MkdirTemp(platform, ".msi-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	wxs := filepath.Join(work, "installer.wxs")
	if err := os.WriteFile(wxs, []byte(source), 0600); err != nil {
		return err
	}
	name := appName + "-" + strings.Replace(version, " build ", "-build-", 1) + "-" + t.directory + "-unsigned.msi"
	output := filepath.Join(work, name)
	if err := run(exec.Command(tool, "-a", "x64", "-o", output, wxs)); err != nil {
		return fmt.Errorf("建立 MSI: %w", err)
	}
	if t.arch == "arm64" {
		if err := run(exec.Command("msibuild", output, "-s", appName+" "+version, "VaderChen", "Arm64;1033")); err != nil {
			return fmt.Errorf("設定 ARM64 MSI 平台資訊: %w", err)
		}
	}
	return os.Rename(output, filepath.Join(platform, name))
}
