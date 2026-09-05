package serviceupdate

import (
	"archive/zip"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	MaxUploadBytes   = 128 << 20
	maxArchiveFiles  = 4096
	maxSingleFile    = 128 << 20
	maxExtractedSize = 512 << 20
)

var allowedRootFiles = map[string]bool{
	"build-info.json":         true,
	"BUILD_INFO.txt":          true,
	"agent.sample.properties": true,
	"README.md":               true,
	"DEPLOY.md":               true,
	"install.md":              true,
	"install.sh":              true,
	"run.sh":                  true,
	"run.command":             true,
	"run_bg.sh":               true,
	"stop.sh":                 true,
}

var updateMu sync.Mutex

type BuildInfo struct {
	Service   string   `json:"service"`
	Version   string   `json:"version"`
	BuildTime string   `json:"build_time"`
	Targets   []string `json:"targets"`
}

type Result struct {
	OperationID        string `json:"operation_id"`
	FileName           string `json:"file_name"`
	Version            string `json:"version"`
	Platform           string `json:"platform"`
	ServiceManager     string `json:"service_manager"`
	ServiceUnit        string `json:"service_unit,omitempty"`
	Status             string `json:"status"`
	ExpectedMainSHA256 string `json:"expected_main_sha256"`
}

// Status is persisted outside the staging directory so an administrator can
// verify that the uploaded package, static website and restarted process all
// belong to the same update operation.
type Status struct {
	OperationID        string `json:"operation_id,omitempty"`
	FileName           string `json:"file_name,omitempty"`
	Version            string `json:"version,omitempty"`
	Platform           string `json:"platform,omitempty"`
	ServiceManager     string `json:"service_manager,omitempty"`
	ServiceUnit        string `json:"service_unit,omitempty"`
	State              string `json:"state"`
	ScheduledAt        string `json:"scheduled_at,omitempty"`
	UpdatedAt          string `json:"updated_at,omitempty"`
	TargetCount        int    `json:"target_count,omitempty"`
	ExpectedMainSHA256 string `json:"expected_main_sha256,omitempty"`
	CurrentMainSHA256  string `json:"current_main_sha256,omitempty"`
	MainApplied        bool   `json:"main_applied"`
}

type stagedPackage struct {
	Info        BuildInfo
	StageDir    string
	TargetFiles []string
	BinaryPath  string
}

type serviceManager struct {
	Mode string
	Unit string
}

// ReadMultipartUpload extracts the ZIP package from a multipart/form-data request.
func ReadMultipartUpload(_contentType string, _body []byte) ([]byte, string, error) {
	if int64(len(_body)) > MaxUploadBytes+(1<<20) {
		return nil, "", fmt.Errorf("更新檔案不得超過 %d MB", MaxUploadBytes>>20)
	}

	_mediaType, _params, _err := mime.ParseMediaType(strings.TrimSpace(_contentType))
	if _err != nil || !strings.EqualFold(_mediaType, "multipart/form-data") {
		return nil, "", errors.New("更新請求必須使用 multipart/form-data")
	}
	_boundary := strings.TrimSpace(_params["boundary"])
	if _boundary == "" {
		return nil, "", errors.New("更新請求缺少 multipart boundary")
	}

	_reader := multipart.NewReader(bytes.NewReader(_body), _boundary)
	for {
		_part, _nextErr := _reader.NextPart()
		if errors.Is(_nextErr, io.EOF) {
			break
		}
		if _nextErr != nil {
			return nil, "", fmt.Errorf("讀取更新檔案失敗: %w", _nextErr)
		}
		if _part.FileName() == "" {
			_ = _part.Close()
			continue
		}

		_fileName := filepath.Base(strings.TrimSpace(_part.FileName()))
		if !strings.EqualFold(filepath.Ext(_fileName), ".zip") {
			_ = _part.Close()
			return nil, "", errors.New("只接受 ZIP 更新檔")
		}
		_data, _readErr := io.ReadAll(io.LimitReader(_part, MaxUploadBytes+1))
		_ = _part.Close()
		if _readErr != nil {
			return nil, "", fmt.Errorf("讀取更新檔案失敗: %w", _readErr)
		}
		if len(_data) == 0 {
			return nil, "", errors.New("更新檔案不可為空")
		}
		if int64(len(_data)) > MaxUploadBytes {
			return nil, "", fmt.Errorf("更新檔案不得超過 %d MB", MaxUploadBytes>>20)
		}
		return _data, _fileName, nil
	}

	return nil, "", errors.New("更新請求中找不到 ZIP 檔案")
}

// PrepareAndLaunch validates and stages a deployment package, then starts a detached updater.
func PrepareAndLaunch(_archive []byte, _fileName string) (Result, error) {
	updateMu.Lock()
	defer updateMu.Unlock()
	if os.Getenv("LBP_DESKTOP_MODE") == "1" {
		return Result{}, errors.New("DMG／MSI 安裝版請先停止服務，再使用新版安裝包升級；不支援套用部署 ZIP")
	}

	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		return Result{}, fmt.Errorf("目前不支援在 %s 平台執行自我更新", runtime.GOOS)
	}

	_root, _err := serviceRoot()
	if _err != nil {
		return Result{}, _err
	}
	_updateRoot := filepath.Join(_root, "data", "system", "service_update")
	_operationID := time.Now().Format("20060102_150405") + "_" + randomSuffix()
	_operationDir := filepath.Join(_updateRoot, "staging", _operationID)
	_stageDir := filepath.Join(_operationDir, "package")
	_backupDir := filepath.Join(_updateRoot, "backups", _operationID)
	if _err := os.MkdirAll(_stageDir, 0750); _err != nil {
		return Result{}, fmt.Errorf("建立更新暫存目錄失敗: %w", _err)
	}

	_archivePath := filepath.Join(_operationDir, "update.zip")
	if _err := os.WriteFile(_archivePath, _archive, 0600); _err != nil {
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("儲存更新檔案失敗: %w", _err)
	}

	_staged, _err := extractAndValidate(_archive, _stageDir)
	if _err != nil {
		_ = os.RemoveAll(_operationDir)
		return Result{}, _err
	}
	_expectedMainSHA256, _err := fileSHA256(filepath.Join(_staged.StageDir, "website", "main.html"))
	if _err != nil {
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("讀取更新包 website/main.html 失敗: %w", _err)
	}

	_targetListPath := filepath.Join(_operationDir, "update-targets.txt")
	if _err := os.WriteFile(_targetListPath, []byte(strings.Join(_staged.TargetFiles, "\n")+"\n"), 0600); _err != nil {
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("建立更新目標清單失敗: %w", _err)
	}

	_scriptPath := filepath.Join(_operationDir, "apply-update.sh")
	if _err := os.WriteFile(_scriptPath, []byte(unixUpdaterScript), 0700); _err != nil {
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("建立更新程序失敗: %w", _err)
	}

	_logPath := filepath.Join(_updateRoot, "service_update.log")
	if _err := os.MkdirAll(filepath.Dir(_logPath), 0750); _err != nil {
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("建立更新記錄目錄失敗: %w", _err)
	}
	_logFile, _err := os.OpenFile(_logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0640)
	if _err != nil {
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("開啟更新記錄失敗: %w", _err)
	}
	_statusPath := filepath.Join(_updateRoot, "latest.json")
	_statePath := filepath.Join(_updateRoot, "latest.state")
	_manager := currentServiceManager()
	_now := time.Now().Format(time.RFC3339)
	_status := Status{
		OperationID:        _operationID,
		FileName:           filepath.Base(_fileName),
		Version:            _staged.Info.Version,
		Platform:           runtime.GOOS + "/" + runtime.GOARCH,
		ServiceManager:     _manager.Mode,
		ServiceUnit:        _manager.Unit,
		State:              "scheduled",
		ScheduledAt:        _now,
		UpdatedAt:          _now,
		TargetCount:        len(_staged.TargetFiles),
		ExpectedMainSHA256: _expectedMainSHA256,
	}
	if _err := writeStatusFile(_statusPath, _status); _err != nil {
		_ = _logFile.Close()
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("建立更新狀態失敗: %w", _err)
	}
	if _err := writeStateFile(_statePath, _operationID, "scheduled"); _err != nil {
		_ = _logFile.Close()
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("建立更新階段狀態失敗: %w", _err)
	}
	_, _ = fmt.Fprintf(_logFile, "%s update scheduled: operation=%s file=%s version=%s targets=%d manager=%s unit=%s main_sha256=%s\n",
		time.Now().Format("2006-01-02 15:04:05"), _operationID, filepath.Base(_fileName), _staged.Info.Version,
		len(_staged.TargetFiles), _manager.Mode, _manager.Unit, _expectedMainSHA256)
	_ = _logFile.Sync()

	_scriptArgs := []string{
		_staged.StageDir,
		_backupDir,
		_targetListPath,
		fmt.Sprintf("%d", os.Getpid()),
		_staged.BinaryPath,
		_operationDir,
		_statePath,
		_operationID,
		_manager.Mode,
		_manager.Unit,
		_logPath,
	}
	var _command *exec.Cmd
	_managedLaunch := _manager.Mode == "systemd-system" || _manager.Mode == "systemd-user"
	if _managedLaunch {
		_transientUnit := "load-balance-provider-update-" + strings.ReplaceAll(_operationID, "_", "-")
		_runArgs := []string{"--unit=" + _transientUnit, "--collect", "--quiet"}
		if _manager.Mode == "systemd-user" {
			_runArgs = append([]string{"--user"}, _runArgs...)
		}
		_runArgs = append(_runArgs, "/bin/sh", _scriptPath)
		_runArgs = append(_runArgs, _scriptArgs...)
		_command = exec.Command("systemd-run", _runArgs...)
	} else {
		_command = exec.Command("/bin/sh", append([]string{_scriptPath}, _scriptArgs...)...)
		detachUpdateProcess(_command)
	}
	_command.Stdout = _logFile
	_command.Stderr = _logFile
	_command.Stdin = nil
	var _launchErr error
	if _managedLaunch {
		// systemd-run returns after the transient unit has been accepted. Running
		// the short client synchronously lets the API reject a failed enqueue.
		_launchErr = _command.Run()
	} else {
		_launchErr = _command.Start()
	}
	if _launchErr != nil {
		_ = writeStateFile(_statePath, _operationID, "failed")
		_ = _logFile.Close()
		_ = os.RemoveAll(_operationDir)
		return Result{}, fmt.Errorf("啟動更新程序失敗: %w", _launchErr)
	}
	if !_managedLaunch {
		_ = _command.Process.Release()
	}
	_ = _logFile.Close()

	return Result{
		OperationID:        _operationID,
		FileName:           filepath.Base(_fileName),
		Version:            _staged.Info.Version,
		Platform:           runtime.GOOS + "/" + runtime.GOARCH,
		ServiceManager:     _manager.Mode,
		ServiceUnit:        _manager.Unit,
		Status:             "scheduled",
		ExpectedMainSHA256: _expectedMainSHA256,
	}, nil
}

// currentServiceManager detects whether this process is the MainPID of a
// systemd service. The updater must leave that service's cgroup before it can
// stop a Restart=always unit; otherwise systemd either kills the updater with
// KillMode=control-group or races it by immediately starting the old binary.
func currentServiceManager() serviceManager {
	if runtime.GOOS != "linux" {
		return serviceManager{Mode: "process"}
	}
	if _, _err := exec.LookPath("systemd-run"); _err != nil {
		return serviceManager{Mode: "process"}
	}
	if _, _err := exec.LookPath("systemctl"); _err != nil {
		return serviceManager{Mode: "process"}
	}

	type _candidate struct {
		Mode string
		Unit string
	}
	_candidates := make([]_candidate, 0, 4)
	_add := func(_mode string, _unit string) {
		_unit = strings.TrimSpace(_unit)
		if (_mode != "systemd-system" && _mode != "systemd-user") || !validSystemdUnit(_unit) {
			return
		}
		for _, _existing := range _candidates {
			if _existing.Mode == _mode && _existing.Unit == _unit {
				return
			}
		}
		_candidates = append(_candidates, _candidate{Mode: _mode, Unit: _unit})
	}

	if _unit := strings.TrimSpace(os.Getenv("LBP_SYSTEMD_UNIT")); _unit != "" {
		_mode := "systemd-system"
		if strings.EqualFold(strings.TrimSpace(os.Getenv("LBP_SYSTEMD_SCOPE")), "user") {
			_mode = "systemd-user"
		}
		_add(_mode, _unit)
	}
	if _data, _err := os.ReadFile("/proc/self/cgroup"); _err == nil {
		for _, _line := range strings.Split(string(_data), "\n") {
			_fields := strings.SplitN(_line, ":", 3)
			if len(_fields) != 3 {
				continue
			}
			_parts := strings.Split(strings.Trim(_fields[2], "/"), "/")
			for _idx := len(_parts) - 1; _idx >= 0; _idx-- {
				if !strings.HasSuffix(_parts[_idx], ".service") {
					continue
				}
				_mode := "systemd-system"
				if strings.Contains(_fields[2], "/user.slice/") || strings.Contains(_fields[2], "/app.slice/") {
					_mode = "systemd-user"
				}
				_add(_mode, _parts[_idx])
				break
			}
		}
	}
	_add("systemd-system", "load-balance-provider.service")

	_currentPID := fmt.Sprintf("%d", os.Getpid())
	for _, _candidate := range _candidates {
		_args := []string{"show", _candidate.Unit, "--property=MainPID", "--value"}
		if _candidate.Mode == "systemd-user" {
			_args = append([]string{"--user"}, _args...)
		}
		_output, _err := exec.Command("systemctl", _args...).Output()
		if _err == nil && strings.TrimSpace(string(_output)) == _currentPID {
			return serviceManager{Mode: _candidate.Mode, Unit: _candidate.Unit}
		}
	}
	return serviceManager{Mode: "process"}
}

func validSystemdUnit(_unit string) bool {
	if !strings.HasSuffix(_unit, ".service") || len(_unit) > 255 {
		return false
	}
	for _, _char := range _unit {
		if (_char >= 'a' && _char <= 'z') || (_char >= 'A' && _char <= 'Z') ||
			(_char >= '0' && _char <= '9') || _char == '-' || _char == '_' || _char == '.' || _char == '@' || _char == ':' || _char == '\\' {
			continue
		}
		return false
	}
	return true
}

// CurrentStatus returns the last update operation together with the hash of
// the website currently served from the service root.
func CurrentStatus() (Status, error) {
	_root, _err := serviceRoot()
	if _err != nil {
		return Status{}, _err
	}
	_updateRoot := filepath.Join(_root, "data", "system", "service_update")
	_statusPath := filepath.Join(_updateRoot, "latest.json")
	_data, _err := os.ReadFile(_statusPath)
	if errors.Is(_err, os.ErrNotExist) {
		return Status{State: "idle"}, nil
	}
	if _err != nil {
		return Status{}, fmt.Errorf("讀取更新狀態失敗: %w", _err)
	}

	var _status Status
	if _err := json.Unmarshal(_data, &_status); _err != nil {
		return Status{}, fmt.Errorf("更新狀態格式不正確: %w", _err)
	}
	_stateBytes, _stateErr := os.ReadFile(filepath.Join(_updateRoot, "latest.state"))
	if _stateErr == nil {
		_fields := strings.Fields(string(_stateBytes))
		if len(_fields) >= 2 && _fields[0] == _status.OperationID {
			_status.State = _fields[1]
			if _info, _statErr := os.Stat(filepath.Join(_updateRoot, "latest.state")); _statErr == nil {
				_status.UpdatedAt = _info.ModTime().Format(time.RFC3339)
			}
		}
	}

	_currentHash, _hashErr := fileSHA256(filepath.Join(_root, "website", "main.html"))
	if _hashErr == nil {
		_status.CurrentMainSHA256 = _currentHash
		_status.MainApplied = _status.ExpectedMainSHA256 != "" && _currentHash == _status.ExpectedMainSHA256
	}
	return _status, nil
}

func writeStatusFile(_path string, _status Status) error {
	_data, _err := json.MarshalIndent(_status, "", "  ")
	if _err != nil {
		return _err
	}
	return writeAtomicFile(_path, append(_data, '\n'), 0640)
}

func writeStateFile(_path string, _operationID string, _state string) error {
	return writeAtomicFile(_path, []byte(_operationID+" "+_state+"\n"), 0640)
}

func writeAtomicFile(_path string, _data []byte, _mode os.FileMode) error {
	if _err := os.MkdirAll(filepath.Dir(_path), 0750); _err != nil {
		return _err
	}
	_tempPath := _path + ".tmp"
	if _err := os.WriteFile(_tempPath, _data, _mode); _err != nil {
		return _err
	}
	if _err := os.Rename(_tempPath, _path); _err != nil {
		_ = os.Remove(_tempPath)
		return _err
	}
	return nil
}

func fileSHA256(_path string) (string, error) {
	_file, _err := os.Open(_path)
	if _err != nil {
		return "", _err
	}
	defer _file.Close()
	_hash := sha256.New()
	if _, _err := io.Copy(_hash, _file); _err != nil {
		return "", _err
	}
	return hex.EncodeToString(_hash.Sum(nil)), nil
}

func extractAndValidate(_archive []byte, _stageDir string) (stagedPackage, error) {
	_reader, _err := zip.NewReader(bytes.NewReader(_archive), int64(len(_archive)))
	if _err != nil {
		return stagedPackage{}, errors.New("無法讀取 ZIP 更新檔")
	}
	if len(_reader.File) == 0 || len(_reader.File) > maxArchiveFiles {
		return stagedPackage{}, fmt.Errorf("ZIP 檔案項目數不合法，最多允許 %d 項", maxArchiveFiles)
	}

	_prefix, _buildFile, _err := findPackagePrefix(_reader.File)
	if _err != nil {
		return stagedPackage{}, _err
	}
	_buildBytes, _err := readZipFile(_buildFile, 1<<20)
	if _err != nil {
		return stagedPackage{}, fmt.Errorf("讀取 build-info.json 失敗: %w", _err)
	}
	var _info BuildInfo
	if _err := json.Unmarshal(_buildBytes, &_info); _err != nil {
		return stagedPackage{}, errors.New("build-info.json 格式不正確")
	}
	if _info.Service != "LoadBalanceProvider" {
		return stagedPackage{}, errors.New("更新檔案不是 LoadBalanceProvider 部署包")
	}
	if strings.TrimSpace(_info.Version) == "" {
		return stagedPackage{}, errors.New("build-info.json 缺少版本資訊")
	}

	_binaryPath, _err := platformBinaryPath()
	if _err != nil {
		return stagedPackage{}, _err
	}

	_seen := map[string]bool{}
	_targets := make([]string, 0, len(_reader.File))
	var _totalSize uint64
	_binaryFound := false
	_stageRoot, _err := filepath.Abs(_stageDir)
	if _err != nil {
		return stagedPackage{}, _err
	}

	for _, _file := range _reader.File {
		_relative, _skip, _pathErr := archiveRelativePath(_file.Name, _prefix)
		if _pathErr != nil {
			return stagedPackage{}, _pathErr
		}
		if _skip || _file.FileInfo().IsDir() {
			continue
		}
		if _file.Mode()&os.ModeSymlink != 0 || !_file.Mode().IsRegular() {
			return stagedPackage{}, fmt.Errorf("更新檔案包含不支援的檔案類型: %s", _relative)
		}
		if !isAllowedTarget(_relative) {
			return stagedPackage{}, fmt.Errorf("更新檔案包含禁止寫入的路徑: %s", _relative)
		}
		if _seen[_relative] {
			return stagedPackage{}, fmt.Errorf("更新檔案包含重複路徑: %s", _relative)
		}
		_seen[_relative] = true
		if _file.UncompressedSize64 > maxSingleFile {
			return stagedPackage{}, fmt.Errorf("更新檔案過大: %s", _relative)
		}
		_totalSize += _file.UncompressedSize64
		if _totalSize > maxExtractedSize {
			return stagedPackage{}, fmt.Errorf("ZIP 解壓縮內容不得超過 %d MB", maxExtractedSize>>20)
		}

		_targetPath, _err := safeJoin(_stageRoot, _relative)
		if _err != nil {
			return stagedPackage{}, _err
		}
		if _err := os.MkdirAll(filepath.Dir(_targetPath), 0750); _err != nil {
			return stagedPackage{}, fmt.Errorf("建立更新目錄失敗: %w", _err)
		}
		_source, _err := _file.Open()
		if _err != nil {
			return stagedPackage{}, fmt.Errorf("開啟更新檔案失敗: %s", _relative)
		}
		_mode := os.FileMode(0644)
		if isExecutableTarget(_relative) {
			_mode = 0755
		}
		_destination, _err := os.OpenFile(_targetPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, _mode)
		if _err != nil {
			_ = _source.Close()
			return stagedPackage{}, fmt.Errorf("建立更新檔案失敗: %s", _relative)
		}
		_, _copyErr := io.Copy(_destination, _source)
		_closeErr := _destination.Close()
		_ = _source.Close()
		if _copyErr != nil || _closeErr != nil {
			return stagedPackage{}, fmt.Errorf("寫入更新檔案失敗: %s", _relative)
		}
		_targets = append(_targets, _relative)
		if _relative == _binaryPath {
			_binaryFound = true
		}
	}

	if !_binaryFound {
		return stagedPackage{}, fmt.Errorf("更新檔案缺少當前平台執行檔: %s", _binaryPath)
	}
	if !_seen["build-info.json"] {
		return stagedPackage{}, errors.New("更新檔案缺少 build-info.json")
	}
	if !_seen["website/main.html"] {
		return stagedPackage{}, errors.New("更新檔案缺少 website/main.html，拒絕只更新執行檔")
	}
	sort.Strings(_targets)
	return stagedPackage{
		Info:        _info,
		StageDir:    _stageRoot,
		TargetFiles: _targets,
		BinaryPath:  _binaryPath,
	}, nil
}

func findPackagePrefix(_files []*zip.File) (string, *zip.File, error) {
	var _prefix string
	var _buildFile *zip.File
	for _, _file := range _files {
		_name, _err := normalizeArchivePath(_file.Name)
		if _err != nil {
			return "", nil, _err
		}
		if _file.FileInfo().IsDir() {
			continue
		}
		_parts := strings.Split(_name, "/")
		if len(_parts) > 2 || _parts[len(_parts)-1] != "build-info.json" {
			continue
		}
		_candidatePrefix := ""
		if len(_parts) == 2 {
			_candidatePrefix = _parts[0] + "/"
		}
		if _buildFile != nil {
			return "", nil, errors.New("ZIP 內只能有一個 build-info.json")
		}
		_prefix = _candidatePrefix
		_buildFile = _file
	}
	if _buildFile == nil {
		return "", nil, errors.New("ZIP 更新檔缺少 build-info.json")
	}
	return _prefix, _buildFile, nil
}

func archiveRelativePath(_name string, _prefix string) (string, bool, error) {
	_normalized, _err := normalizeArchivePath(_name)
	if _err != nil {
		return "", false, _err
	}
	if _prefix != "" {
		_prefixDirectory := strings.TrimSuffix(_prefix, "/")
		if _normalized == _prefixDirectory {
			return "", true, nil
		}
		if !strings.HasPrefix(_normalized, _prefix) {
			return "", false, fmt.Errorf("ZIP 內含封裝目錄外的路徑: %s", _normalized)
		}
		_normalized = strings.TrimPrefix(_normalized, _prefix)
	}
	if _normalized == "" || _normalized == "." {
		return "", true, nil
	}
	return _normalized, false, nil
}

func normalizeArchivePath(_name string) (string, error) {
	if _name == "" || strings.ContainsRune(_name, '\x00') || strings.Contains(_name, "\\") {
		return "", errors.New("ZIP 內含不合法的檔案路徑")
	}
	if strings.HasPrefix(_name, "/") {
		return "", fmt.Errorf("ZIP 內含絕對路徑: %s", _name)
	}
	_clean := path.Clean(strings.TrimSuffix(_name, "/"))
	if _clean == ".." || strings.HasPrefix(_clean, "../") {
		return "", fmt.Errorf("ZIP 內含目錄穿越路徑: %s", _name)
	}
	return _clean, nil
}

func isAllowedTarget(_relative string) bool {
	_relative = path.Clean(_relative)
	if _relative == "agent.properties" || strings.HasSuffix(strings.ToLower(_relative), ".bak") {
		return false
	}
	if strings.HasPrefix(_relative, "data/") || _relative == "data" {
		return false
	}
	if allowedRootFiles[_relative] {
		return true
	}
	return strings.HasPrefix(_relative, "bin/") ||
		strings.HasPrefix(_relative, "website/") ||
		strings.HasPrefix(_relative, "questBank/")
}

func isExecutableTarget(_relative string) bool {
	if strings.HasPrefix(_relative, "bin/") {
		return true
	}
	switch _relative {
	case "install.sh", "run.sh", "run.command", "run_bg.sh", "stop.sh":
		return true
	default:
		return false
	}
}

func platformBinaryPath() (string, error) {
	switch runtime.GOOS + "/" + runtime.GOARCH {
	case "darwin/arm64":
		return "bin/LoadBalanceProvider_mac_arm64", nil
	case "linux/amd64":
		return "bin/LoadBalanceProvider_linux_x64", nil
	case "linux/arm64":
		return "bin/LoadBalanceProvider_linux_arm64", nil
	default:
		return "", fmt.Errorf("更新包不支援目前平台: %s/%s", runtime.GOOS, runtime.GOARCH)
	}
}

func safeJoin(_root string, _relative string) (string, error) {
	_target := filepath.Join(_root, filepath.FromSlash(_relative))
	_cleanRoot := filepath.Clean(_root)
	_cleanTarget := filepath.Clean(_target)
	if _cleanTarget != _cleanRoot && !strings.HasPrefix(_cleanTarget, _cleanRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("更新路徑超出服務目錄: %s", _relative)
	}
	return _cleanTarget, nil
}

func readZipFile(_file *zip.File, _limit int64) ([]byte, error) {
	_reader, _err := _file.Open()
	if _err != nil {
		return nil, _err
	}
	defer _reader.Close()
	_data, _err := io.ReadAll(io.LimitReader(_reader, _limit+1))
	if _err != nil {
		return nil, _err
	}
	if int64(len(_data)) > _limit {
		return nil, errors.New("檔案超過允許大小")
	}
	return _data, nil
}

func serviceRoot() (string, error) {
	if _configured := strings.TrimSpace(os.Getenv("LBP_SERVICE_ROOT")); _configured != "" {
		return filepath.Abs(_configured)
	}
	_current, _err := os.Getwd()
	if _err == nil {
		if fileExists(filepath.Join(_current, "agent.properties")) || fileExists(filepath.Join(_current, "agent.sample.properties")) {
			return filepath.Abs(_current)
		}
	}
	_executable, _err := os.Executable()
	if _err != nil {
		return "", fmt.Errorf("無法判斷服務目錄: %w", _err)
	}
	return filepath.Abs(filepath.Dir(_executable))
}

func fileExists(_name string) bool {
	_info, _err := os.Stat(_name)
	return _err == nil && !_info.IsDir()
}

func randomSuffix() string {
	var _bytes [4]byte
	if _, _err := rand.Read(_bytes[:]); _err == nil {
		return hex.EncodeToString(_bytes[:])
	}
	return fmt.Sprintf("%08x", uint32(time.Now().UnixNano()))
}

const unixUpdaterScript = `#!/bin/sh
set -u

# Keep the service root out of argv so duplicate-instance scanners do not
# mistake this updater shell for another LoadBalanceProvider process.
SCRIPT_DIR=$(CDPATH= cd "$(dirname "$0")" && pwd -P) || exit 1
ROOT=$(CDPATH= cd "$SCRIPT_DIR/../../../../.." && pwd -P) || exit 1
STAGE="$1"
BACKUP="$2"
TARGETS="$3"
OLD_PID="$4"
PLATFORM_BINARY="$5"
OPERATION_DIR="$6"
STATE_FILE="$7"
OPERATION_ID="$8"
SERVICE_MODE="$9"
SERVICE_UNIT="${10}"
LOG_PATH="${11}"
APP="LoadBalanceProvider"
UPDATE_COMPLETED=0
NEW_PID=""

exec >> "$LOG_PATH" 2>&1

timestamp() { date '+%Y-%m-%d %H:%M:%S'; }
log() { printf '%s %s\n' "$(timestamp)" "$*"; }
write_state() {
  state="$1"
  state_tmp="${STATE_FILE}.tmp.$$"
  printf '%s %s\n' "$OPERATION_ID" "$state" > "$state_tmp" && mv -f "$state_tmp" "$STATE_FILE"
}
on_exit() {
  exit_code=$?
  trap - EXIT
  if [ "$UPDATE_COMPLETED" -ne 1 ]; then
    write_state failed 2>/dev/null || true
  fi
  exit "$exit_code"
}
trap on_exit EXIT

start_service() {
  if [ "$SERVICE_MODE" = "systemd-system" ] || [ "$SERVICE_MODE" = "systemd-user" ]; then
    if [ "$SERVICE_MODE" = "systemd-user" ]; then
      systemctl --user start "$SERVICE_UNIT" || return 1
      sleep 4
      if ! systemctl --user is-active --quiet "$SERVICE_UNIT"; then
        return 1
      fi
      NEW_PID=$(systemctl --user show "$SERVICE_UNIT" --property=MainPID --value 2>/dev/null || true)
    else
      systemctl start "$SERVICE_UNIT" || return 1
      sleep 4
      if ! systemctl is-active --quiet "$SERVICE_UNIT"; then
        return 1
      fi
      NEW_PID=$(systemctl show "$SERVICE_UNIT" --property=MainPID --value 2>/dev/null || true)
    fi
    case "$NEW_PID" in
      ''|*[!0-9]*|0) return 1 ;;
    esac
    rm -f "$ROOT/run.pid"
    return 0
  fi
  cd "$ROOT" || return 1
  nohup "./$APP" >> service.log 2>&1 </dev/null &
  NEW_PID=$!
  printf '%s\n' "$NEW_PID" > run.pid
  sleep 4
  kill -0 "$NEW_PID" 2>/dev/null
}

stop_service() {
  if [ "$SERVICE_MODE" = "systemd-user" ]; then
    log "停止 systemd user service: $SERVICE_UNIT"
    systemctl --user stop "$SERVICE_UNIT"
    return $?
  fi
  if [ "$SERVICE_MODE" = "systemd-system" ]; then
    log "停止 systemd system service: $SERVICE_UNIT"
    systemctl stop "$SERVICE_UNIT"
    return $?
  fi

  if kill -0 "$OLD_PID" 2>/dev/null; then
    kill -TERM "$OLD_PID" 2>/dev/null || true
    WAIT_COUNT=0
    while kill -0 "$OLD_PID" 2>/dev/null && [ "$WAIT_COUNT" -lt 30 ]; do
      sleep 1
      WAIT_COUNT=$((WAIT_COUNT + 1))
    done
    if kill -0 "$OLD_PID" 2>/dev/null; then
      log "舊程序未於期限內停止，強制結束"
      kill -KILL "$OLD_PID" 2>/dev/null || true
      sleep 1
    fi
  fi
}

restore_backup() {
  log "更新失敗，開始回滾"
  while IFS= read -r rel; do
    [ -n "$rel" ] || continue
    rm -f "$ROOT/$rel"
  done < "$TARGETS"
  if [ -d "$BACKUP/files" ]; then
    cp -Rp "$BACKUP/files/." "$ROOT/"
  fi
  if [ -f "$BACKUP/$APP" ]; then
    cp -p "$BACKUP/$APP" "$ROOT/$APP"
    chmod +x "$ROOT/$APP"
  fi
  if start_service; then
    log "已回滾並重新啟動舊版本"
  else
    log "回滾後仍無法啟動服務，請人工處理"
  fi
}

write_state applying || exit 1
log "更新程序已啟動，operation=$OPERATION_ID"

mkdir -p "$BACKUP/files"
while IFS= read -r rel; do
  [ -n "$rel" ] || continue
  if [ -f "$ROOT/$rel" ]; then
    mkdir -p "$BACKUP/files/$(dirname "$rel")"
    cp -p "$ROOT/$rel" "$BACKUP/files/$rel" || {
      log "備份失敗: $rel"
      exit 1
    }
  fi
done < "$TARGETS"
if [ -f "$ROOT/$APP" ]; then
  cp -p "$ROOT/$APP" "$BACKUP/$APP" || exit 1
fi

sleep 2
log "開始套用更新，服務模式=$SERVICE_MODE unit=${SERVICE_UNIT:-none} 舊程序 PID=$OLD_PID"
if ! stop_service; then
  log "停止既有服務失敗"
  start_service >/dev/null 2>&1 || true
  exit 1
fi

APPLY_FAILED=0
while IFS= read -r rel; do
  [ -n "$rel" ] || continue
  if [ ! -f "$STAGE/$rel" ]; then
    log "暫存檔案遺失: $rel"
    APPLY_FAILED=1
    break
  fi
  mkdir -p "$ROOT/$(dirname "$rel")" || { APPLY_FAILED=1; break; }
  cp -p "$STAGE/$rel" "$ROOT/$rel" || { APPLY_FAILED=1; break; }
  cmp -s "$STAGE/$rel" "$ROOT/$rel" || {
    log "更新後檔案驗證失敗: $rel"
    APPLY_FAILED=1
    break
  }
done < "$TARGETS"

if [ "$APPLY_FAILED" -eq 0 ]; then
  cp -p "$STAGE/$PLATFORM_BINARY" "$ROOT/$APP" || APPLY_FAILED=1
  if [ "$APPLY_FAILED" -eq 0 ] && ! cmp -s "$STAGE/$PLATFORM_BINARY" "$ROOT/$APP"; then
    log "更新後執行檔驗證失敗: $APP"
    APPLY_FAILED=1
  fi
  chmod +x "$ROOT/$APP" 2>/dev/null || APPLY_FAILED=1
fi

if [ "$APPLY_FAILED" -ne 0 ]; then
  restore_backup
  exit 1
fi

for executable in install.sh run.sh run.command run_bg.sh stop.sh; do
  [ -f "$ROOT/$executable" ] && chmod +x "$ROOT/$executable"
done

if ! start_service; then
  restore_backup
  exit 1
fi

log "更新完成，新程序 PID=$NEW_PID"
write_state completed || {
  log "無法寫入更新完成狀態"
  exit 1
}
UPDATE_COMPLETED=1
rm -rf "$OPERATION_DIR"
exit 0
`
