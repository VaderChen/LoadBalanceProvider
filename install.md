# LoadBalanceProvider 安裝說明

## DMG／MSI 安裝版

安裝包是既有 Web 管理服務的桌面啟動方式，不另建原生管理視窗，也不自動註冊開機服務。

- **macOS**：開啟 DMG，將 `LoadBalanceProvider.app` 拖到「應用程式」。啟動 App 後會開啟 Terminal 並執行服務。
- **Windows**：執行符合 CPU 架構的 MSI，安裝至 Program Files，需要系統管理員權限。完成後由開始功能表啟動 `LoadBalanceProvider`，服務在主控台視窗執行。
- 啟動後以瀏覽器開啟設定的 HTTP 位址，首次安裝預設為 `http://localhost:10081`。停止服務請在服務視窗按 `Ctrl+C`；更新安裝前須先停止服務。
- 安裝包不包含開發機的 `agent.properties`、Provider／金鑰資料、歷史紀錄或 `cmd/` 設定腳本。

設定與用量資料位於使用者資料目錄，不寫入唯讀 App 或 Program Files：

| 平台 | 資料目錄 |
| :--- | :--- |
| macOS | `~/Library/Application Support/LoadBalanceProvider/` |
| Windows | `%LOCALAPPDATA%\LoadBalanceProvider\` |

安裝版使用 `--desktop` 啟動模式，每次啟動同步封裝內的 Web 資源、題庫及版本資訊，並保留已存在的 `agent.properties`、`data/` 與 `usage/`。同一使用者資料目錄以檔案鎖避免重複啟動。原本 ZIP 部署的資料不會自動搬入；若要沿用，請先停止兩邊服務，再自行備份並移入上述目錄。

安裝版不支援管理介面的 ZIP 自我更新，請使用新版 DMG／MSI 升級。移除 App 或解除安裝 MSI 不會刪除使用者設定與統計。正式 DMG 含 Developer ID 簽章與公證；檔名含 `-local` 的 DMG 僅供本地使用，含 `-unsigned` 的 MSI 尚未簽章。簽章狀態請以同版本的 `SIGNING_STATUS.txt` 為準。

以下章節說明原有部署 ZIP 的安裝方式；建立安裝包請參考 [部署手冊](DEPLOY.md#dmgmsi-安裝包)。

## 封裝內容

部署 zip 內會包含三個平台的執行檔：

- `bin/LoadBalanceProvider_mac_arm64`：macOS Apple Silicon。
- `bin/LoadBalanceProvider_linux_x64`：Linux x86_64 / amd64。
- `bin/LoadBalanceProvider_linux_arm64`：Linux arm64 / aarch64。

部署 zip 不包含 `cmd/` 下的 Codex App／CLI 設定腳本。相關腳本僅在說明文件以範例名稱與範例下載網址呈現，應由實際管理的發佈管道另行提供。

正式執行檔不直接放在根目錄，需透過 `install.sh` 依照目前 OS 與 CPU 架構複製。

## 安裝與啟動

解壓縮部署檔後，進入目錄執行：

```bash
./install.sh
```

`install.sh` 會自動偵測平台，將符合的平台執行檔複製為：

```text
./LoadBalanceProvider
```

接著會直接執行 `./LoadBalanceProvider`。

也可以執行：

```bash
./run.sh
```

若要在背景執行：

```bash
./run_bg.sh
```

`run_bg.sh` 會自動安裝符合平台的執行檔、建立缺少的 `agent.properties`、停止既有程序，並將輸出寫入：

```text
service.log
```

若要停止背景服務：

```bash
./stop.sh
```

macOS 桌面環境可直接執行：

```bash
./run.command
```

## 設定檔

封裝內只包含：

```text
agent.sample.properties
```

第一次啟動時，如果目錄中不存在 `agent.properties`，系統會自動由 `agent.sample.properties` 複製建立：

```text
agent.sample.properties -> agent.properties
```

部署後請依實際環境修改 `agent.properties`，例如雲端服務連線資訊、HTTP port、HTTPS port 與 web path。

## 支援平台

目前 `install.sh` 支援：

- macOS arm64
- Linux x86_64 / amd64
- Linux arm64 / aarch64

其他平台會停止並顯示不支援訊息。若要支援其他架構，需要在 `build.sh` 增加對應的 `GOOS/GOARCH` 編譯目標，並同步更新 `install.sh` 的平台判斷。

## Codex App 用戶端設定

以下僅為 Codex App／CLI 設定工具的範例檔名，不代表部署 ZIP 內含這些檔案：

- macOS：`marsCodexApp.sh`
- Windows：`marsCodexApp.bat`
- Ubuntu CLI／桌面環境：`marsCodexApp_Linux_CLI.sh`
- VS Code SSH Remote：`marsCodexApp_Linux_VSC_Remote.sh`

若從下載站取得腳本，文件中的網址一律是範例，請替換成實際的內部發佈位置：

```bash
curl -fsSL https://example.com/downloads/marsCodexApp.sh -o marsCodexApp.sh
chmod +x marsCodexApp.sh
./marsCodexApp.sh
```

```powershell
Invoke-WebRequest https://example.com/downloads/marsCodexApp.bat -OutFile marsCodexApp.bat
.\marsCodexApp.bat
```

`example.com` 不是真實腳本位置，部署文件不會揭露正式下載網址。

這些工具會偵測目前使用者的 Codex 設定目錄，不使用寫死的帳號或家目錄。套用代理服務來源時會：

- 寫入使用者環境變數 `MARS_API_KEY`。
- 下載完整 Codex model catalog。
- 設定代理服務的 Responses Provider。
- 啟用 Codex 影像生成功能與代理服務的 MCP `image_gen` 工具。
- 合併現有 `config.toml`，保留 `[projects."..."]` 與其他非工具管理的設定。

工具仍會建立 `config.toml.mars-llm-proxy.bak` 作為緊急備份，但正常「恢復 Codex 原始設定」不會使用該檔案覆蓋。恢復流程只會移除工具管理內容、model catalog 與 `MARS_API_KEY`，並還原套用前的作用中 Provider；因此目前的專案信任及其他個人設定會保留。

若 `config.toml` 已經定義其他 Provider 或 Profile，這些區段不會被移除。工具會把原本的頂層 `model`、`model_provider`、`model_catalog_json`、`profile` 保存至 `config.toml.mars-llm-proxy.defaults`，再將 model／Provider／catalog 切換到代理服務來源，並暫時清除可能衝突的作用中 profile。恢復時會把原值寫回，因此可以和既有 Provider 共存，也能回到套用前的預設來源。

若備份檔已存在，可選擇覆蓋或保留；兩種選擇都不會中止後續套用。操作完成後必須完整重啟 Codex App、CLI 或 VS Code Extension Host。
