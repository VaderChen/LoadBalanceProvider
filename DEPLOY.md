# 部署手冊

本文件說明 `LoadBalanceProvider` 的部署與設定方式。

## 設定檔

服務啟動時會讀取專案根目錄的 `agent.properties`、`data/llm_proxy.json` 與 `data/advanced_settings.json`。`agent.properties` 保留服務基本設定，LLM Provider 與負載平衡設定集中放在 `data/llm_proxy.json`；進階路由、輸出比分級與低推理降級設定保存在 `data/advanced_settings.json`。

| 參數 | 說明 |
| :--- | :--- |
| `selection_strategy` | 負載平衡策略，目前預設 `random`，並保留 `weighted_score`。 |
| `retry_count` | 尚未轉送有效內容時，遇到可重試故障可重新選路由的次數；文字或工具呼叫已送出後不再重播。 |
| `providers[].id` | Provider 唯一識別碼。 |
| `providers[].base_url` | OpenAI-compatible Provider base URL。 |
| `providers[].api_key_env` | API key 的環境變數名稱。 |
| `providers[].chat_completions_path` | Chat Completions endpoint path。 |
| `providers[].enabled` | 是否啟用此 Provider。 |
| `providers[].weight` | 權重，數值越高越容易被選中。 |
| `providers[].priority` | 優先序調整，數值越高會降低分數。 |
| `providers[].max_concurrent` | 同時處理 request 上限。 |
| `providers[].timeout_seconds` | 預設 `300` 秒；串流用於上游回應標頭等待與無進展逾時，而非整段串流的總時長。非串流請求仍受請求期限限制。 |
| `models[].max_input_tokens` | 模型可接受的最大輸入 token。 |
| `models[].max_output_tokens` | 模型可接受的最大輸出 token。 |
| `models[].capabilities` | 模型適合的任務類型，例如 `chat`、`reasoning`、`coding`、`summarization`。 |
| `models[].cost_tier` | 成本級距，數值越高代表越昂貴。 |
| `models[].quality_tier` | 品質級距，數值越高代表越適合高複雜度任務。 |

### 容量冷卻與 OAuth

`data/advanced_settings.json` 的 `provider_capacity_cooldown_seconds` 為預設容量冷卻時間，預設 `10` 秒，可在管理介面的進階設定調整。上游回報有效 `Retry-After` 時優先採用該值。模型故障與帳號配額故障分開冷卻，同一有效窗口內的並發失敗不會重設冷卻期限，較早開始的請求成功也不會清除新的容量冷卻。

串流請求可在候選都處於容量冷卻時等待恢復，每個請求累計最多 `30` 秒；等待時間超過剩餘預算便回傳終止錯誤，不會無限保留請求。這是程式內建的冷卻等待預算，不是可編輯設定，也不是整段任務總時限。冷卻狀態僅保存在目前服務程序記憶體，重啟後重新累積。

Codex OAuth 的並發刷新依 token 儲存路徑與 Provider ID 序列化；同一程序共用 token 儲存鎖，避免不同 Provider 寫入同一檔案時互相覆蓋。HTTP `401` 最多觸發一次刷新後重送，不套用於 API key 認證，也不重播已開始輸出的串流。多個服務程序之間不共用此鎖，請勿讓多個實例同時寫入同一份 token 檔案。

### 串流連線與反向代理

- 關閉 SSE 回應緩衝與快取，並允許即時傳送小型資料區塊。服務會設定 `Cache-Control: no-cache` 與 `X-Accel-Buffering: no`，反向代理仍需確認未覆寫這些行為。
- 尚未取得有效上游內容的嘗試與冷卻等待期間，服務每 `3` 秒提供下游保活心跳；串流內也有保活處理。心跳不代表模型有進展，上游純心跳不會重設無進展逾時。
- 反向代理的讀取閒置期限應大於心跳間隔並預留網路延遲；若平台另有總請求時長限制，心跳無法解除該限制。
- 用戶端取消或下游寫入失敗時會停止等待，不應透過無限制增加逾時來掩蓋問題。Responses 故障以 `response.failed` 帶出原因，請區分上游拒絕、過載、等待標頭逾時與真正的網路斷線。
- 重送僅限尚未轉交文字或工具呼叫的階段；此限制避免下游重複執行工具，但不保證上游未產生計費或其他副作用。

### 低推理降級

管理介面的「設定 > 進階 > 低推理降級」使用每支 API 金鑰最近 `15` 分鐘的密度與完成輸出進行評估。預設關閉；預設條件為跨啟用 Provider 的當日平均配額消耗達 `18%`、金鑰頻率 `≥8 req/min`、推理比 `<10%`，且至少有 `5` 筆上游回報推理量的完成樣本。成立後套用品質等級上限 `4`，預設維持 `10` 分鐘。

需注意：這個機制設定的是候選模型品質上限，不會覆寫明確指定模型或金鑰強制 Provider。若沒有符合上限的候選，負載平衡器會 fail-open 使用較高等級模型。降級狀態只保存在記憶體，設定更新或服務重啟會清除。

## 部署步驟

### DMG／MSI 安裝包

在 macOS 執行 `./pack.command`，會另外建置 macOS Apple Silicon 的 DMG 與 Windows x64 的 MSI；既有 `build.command`／`build.sh` 仍負責 macOS／Linux 部署 ZIP，兩種流程互不取代。安裝包由目前原始碼重新編譯，不混用 `bin/` 中可能過期的執行檔，也不自動執行 Git、發布 Release 或安裝到本機。

必要工具為 Go、Xcode Command Line Tools、`hdiutil`、`codesign`、`xcrun`、`shasum`，以及 msitools 的 `wixl`；Windows ARM64 另需 `msibuild`。正式 DMG 需要 Developer ID Application 簽章身分與有效的 notarytool Keychain Profile。

```bash
./pack.command

# 僅建立本地驗證用版本，不送出 Apple 公證
./pack.command --local

# 沿用 dist 中最新安裝版的執行檔及資源，重新封裝
./pack.command --no-build

# 指定版本與目標架構
LBP_PACKAGE_VERSION='1.26.0906 build 1200' \
LBP_BUILD_TARGETS='darwin/arm64,windows/amd64,windows/arm64' \
./pack.command
```

| 環境變數 | 用途 |
| :--- | :--- |
| `LBP_PACKAGE_VERSION` | 顯示版本，格式為 `1.YY.MMDD build HHmm`；預設使用台北時間。 |
| `LBP_BUILD_TARGETS` | 預設 `darwin/arm64,windows/amd64`；支援兩個 OS 的 `arm64`／`amd64`，且須同時包含 macOS 與 Windows。 |
| `LBP_CODESIGN_IDENTITY` | 指定 Developer ID Application；未設定時尋找本機可用身分。 |
| `LBP_NOTARY_PROFILE` | notarytool Keychain Profile 名稱，預設 `VaderApp`。不在腳本中保存 Apple ID 或密碼。 |
| `LBP_WIXL` | 自訂 `wixl` 執行檔路徑。 |
| `LBP_MSI_SIGN_COMMAND` | 可選的 MSI 簽章程式路徑，第一個參數為待簽章 MSI；須原地產出簽章檔，並以 `osslsigncode verify` 驗證成功後才列為已簽章。 |

輸出位於 `dist/1.YY.MMDD-build-HHmm/` 的平台子目錄。已存在的版本不會直接覆蓋，重新封裝請使用 `--no-build`；可搭配 `LBP_PACKAGE_VERSION` 選擇特定版本。只有全部選定平台封裝完成後，才產生本輪的 `PACKAGES-SHA256SUMS` 與 `SIGNING_STATUS.txt`。前者是完整性檢查碼，不等同程式簽章；後者明確記錄每個安裝包的簽章狀態。

正式 DMG 依序簽署服務執行檔與 App、對 App 公證並附加票根，再建立、簽署及公證 DMG。`--local` 產物檔名含 `-local`，只使用 adhoc 簽章，未公證，不可當作正式發行檔。未設定 Windows 簽章程式時，MSI 檔名含 `-unsigned`，Windows 可能顯示未知發行者；不會因雜湊驗證成功就宣稱通過 SmartScreen。

MSI 包含開始功能表捷徑與升級／移除資訊。版本排序包含日期及當日分鐘，避免同一天的新版本被視為相同版本；同版本重新封裝保留相同產品識別碼。安裝前請停止服務。安裝與移除不清除使用者資料，詳見 [安裝說明](install.md#dmgmsi-安裝版)。

### ZIP／原始碼部署

1. 設定 Provider API key：

   ```bash
   export OPENAI_API_KEY="..."
   ```

2. 編輯 `data/llm_proxy.json`，將要使用的 Provider `enabled` 改為 `true`。

3. 整理依賴並確認語法：

   ```bash
   go mod tidy
   go test ./...
   ```

4. 編譯：

   ```bash
   go build -o LoadBalanceProvider ./src/cmd/loadbalanceprovider
   ```

5. 啟動：

   ```bash
   ./LoadBalanceProvider
   ```

## 維運端點

```http
GET /api/health
GET /api/providers
GET /api/api-keys/density?window=15m
```

`/api/providers` 可查看目前各 Provider 的 active request、成功次數、失敗次數與模型設定摘要。

`/api/api-keys/density` 是管理端即時監看端點，需使用有效的 Web 登入 Session；一般 API 金鑰與 MCP 金鑰不能存取。`window` 可使用秒數或 `1m`、`5m`、`15m`、`30m`、`1h`，最大觀察範圍為一小時。回應除了請求頻率與複雜度外，也包含 `prompt_tokens`、`quality_tier_avg`、輸出比 `output_ratio`／`output_ratio_median`、正文比 `prose_ratio`／`prose_ratio_median`／`prose_samples`、推理量 `reasoning_tokens`／`reasoning_ratio`、工具呼叫與輪次、續接與重複任務，以及 `yield_low`／`yield_mid`／`yield_high` 分布和目前套用的 `yield_thresholds`。輸出比以實際完成輸出 Token ÷ 估算輸入 Token 計算，預設分級門檻為 `≤2%`、`>2% 且 ≤20%`、`>20%`，可從管理介面的進階設定調整。舊版 `os_tool_ratio`、`tool_type_counts` 與 OS 工具分類已移除。最近逐筆樣本僅保存在記憶體，服務重啟後會重新累積；月次永久統計不受影響。
