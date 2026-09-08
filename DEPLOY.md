# 部署手冊

本文件說明 `LoadBalanceProvider` 的部署與設定方式。

建置需要 Go 1.25.6 或相容的新版本。功能概覽請見 [README](README.md)，一般安裝與啟動請見 [安裝說明](install.md)。正式環境請啟用 HTTPS、限制管理入口，並將認證與資料目錄納入備份。

## 設定檔

服務啟動時會讀取專案根目錄的 `agent.properties`、`data/llm_proxy.json` 與 `data/advanced_settings.json`。`agent.properties` 保留服務基本設定，LLM Provider 與負載平衡設定集中放在 `data/llm_proxy.json`；進階路由、輸出比分級與低推理降級設定保存在 `data/advanced_settings.json`。

| 參數 | 說明 |
| :--- | :--- |
| `selection_strategy` | 負載平衡策略，目前預設 `random`，並保留 `weighted_score`。 |
| `retry_count` | 尚未轉送有效內容時允許的額外重試次數；容量備援也共用此上限，另受固定來源重試上限限制。文字或工具參數已送出後，不在原串流內重試；跨重連另有工具防重播限制。 |
| `providers[].id` | Provider 唯一識別碼。 |
| `providers[].base_url` | OpenAI-compatible Provider base URL。 |
| `providers[].api_key_env` | API key 的環境變數名稱。 |
| `providers[].chat_completions_path` | Chat Completions endpoint path。 |
| `providers[].enabled` | 是否啟用此 Provider。 |
| `providers[].weight` | 加權評分策略使用的權重；隨機策略不以此保證流量比例。 |
| `providers[].priority` | 優先序調整，數值越高會降低分數。 |
| `providers[].max_concurrent` | 同時處理 request 上限。 |
| `providers[].timeout_seconds` | 預設 `300` 秒；串流用於上游回應標頭等待與無進展逾時，而非整段串流的總時長。非串流請求仍受請求期限限制。 |
| `models[].max_input_tokens` | 模型可接受的最大輸入 token。 |
| `models[].max_output_tokens` | 模型可接受的最大輸出 token。 |
| `models[].capabilities` | 模型適合的任務類型，例如 `chat`、`reasoning`、`coding`、`summarization`。 |
| `models[].cost_tier` | 成本級距，數值越高代表越昂貴。 |
| `models[].quality_tier` | 品質級距，數值越高代表越適合高複雜度任務。 |

### 容量冷卻與 OAuth

可在管理介面的進階設定調整冷卻與等待。上游提供有效 `Retry-After` 時優先採用；暫時過載未提供提示時，採 2、4、8、16 秒加隨機偏移的退避。一般可重試伺服器錯誤預設冷卻 30 秒，模型故障與帳號配額故障分開處理。

| 進階設定欄位 | 預設 | 用途 |
| --- | --- | --- |
| `provider_server_error_cooldown_seconds` | 30 | 一般伺服器錯誤冷卻，可設 1–300 秒。 |
| `provider_retry_wait_seconds` | 30 | 首次排隊、失敗後退避各自的累計上限，可設 0–300 秒；0 不等待。預設兩階段合計最多 60 秒，不含上游執行時間。 |
| `persist_quota_cooldown` | true | 保存仍有至少 5 分鐘的配額冷卻，重啟時驗證後恢復。 |

一般故障等待原 Provider；容量失敗在未送出內容、沒有強制路由且歷史可恢復時，才允許改用其他帳號，並遵守選路輪數、每輪來源數與總嘗試上限。預算不包含上游生成耗時，也不是整段任務總時限。短暫故障冷卻在重啟後重新累積，長期配額冷卻可保存。詳見 [連線與重試](RETRY_POLICY.md)。

Codex OAuth 的背景用量更新改用帳號用量 API，不發送模型推論請求；一般回應標頭保留為備援。Codex Responses 恢復即時逐字串流，不等待整筆回應完成；等待期間包含心跳，反向代理需允許串流並關閉回應緩衝。部署前請確認 [串流連線與反向代理](#串流連線與反向代理) 設定。

Codex OAuth 的並發刷新依 token 儲存路徑與 Provider ID 序列化；同一程序共用 token 儲存鎖，避免不同 Provider 寫入同一檔案時互相覆蓋。HTTP `401` 最多觸發一次刷新後重送，不套用於 API key 認證，也不重播已開始輸出的串流。多個服務程序之間不共用此鎖，請勿讓多個實例同時寫入同一份 token 檔案。

### 串流連線與反向代理

- 關閉 SSE 回應緩衝與快取，並允許即時傳送小型資料區塊。服務會設定 `Cache-Control: no-cache` 與 `X-Accel-Buffering: no`，反向代理仍需確認未覆寫這些行為。
- 尚未取得有效上游內容的嘗試與冷卻等待期間，服務每 `3` 秒提供下游保活心跳；串流內也有保活處理。心跳不代表模型有進展，上游純心跳不會重設無進展逾時。
- 反向代理的讀取閒置期限應大於心跳間隔並預留網路延遲；若平台另有總請求時長限制，心跳無法解除該限制。
- 用戶端取消或下游寫入失敗時會停止等待，不應透過無限制增加逾時來掩蓋問題。上游 Responses 故障維持失敗語意；代理跨重連額度用盡或工具防重播封鎖，則以完成型助理通知交付等待或確認工具結果的提示，不代表上游生成成功。請區分上游拒絕、過載、等待標頭逾時與真正的網路斷線。
- 同一條串流的內部重送僅限尚未轉交有效內容的階段。跨重連時，僅文字、推理或未完成工具參數不觸發工具防重播封鎖；已交付完整工具呼叫則以助理通知結束，不重送上游。這不保證上游未產生計費或其他副作用。

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
   go mod download
   go build -buildvcs=false ./...
   ```

4. 編譯：

   ```bash
   go build -buildvcs=false -o LoadBalanceProvider ./src/cmd/loadbalanceprovider
   ```

5. 啟動：

   ```bash
   ./LoadBalanceProvider
   ```

### 配對資料升級

新版在進階設定檔所在目錄建立 `provider_routes.db`，使用內嵌 bbolt 交易資料庫，無須另建資料庫服務。首次使用時匯入 `turn_provider_bindings.json` 中仍有效的紀錄，保留原檔不覆寫；之後的新配對只寫入資料庫。回合與回應識別分別保存，接近上限會留下容量警告。

升級前停止寫入並備份完整 `data/` 與設定目錄，確認服務帳號對該目錄有寫入權限。管理頁面的 ZIP 更新持續保護整個 `data/`。不要讓不同服務實例共用同一份可寫資料。

舊版不能讀取新資料庫；降版應還原與該版本相配的完整備份，升級後新增的配對不會出現在舊 JSON 中。資料庫損毀或綁定衝突時會拒絕未驗證的跨帳號接續，不會自動丟棄紀錄或隨機重配。

## 發布與升級

- macOS 正式 Release 僅附上已完成 Developer ID 簽章與公證的 DMG。
- Windows 正式 Release 僅附上已驗證簽章的 MSI；不發布 `-unsigned` 安裝檔。
- Linux ARM64 與 x86_64 發布 ZIP，不要求程式簽章；提供 SHA-256 供下載後驗證完整性。
- `--local` 產物與未簽章的 macOS／Windows 程式不作為正式發行檔。發布說明使用英文。
- `build.sh` 目前產出包含 macOS／Linux 三平台執行檔的部署 ZIP；正式 Linux 發行需分別整理各架構的 ZIP，不將此混合部署包當成已簽章的 macOS 發行檔。

部署 ZIP 可從管理頁面「系統更新」升級。支援版本會在更新前保存路由快照，更新後恢復仍有效的 Provider 配對。更新會重新啟動服務，不保留進行中的連線；更新前請備份 `agent.properties`、`data/`、`usage/` 與自訂資料路徑。勿讓多個程序共用同一份可寫認證或配對檔。

DMG／MSI 請使用對應的新版安裝包升級。詳細限制見 [對話配對與更新恢復](TURN_BINDING.md)。

## 維運端點

```http
GET /api/health
GET /api/providers
GET /api/api-keys/density?window=15m
```

`/api/providers` 可查看目前各 Provider 的 active request、成功次數、失敗次數與模型設定摘要。

`/api/api-keys/density` 僅供 Web 管理登入使用，可查看近期請求密度、Token 與輸出結構；一般 API 金鑰與 MCP 金鑰不能存取。最近逐筆樣本在服務重啟後重新累積，月次統計另行保存。
