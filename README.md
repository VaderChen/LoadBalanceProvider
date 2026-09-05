# Load Balance Provider

**繁體中文** | [English](README.en.md) | [日本語](README.ja.md) | [한국어](README.ko.md)

`LoadBalanceProvider` 是一個 LLM Proxy 服務。服務提供 OpenAI Chat Completions 與 Responses 相容 API，並依照請求內容大小、工作量、工作性質與 Provider 即時負載，選擇合適的後端 LLM Provider 處理。

## 功能目標

- **OpenAI 相容入口**：支援 `POST /v1/chat/completions` 與 `POST /v1/responses`，保留既有 OpenAI SDK 或相容 Client 的接入方式。
- **長任務 Prompt Cache 黏著**：以 `previous_response_id` 或 `prompt_cache_key` 將同一段 Responses 對話導回原 Provider/Model，避免多輪工具呼叫與加密推理內容因切換帳號而失效。
- **多 Provider 管理**：透過 `data/llm_proxy.json` 登錄多個 OpenAI-compatible Provider、模型能力、權重、成本與併發上限。
- **智慧路由**：依照輸入 token 估算、輸出需求、訊息數、任務類型與模型能力進行分數式選擇。
- **一般與串流支援**：非 streaming 以一般 HTTP response 轉回，`stream=true` 時維持 SSE/Chunked 轉送。
- **負載平衡**：以 provider 權重、模型品質、成本、目前 active request 與 max concurrent 做通用評分。
- **請求密度與輸出結構監看**：依 API 金鑰統計近期請求頻率、Token 消耗、模型等級、輸入／輸出比、正文比例與推理比例，協助辨識大量低輸出請求或高階模型使用不當的帳號。
- **標準 MCP**：提供 MCP `2025-11-25` Streamable HTTP 端點，將金鑰管理以外的查詢與操作公開為工具。

## 目錄結構

- `src/cmd/loadbalanceprovider/main.go`：服務進入點，初始化服務框架、HTTP API 與 LLM Proxy 元件。
- `src/service/cloud_service.go`：服務生命週期與背景 Provider 狀態記錄。
- `src/api/http_api.go`：REST API 路由，包含 `/v1/chat/completions`、`/v1/responses`、`/api/health`、`/api/providers`。
- `src/api/mcp.go`：標準 MCP Streamable HTTP、JSON-RPC 生命週期、工具目錄與既有 API 轉接。
- `src/domain/types.go`：Chat Completion、Provider、Model、錯誤格式等共用型別。
- `src/config/provider_config.go`：讀取 `data/llm_proxy.json` 的 LLM Proxy 設定並套用預設值。
- `src/analyzer/request_analyzer.go`：估算請求大小、輸出工作量、任務類型與複雜度。
- `src/balancer/load_balancer.go`：候選 Provider/Model 過濾、評分與即時負載統計。
- `src/keyusage/recorder.go`：API 金鑰每月用量，以及最近一小時的請求密度與 Token 消耗統計。
- `src/telemetry/request.go`：從 Chat／Responses 請求抽取工具呼叫、工具輪次、工具輸出量、續接與重複任務訊號。
- `src/proxy/client.go`：OpenAI-compatible Chat Completion HTTP 轉發與串流代理。
- `agent.properties`：服務基本設定。
- `data/llm_proxy.json`：LLM Provider、模型能力與負載平衡設定。

## API

### 儀表板快照

管理頁面使用 `GET /api/dashboard`，僅供 Web 管理登入存取。首屏回傳必要的 Provider 資訊、記憶體中的執行狀態與剩餘量快照，不等待 OAuth 更新或歷史檔案掃描，也不傳送完整模型 catalog、金鑰欄位及 Provider 編輯設定。

帳號資訊以一次本地批次讀取取得，不觸發 token 更新；統計基準與歷史累計由同一服務實例共用的背景工作補齊，每次更新完成後快取 60 秒，多個使用者不會各自啟動重複工作。OAuth 更新與上游用量查詢沿用既有背景刷新及實際請求流程。歷史掃描採獨立鎖，不在掃描期間持有請求紀錄寫入鎖。

回應包含 `updatedAt`、`historyUpdatedAt`、`accountsUpdatedAt`、`refreshing`、`historyReady`、`accountsReady`、`baselinesReady` 與 `refreshError`。前端先顯示快照，背景更新期間逐步補取；未就緒的累計統計、基準相關指標或缺少的配額顯示待更新狀態。只有統計與基準就緒後才允許歸零。歸零成功會立即更新後端基準快取。

一般刷新及 TAB 快取為 60 秒，背景補取／失敗重試採 1、2、4 秒逐步增加至最多 30 秒的間隔。離開儀表板或瀏覽器分頁隱藏時暫停補取，更新失敗保留已有畫面；頁面顯示快照及歷史統計的更新時間。每日用量圖表仍在開啟對話框時才讀取。

### Chat Completions

```http
POST /v1/chat/completions
Content-Type: application/json
Authorization: Bearer <client-token>
```

請求格式維持 OpenAI Chat Completions 相容。服務會解析 `model`、`messages`、`stream`、`max_tokens` 或 `max_completion_tokens`，選擇後端模型後轉發。

```json
{
  "model": "auto",
  "messages": [
    {"role": "user", "content": "請幫我規劃一個高可用架構"}
  ],
  "stream": false
}
```

### Responses API

```http
POST /v1/responses
Content-Type: application/json
Authorization: Bearer <client-token>
```

請求格式維持 OpenAI Responses API 相容，支援一般回應與 `stream=true` 的 SSE 串流轉送。`model` 可指定實際模型或使用 `AUTO`，由服務依 Provider 能力與即時負載選擇後端。

```json
{
  "model": "AUTO",
  "input": "請分析此專案並提出重構計畫",
  "stream": true,
  "prompt_cache_key": "project-refactor-2026"
}
```

Responses API 亦支援既有 response 的查詢、刪除、取消、輸入項目與 input token 計算等相容子路由；服務會將請求轉送至建立該 response 的 Provider，並以呼叫端 API Key 隔離路由資料。

#### 串流連線與故障恢復

- 上游尚未送出有效內容時，服務暫存初始化事件與待轉送請求；遇到可重試故障可重新選擇 Provider，期間維持同一條下游連線並傳送心跳。失敗嘗試的初始化事件不會混入成功回應。
- 一旦文字或工具呼叫已轉送至下游，就不再切換 Provider 或重播該請求，避免重複執行。Responses 串流失敗使用 `response.failed` 結束並帶出原因，不將失敗偽裝成成功完成。
- 請求格式、上下文長度與政策拒絕等請求本身的錯誤不會觸發容量冷卻；模型不存在或明確的模型過載只冷卻該 Provider 的該模型，帳號配額與限流則冷卻整個 Provider。有效的 `Retry-After` 優先於預設冷卻秒數，同一個有效冷卻窗口不會因並發失敗持續延長。
- 候選 Provider 都在容量冷卻且可在剩餘等待預算內恢復時，串流請求會保持連線等待再選路由。每個請求累計冷卻等待上限為 `30` 秒，不因切換 Provider 重設；此上限不包含上游執行時間，實際請求重試仍受 `retry_count` 限制。
- Codex OAuth 依 Provider 身分合併並發 token 刷新；遇到 HTTP `401` 時，OAuth 請求最多刷新並重送一次。若其他請求已完成刷新，直接使用新 token；API key 認證不套用 OAuth 刷新。
- Responses 完成事件若缺少 `output` 或項目 ID，會以同一串流已收到的 `response.output_item.done` 完整項目補齊，不覆蓋既有內容，也不以未完成的 delta 組合工具參數。

這些處理可降低上游暫時故障造成的重新連線，但無法保證跨反向代理、用戶端或網路中斷後仍維持連線。等待回應標頭與串流無進展逾時由 Provider 的 `timeout_seconds` 控制；上游純心跳不會延長無進展期限。部署時亦需確認反向代理未緩衝 SSE，詳見 [部署手冊](DEPLOY.md#串流連線與反向代理)。

#### 長任務 Prompt Cache 黏著

Responses 長任務通常包含多輪推理、工具呼叫或加密 reasoning 內容，過程中若切換 Provider 或帳號，上游可能無法識別先前狀態。服務會自動建立對話黏著關係：

1. 回應建立成功後，記錄 response ID、`prompt_cache_key`、Provider、Model 與呼叫端身分的對應。
2. 後續請求帶有 `previous_response_id` 時，優先導回原 Provider/Model。
3. Codex 等 Client 未送出 `previous_response_id`、但沿用相同 `prompt_cache_key` 時，仍會導回原 Provider/Model，適合長時間 Agent 任務與多輪工具呼叫。
4. 黏著資料依 API Key 隔離，不同呼叫端無法共用其他使用者的 response 或 Prompt Cache 路由。
5. Provider 不可用、配額顯著低於其他 Provider、黏著逾時或路由被淘汰時，服務會移除無法跨 Provider 使用的 `previous_response_id` 與加密 reasoning，再重新執行負載平衡，避免整個任務直接失敗。

管理介面的「設定 > 進階」可調整以下參數：

- **對話黏著 TTL**：預設 `30` 分鐘，可設定 `1` 至 `10080` 分鐘；長任務應依最長步驟間隔適度提高。
- **黏著配額容忍值**：預設 `10` 個百分點；原 Provider 配額低於同儕平均超過此值時，允許解除黏著並重新選擇。
- **Response 路由上限**：預設 `2000` 筆，可設定 `100` 至 `100000` 筆；超過上限時會淘汰較舊的路由。

#### 每金鑰低推理降級

「設定 > 進階 > 低推理降級」可在整體 Provider 配額開始消耗後，暫時限制高頻、低推理 API 金鑰可選模型的品質等級。功能預設關閉，設定保存在 `data/advanced_settings.json`。

- 啟動閘門採所有啟用且有觀測資料的 Provider 當日配額消耗平均值，預設達 `18%` 才開始評估；這不是單一 API 金鑰的當日用量。門檻設為 `0` 可停用閘門，尚無配額資料時不會啟動。
- 每支 API 金鑰使用最近 `15` 分鐘的滾動窗口。預設須同時滿足 `≥8 req/min`、推理 Token 佔實際輸出 `<10%`，且至少有 `5` 筆上游確實回報推理量的完成樣本。
- 符合條件後，預設將模型品質等級上限設為 `4`、維持 `10` 分鐘；到期後解除並重新觀察。設定變更或服務重啟會清除記憶體中的降級狀態。
- 降級目前是候選模型的品質上限，不會改寫呼叫端明確指定的模型或金鑰強制路由。若沒有符合上限的候選，負載平衡器會 fail-open 選用較高等級模型，避免回傳無可用 Provider。

若要讓同一個長任務穩定命中既有 Prompt Cache，Client 應在整段任務期間持續使用相同的 `prompt_cache_key`，不同任務則使用不同且穩定的識別值。

### 健康檢查

```http
GET /api/health
GET /api/providers
```

### 請求密度監看

管理介面的「即時監看」頁面會依 API 金鑰顯示近期使用情形，包括總請求、每分鐘請求、完成請求的 Token 總數、每請求平均 Token、實際使用模型的平均品質等級、輸出比、正文比、推理比，以及低／中／高輸出與需求複雜度分布。API 另提供工具呼叫、工具輪次、工具輸出量、續接比例與重複任務比例，供後續異常偵測及模型降級策略使用。頁面每 `60` 秒背景更新；觀察視窗、帳號狀態與顯示欄位會保存在目前瀏覽器。

```http
GET /api/api-keys/density?window=15m
```

- `window` 接受秒數或 Go duration，例如 `60`、`5m`、`1h`；預設 `5m`，上限 `1h`。
- **輸出比**為實際完成輸出 Token ÷ 估算輸入 Token。輸出 Token 包含上游回報的正文、推理與工具呼叫參數；輸入量未知的完成請求不列入輸出比分級。預設 `≤2%` 為低輸出、`>2%` 且 `≤20%` 為中輸出、`>20%` 為高輸出；管理員可在「設定 > 進階 > 輸出比分級門檻」調整兩個分界，儲存後即時監看會立即套用。
- 介面的輸出比主值採用逐筆比值的中位數 `output_ratio_median`，避免單一超大 Context 主導結果；副值「總和」為視窗內總輸出 Token ÷ 總估算輸入 Token，即 `output_ratio`。
- **正文比**為可辨識的使用者可見文字估算 Token ÷ 該請求實際完成輸出 Token。介面主值使用逐筆中位數 `prose_ratio_median`，副值使用總和比例 `prose_ratio`。只有能從回應事件或輸出項目拆出用途的完成請求才納入，讀取端必須同時檢查 `prose_samples`；沒有樣本不等於正文比為 `0%`。
- **推理比** `reasoning_ratio` 為上游回報的推理 Token `reasoning_tokens` ÷ 實際完成輸出 Token。推理 Token 已包含在完成輸出量內，此欄位是輸出結構的拆分，不可再次加到總輸出 Token。
- 模型等級是完成請求實際使用模型之 `quality_tier` 加權平均；將高模型等級與極低輸出比並列，可協助找出以高階模型重複執行低輸出工作的帳號。
- 工具行為只保留可直接驗證的聚合數據：`tool_call_count`、`tool_calls_per_request`、`tool_round_count`、`tool_rounds_per_request` 與 `tool_output_tokens`。舊版 `os_tool_ratio`、`tool_type_counts` 與 OS 工具分類已移除，不應再由名稱推測工具用途。
- 重複任務以最後一筆 User 文字正規化後的 SHA-256 指紋判斷；不保存原始提示詞。輸出比分級維持純粹的 Token 比例，不會因工具操作而改變，降級策略應組合輸出結構、模型等級、工具輪次、續接與重複任務等多項指標判斷。
- 複雜度分數 `1–3` 為低需求、`4–6` 為中需求、`7–10` 為高需求；無有效分數時歸入低需求。
- 請求頻率在請求完成分類後記錄；Token 消耗只統計已完成並取得實際用量的請求，因此兩者母體可能不同。
- 最近一小時的逐筆樣本與任務指紋只保存在記憶體，服務重新啟動後會重新累積；每月請求與活動彙總仍會連同既有統計保存在 `usage/<key>/YYYY-MM.json`。
- 回應會列出無流量、停用及視窗內已刪除的金鑰，但只提供名稱與聚合數據，不回傳 Key ID、前綴或遮罩金鑰。
- 此端點僅供 Web 管理登入使用；一般 API 金鑰與 MCP 金鑰不能存取。

## MCP

服務提供單一 Streamable HTTP 端點：

```text
http://<host>:<port>/mcp/
```

MCP 預設啟用，協定版本為 `2025-11-25`，支援 `initialize`、`notifications/initialized`、`ping`、`tools/list` 與 `tools/call`。端點不維持伺服器主動 SSE，因此 `GET /mcp/` 依規格回傳 `405 Method Not Allowed`；每個 JSON-RPC 訊息使用獨立的 HTTP `POST`。

連線可使用管理介面「金鑰管理」核發的 API 金鑰或 MCP 專用金鑰驗證：

```http
POST /mcp/ HTTP/1.1
Authorization: Bearer <api-key>
Content-Type: application/json
Accept: application/json, text/event-stream

{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"example","version":"1.0.0"}}}
```

API Key 可呼叫一般 REST API 與 MCP；Web 登入暫時金鑰不能呼叫 MCP。MCP 金鑰只能呼叫 MCP，不能呼叫 Chat、Responses 或其他一般 REST API，亦不收集累計或每月使用統計。

管理介面的「設定 > MCP」可調整：

- 啟用或停用 MCP。
- 唯讀模式；開啟時，`tools/list` 只提供不改變狀態的查詢工具。
- 額外允許的瀏覽器 Origin；未帶 `Origin` 的原生 MCP Client 與同源瀏覽器請求不需加入清單。
- 檢視實際端點、協定版本與目前公開工具。

工具以既有 REST handler 為唯一執行來源，涵蓋服務狀態、模型、Provider、儀表板與用量、一般／進階／通知／MCP 設定、基準測試、系統監控、系統更新、Chat Completions、Responses 與多模態代理。API Key 的列出、核發、修改、啟停、刪除、路由綁定與用量查詢固定不會透過 MCP 公開。

## Provider 設定

Provider 以 `data/llm_proxy.json` 的 `providers` 設定。`enabled` 預設範例為 `false`，正式使用前需改為 `true` 並設定對應 API key 環境變數。
Provider 與通知目標 URL 只允許 `http`/`https`，並阻擋 link-local、unspecified、multicast 等位址；private CIDR 與 localhost/loopback 暫時允許供內網模型服務使用。

Codex OAuth 的模型清單由上游動態取得，上游會依 `client_version` 決定可見模型。管理介面的模型查詢預設宣告 Codex `0.153.0`；日後可在服務啟動環境設定 `MARS_CODEX_MODELS_CLIENT_VERSION` 為已確認可用的新版 Codex 版本，重新啟動服務後重新整理模型清單。查詢的 `Version` 與預設 `User-Agent` 會同步使用此版本；若 Codex 呼叫端已提供 `client_version` 或 `Version`，仍優先保留呼叫端版本。實際可用模型由該 Provider 的 OAuth 帳號及上游回應決定。

```json
{
  "id": "primary-openai-compatible",
  "base_url": "https://api.openai.com",
  "api_key_env": "OPENAI_API_KEY",
  "chat_completions_path": "/v1/chat/completions",
  "enabled": true,
  "weight": 10,
  "max_concurrent": 32,
  "models": [
    {
      "name": "gpt-4.1-mini",
      "aliases": ["auto", "balanced"],
      "max_input_tokens": 1040000,
      "max_output_tokens": 32768,
      "capabilities": ["chat", "reasoning", "coding"],
      "cost_tier": 2,
      "quality_tier": 7
    }
  ]
}
```

## 選擇策略

目前預設採 `random`：

1. 先排除停用、未設定 `base_url`、超過 `max_concurrent`、token 容量不足的候選。
2. 將候選交給策略層處理，目前 `random` 會從合格 Provider/Model 中隨機挑選。
3. 策略層會產生 selection meta，回應 Header 會帶出 `X-Proxy-Strategy`、`X-Proxy-Provider`、`X-Proxy-Model` 等資訊。
4. 保留 `weighted_score` 策略實作，後續可加入成本、延遲、能力分類、健康度等策略。
5. Responses 後續請求若命中 `previous_response_id` 或 `prompt_cache_key` 黏著路由，會優先使用原 Provider/Model；僅在路由失效、Provider 不可用或配額差距超過容忍值時才降級回一般負載平衡。

## Codex 設定整合

設定整合功能可協助管理模型來源、Provider、模型目錄及相關擴充功能。實際支援項目會依執行環境、部署方式與可用權限而有所不同。

設定變更會以增量方式合併至既有的 `config.toml`，並保留專案信任資訊、功能旗標及其他不相關設定。套用前會保存必要狀態，以便需要時還原原先的 Provider、Profile 與模型選擇。

以下為設定整合的格式範例；實際的服務位址、驗證資訊與檔案路徑會依部署環境調整：

```toml
# BEGIN Mars LLM Proxy managed settings
model = "AUTO"
model_catalog_json = "<codex-home>/mars-model-catalog.json"
model_provider = "mars-llm-proxy"
# END Mars LLM Proxy managed settings

[model_providers.mars-llm-proxy]
name = "LoadBalanceProvider"
base_url = "https://proxy.example.com/v1"
env_key = "MARS_API_KEY"
wire_api = "responses"
requires_openai_auth = true

[features]
image_generation = true

[mcp_servers.mars-llm-proxy]
url = "https://proxy.example.com/mcp/"
bearer_token_env_var = "MARS_API_KEY"
enabled_tools = ["image_gen"]
tool_timeout_sec = 600
```

完成套用、還原或更新後，請完整重新啟動 Codex App、CLI 或 VS Code Extension Host，使新的設定與帳號狀態重新載入。

## 本地開發

```bash
go mod tidy
go test ./...
go run ./src/cmd/loadbalanceprovider
```

語法檢查通過後即可啟動。若未啟用任何 Provider，`/v1/chat/completions` 與 `/v1/responses` 會回傳 `service_unavailable`。
