# Load Balance Provider

[繁體中文](README.md) | [English](README.en.md) | **日本語** | [한국어](README.ko.md)

`LoadBalanceProvider` は LLM プロキシサービスです。OpenAI 互換の Chat Completions API と Responses API を提供し、リクエストサイズ、処理負荷、タスク特性、Provider のリアルタイム負荷に基づいて、適切なバックエンド LLM Provider を選択します。

## 目的と機能

- **OpenAI 互換エンドポイント**：`POST /v1/chat/completions` と `POST /v1/responses` をサポートし、既存の OpenAI SDK や互換 Client から接続できます。
- **長時間タスク向け Prompt Cache アフィニティ**：`previous_response_id` または `prompt_cache_key` を使って同じ Responses 会話を元の Provider/Model に戻し、複数ターンのツール呼び出しや暗号化された reasoning 内容がアカウント切り替えによって失効することを防ぎます。
- **複数 Provider の管理**：`data/llm_proxy.json` で複数の OpenAI-compatible Provider、モデル能力、重み、コスト、同時実行上限を登録します。
- **インテリジェントルーティング**：入力 token の推定値、出力負荷、メッセージ数、タスク種別、モデル能力を使用してスコアリングします。
- **通常応答とストリーミング**：通常の HTTP response を転送し、`stream=true` の場合は SSE/Chunked ストリーミングを維持します。
- **ロードバランシング**：Provider の重み、モデル品質、コスト、active request、max concurrent に基づいて汎用的に評価します。
- **リクエスト密度と出力比の監視**：API キーごとに直近のリクエスト頻度、Token 消費量、モデル等級、入出力比を集計し、低出力処理の反復や高性能モデルの不適切な利用を把握できます。
- **標準 MCP**：MCP `2025-11-25` Streamable HTTP エンドポイントを提供し、キー管理以外の照会と操作をツールとして公開します。

## ディレクトリ構成

- `src/cmd/loadbalanceprovider/main.go`：サービスフレームワーク、HTTP API、LLM Proxy コンポーネントを初期化するサービスエントリポイント。
- `src/service/cloud_service.go`：サービスのライフサイクルとバックグラウンドの Provider 状態記録。
- `src/api/http_api.go`：`/v1/chat/completions`、`/v1/responses`、`/api/health`、`/api/providers` を含む REST API ルート。
- `src/api/mcp.go`：標準 MCP Streamable HTTP、JSON-RPC ライフサイクル、ツールカタログ、既存 API アダプター。
- `src/domain/types.go`：Chat Completion、Provider、Model、エラー形式などの共通型。
- `src/config/provider_config.go`：`data/llm_proxy.json` から LLM Proxy 設定を読み込み、既定値を適用します。
- `src/analyzer/request_analyzer.go`：リクエストサイズ、出力負荷、タスク種別、複雑度を推定します。
- `src/balancer/load_balancer.go`：Provider/Model 候補のフィルタリング、スコアリング、リアルタイム負荷の追跡。
- `src/keyusage/recorder.go`：API キーの月次利用量と、直近 1 時間のリクエスト密度および Token 消費量を記録します。
- `src/proxy/client.go`：OpenAI-compatible Chat Completion の HTTP 転送とストリーミングプロキシ。
- `agent.properties`：サービスの基本設定。
- `data/llm_proxy.json`：LLM Provider、モデル能力、ロードバランシング設定。

## API

### Chat Completions

```http
POST /v1/chat/completions
Content-Type: application/json
Authorization: Bearer <client-token>
```

リクエスト形式は OpenAI Chat Completions と互換です。サービスは `model`、`messages`、`stream`、`max_tokens` または `max_completion_tokens` を解析し、バックエンドモデルを選択して転送します。

```json
{
  "model": "auto",
  "messages": [
    {"role": "user", "content": "高可用性アーキテクチャを設計してください。"}
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

リクエスト形式は OpenAI Responses API と互換です。通常応答と `stream=true` の SSE ストリーミングに対応します。`model` には実際のモデル名または `AUTO` を指定でき、Provider の能力とリアルタイム負荷に応じてバックエンドを選択します。

```json
{
  "model": "AUTO",
  "input": "このプロジェクトを分析し、リファクタリング計画を提案してください。",
  "stream": true,
  "prompt_cache_key": "project-refactor-2026"
}
```

Responses API は、既存 response の取得、削除、キャンセル、入力項目の取得、input token の計算などの互換サブルートにも対応します。リクエストは response を作成した Provider に転送され、ルーティング情報は呼び出し元の API Key ごとに分離されます。

#### 長時間タスク向け Prompt Cache アフィニティ

Responses の長時間タスクには、複数ターンの推論、ツール呼び出し、暗号化された reasoning 内容が含まれることがあります。途中で Provider またはアカウントが切り替わると、上流サービスが以前の状態を認識できない場合があります。本サービスは会話アフィニティを自動的に維持します。

1. response の作成に成功すると、response ID、`prompt_cache_key`、Provider、Model、呼び出し元 ID の対応を記録します。
2. 後続リクエストに `previous_response_id` がある場合、元の Provider/Model に優先的に戻します。
3. Codex などの Client が `previous_response_id` を送信せず、同じ `prompt_cache_key` を継続利用する場合でも、元の Provider/Model に戻します。長時間の Agent タスクや複数ターンのツール呼び出しに適しています。
4. アフィニティ情報は API Key ごとに分離され、別の呼び出し元の response または Prompt Cache ルートを共有できません。
5. Provider が利用不能、割り当て量が他 Provider より大幅に低い、アフィニティの期限切れ、またはルートが削除された場合、Provider 間で移行できない `previous_response_id` と暗号化 reasoning を除去してから再度ロードバランシングし、タスク全体の失敗を回避します。

管理画面の **設定 > 詳細設定** では、次の値を調整できます。

- **会話アフィニティ TTL**：既定値は `30` 分、設定範囲は `1`～`10080` 分です。長時間タスクでは、想定される最大ステップ間隔より長く設定してください。
- **アフィニティ割り当て許容値**：既定値は `10` パーセントポイントです。元の Provider の割り当て量が他 Provider の平均をこの値より大きく下回る場合、アフィニティを解除できます。
- **Response ルート上限**：既定値は `2000` 件、設定範囲は `100`～`100000` 件です。上限を超えると古いルートから削除されます。

#### API キー単位の低推論降格

**設定 > 詳細設定 > 低推論降格**では、Provider 全体の割り当て消費が進んだ後、高頻度かつ推論量が少ない API キーに対してモデル品質等級の上限を一時的に設定できます。既定では無効で、設定は `data/advanced_settings.json` に保存されます。

- 起動条件は、観測値がある有効 Provider の当日割り当て消費率の平均です。既定値は `18%` で、API キー単位の当日利用量ではありません。`0` は条件を無効化し、割り当て情報が不明な場合は起動しません。
- API キーごとに直近 `15` 分の移動窓を評価します。既定では `≥8 req/min`、実出力に占める推論 Token が `<10%`、かつ上流が推論量を報告した完了サンプルが `5` 件以上必要です。
- 条件成立後は品質等級上限を既定で `4`、`10` 分間適用します。時間経過で解除され、設定変更またはサービス再起動でもメモリ上の状態が消去されます。
- これは候補モデルの品質上限であり、明示指定モデルや API キーの Provider 固定設定を書き換えません。上限内の候補がなければ、Provider 不在エラーを避けるため高等級モデルへ fail-open します。

長時間タスクで既存の Prompt Cache を安定して再利用するには、タスク全体で同じ `prompt_cache_key` を使い続け、別のタスクには異なる安定した識別値を使用してください。

### ヘルスチェック

```http
GET /api/health
GET /api/providers
```

### リクエスト密度監視

管理画面の **リアルタイム監視** では、API キー別に総リクエスト数、1 分あたりのリクエスト数、完了済みリクエストの Token 総数と平均 Token 数、実際に使用したモデルの平均品質等級、出力比、本文比、推論比、低／中／高出力および複雑度分布を表示します。画面は `60` 秒ごとにバックグラウンド更新され、観測時間、アカウント状態、表示列は現在のブラウザーに保存されます。

```http
GET /api/api-keys/density?window=15m
```

- `window` は秒数、または `60`、`5m`、`1h` などの Go duration を受け付けます。既定値は `5m`、上限は `1h` です。
- **出力比**は完了出力 Token ÷ 推定入力 Token です。既定では、リクエスト単位で `≤2%` は低出力、`>2%` かつ `≤20%` は中出力、`>20%` は高出力です。管理者は「設定 > 詳細 > 出力比分類しきい値」で 2 つの境界を変更でき、保存後すぐにリアルタイム監視へ反映されます。入力量が不明な完了済みサンプルは出力比分類から除外されます。
- 画面の主な出力比はリクエスト単位の中央値で、単一の巨大 Context に結果が支配されることを防ぎます。副表示の集計値は、観測時間内の総出力 Token ÷ 総推定入力 Token です。
- **本文比**は、推定されたユーザー向け本文 Token ÷ 実際の完了出力 Token です。分類可能な応答だけが対象になるため、`prose_samples` も確認する必要があります。サンプルがない状態は本文比 `0%` を意味しません。
- **推論比**は、上流が報告した推論 Token ÷ 実際の完了出力 Token です。推論 Token はすでに完了出力量に含まれており、総量へ再加算してはいけません。
- モデル等級は、実際に使用したモデルの `quality_tier` をリクエスト数で加重平均した値です。高いモデル等級と極端に低い出力比を比較することで、高性能モデルを低出力処理に繰り返し使用しているアカウントを特定できます。
- ツール動作は、呼び出し数、ラウンド数、ツール出力 Token、継続、反復タスクなど検証可能な集計だけを保持します。旧 OS ツール比率とツール種別分類フィールドは削除されました。
- 複雑度スコア `1–3` は低、`4–6` は中、`7–10` は高です。無効なスコアは低として扱います。
- リクエスト密度は分類完了後に記録され、Token 消費量は実使用量を取得できた完了済みリクエストだけを対象とするため、集計母数は一致しない場合があります。
- 直近サンプルはメモリだけに保持され、サービス再起動後に再集計されます。月次の永続統計は `usage/<key>/YYYY-MM.json` に残ります。
- 無通信、無効化済み、直近に削除されたキーも表示されますが、Key ID、接頭辞、マスク済みキーは返さず、名称と集計値だけを公開します。
- この管理 API は認証済み Web セッション専用で、通常の API キーおよび MCP キーからは利用できません。

## MCP

サービスは単一の Streamable HTTP エンドポイントを提供します。

```text
http://<host>:<port>/mcp/
```

MCP は既定で有効で、プロトコルバージョンは `2025-11-25` です。`initialize`、`notifications/initialized`、`ping`、`tools/list`、`tools/call` をサポートします。サーバー主導の SSE 接続は維持しないため、仕様に従って `GET /mcp/` は `405 Method Not Allowed` を返し、各 JSON-RPC メッセージは個別の HTTP `POST` を使用します。

管理画面の **キー管理** で発行した API キーまたは MCP 専用キーで認証できます。

```http
POST /mcp/ HTTP/1.1
Authorization: Bearer <api-key>
Content-Type: application/json
Accept: application/json, text/event-stream

{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"example","version":"1.0.0"}}}
```

API Key は一般 REST API と MCP を呼び出せます。一時的な Web ログインキーは MCP を呼び出せません。MCP キーは MCP のみ呼び出せ、Chat、Responses、その他の一般 REST API は呼び出せません。また、累積および月次利用統計も収集しません。

管理画面の **設定 > MCP** では、次の操作ができます。

- MCP の有効化または無効化。
- 読み取り専用モード。有効な場合、`tools/list` は状態を変更しない照会ツールだけを返します。
- 許可するブラウザー Origin の追加。`Origin` を送信しないネイティブ MCP Client と同一オリジンのブラウザーリクエストは追加不要です。
- 実際のエンドポイント、プロトコルバージョン、現在公開中のツールの確認。

ツールは既存の REST handler を唯一の実行元として使用します。サービス状態、モデル、Provider、ダッシュボードと利用量、一般／詳細／通知／MCP 設定、ベンチマーク、システム監視、システム更新、Chat Completions、Responses、マルチモーダルプロキシを対象とします。API Key の一覧、発行、変更、有効化、削除、ルートバインド、利用量照会は MCP では公開されません。

## Provider 設定

Provider は `data/llm_proxy.json` の `providers` で設定します。サンプル設定では `enabled` が `false` です。本番利用前に `true` に変更し、対応する API Key 環境変数を設定してください。

Provider と通知先 URL は `http` と `https` のみ許可します。link-local、unspecified、multicast アドレスは拒否し、内部モデルサービス向けに private CIDR と localhost/loopback は許可します。

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

## 選択戦略

現在の既定戦略は `random` です。

1. 無効、`base_url` 未設定、`max_concurrent` 超過、token 容量不足の候補を除外します。
2. 適格な候補を戦略レイヤーへ渡します。現在の `random` は、適格な Provider/Model からランダムに選択します。
3. 戦略レイヤーは selection meta を生成し、レスポンスヘッダーに `X-Proxy-Strategy`、`X-Proxy-Provider`、`X-Proxy-Model` などを追加します。
4. 将来のコスト、遅延、能力分類、健全性などの戦略向けに `weighted_score` 実装を保持しています。
5. Responses の後続リクエストが `previous_response_id` または `prompt_cache_key` のアフィニティルートに一致した場合、元の Provider/Model を優先します。ルート失効、Provider 利用不能、または割り当て差が許容値を超えた場合のみ、通常のロードバランシングにフォールバックします。

## Codex 設定統合

設定統合機能は、モデルソース、Provider、モデルカタログ、および関連する拡張機能の管理を支援します。利用可能な機能は、実行環境、デプロイ方法、付与された権限によって異なります。

変更は既存の `config.toml` に増分方式でマージされ、プロジェクト信頼情報、機能フラグ、および無関係な設定は保持されます。必要に応じて以前の Provider、Profile、モデル選択を復元できるよう、適用前に必要な状態を保存します。

設定の適用、復元、更新後は、Codex App、CLI、または VS Code Extension Host を完全に再起動し、更新された設定とアカウント状態を再読み込みしてください。

## ローカル開発

```bash
go mod tidy
go test ./...
go run ./src/cmd/loadbalanceprovider
```

構文チェックに成功したらサービスを起動できます。有効な Provider がない場合、`/v1/chat/completions` と `/v1/responses` は `service_unavailable` を返します。
