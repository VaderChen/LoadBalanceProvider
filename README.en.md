# Load Balance Provider

[繁體中文](README.md) | **English** | [日本語](README.ja.md) | [한국어](README.ko.md)

`LoadBalanceProvider` is an LLM proxy service. It provides OpenAI-compatible Chat Completions and Responses APIs, and selects an appropriate backend LLM provider based on request size, workload, task characteristics, and real-time provider load.

## Goals

- **OpenAI-compatible endpoints**: Supports `POST /v1/chat/completions` and `POST /v1/responses`, allowing existing OpenAI SDKs and compatible clients to connect without changing their request style.
- **Prompt Cache affinity for long-running tasks**: Routes the same Responses conversation back to its original provider and model using `previous_response_id` or `prompt_cache_key`, preventing multi-turn tool calls and encrypted reasoning content from breaking after an account switch.
- **Multi-provider management**: Registers multiple OpenAI-compatible providers, model capabilities, weights, costs, and concurrency limits through `data/llm_proxy.json`.
- **Intelligent routing**: Uses estimated input tokens, expected output workload, message count, task type, and model capabilities for scored selection.
- **Regular and streaming responses**: Proxies standard HTTP responses and preserves SSE/chunked streaming when `stream=true`.
- **Load balancing**: Provides general-purpose evaluation based on provider weight, model quality, cost, active requests, and maximum concurrency.
- **Request-density and output-ratio monitoring**: Aggregates recent request rate, token consumption, model tier, and input/output ratio per API key to identify repeated low-output work or inappropriate use of high-tier models.
- **Standard MCP support**: Provides an MCP `2025-11-25` Streamable HTTP endpoint and exposes queries and operations other than key management as tools.

## Project Structure

- `src/cmd/loadbalanceprovider/main.go`: Service entry point that initializes the service framework, the HTTP API, and LLM proxy components.
- `src/service/cloud_service.go`: Service lifecycle and background provider status recording.
- `src/api/http_api.go`: REST API routes, including `/v1/chat/completions`, `/v1/responses`, `/api/health`, and `/api/providers`.
- `src/api/mcp.go`: Standard MCP Streamable HTTP transport, JSON-RPC lifecycle, tool catalog, and adapters for existing APIs.
- `src/domain/types.go`: Shared types for Chat Completions, providers, models, and error responses.
- `src/config/provider_config.go`: Loads LLM proxy settings from `data/llm_proxy.json` and applies defaults.
- `src/analyzer/request_analyzer.go`: Estimates request size, output workload, task type, and complexity.
- `src/balancer/load_balancer.go`: Filters and scores provider/model candidates and tracks real-time load.
- `src/keyusage/recorder.go`: Records monthly API-key usage and the most recent hour of request density and token consumption.
- `src/proxy/client.go`: Proxies OpenAI-compatible Chat Completions requests and streaming responses.
- `agent.properties`: Basic service configuration.
- `data/llm_proxy.json`: LLM provider, model capability, and load-balancing configuration.

## API

### Chat Completions

```http
POST /v1/chat/completions
Content-Type: application/json
Authorization: Bearer <client-token>
```

The request format remains compatible with OpenAI Chat Completions. The service parses `model`, `messages`, `stream`, `max_tokens`, or `max_completion_tokens`, selects a backend model, and forwards the request.

```json
{
  "model": "auto",
  "messages": [
    {"role": "user", "content": "Help me design a highly available architecture."}
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

The request format remains compatible with the OpenAI Responses API. Both regular responses and SSE streaming with `stream=true` are supported. `model` may specify an actual model or use `AUTO`, allowing the service to select a backend based on provider capabilities and real-time load.

```json
{
  "model": "AUTO",
  "input": "Analyze this project and propose a refactoring plan.",
  "stream": true,
  "prompt_cache_key": "project-refactor-2026"
}
```

The Responses API also supports compatible subroutes for retrieving, deleting, or canceling an existing response, listing its input items, and calculating input tokens. The service forwards each request to the provider that created the response and isolates routing data by the caller's API key.

#### Prompt Cache Affinity for Long-Running Tasks

Long-running Responses tasks often contain multi-turn reasoning, tool calls, or encrypted reasoning content. Switching providers or accounts during the task may prevent the upstream service from recognizing earlier state. The service automatically maintains conversation affinity:

1. After a response is created successfully, the service records the mapping between the response ID, `prompt_cache_key`, provider, model, and caller identity.
2. A later request carrying `previous_response_id` is routed back to the original provider and model.
3. When clients such as Codex omit `previous_response_id` but continue using the same `prompt_cache_key`, the request is still routed to the original provider and model. This is suitable for long-running agent tasks and multi-turn tool calls.
4. Affinity data is isolated by API key, so one caller cannot reuse another caller's response or Prompt Cache route.
5. If the provider becomes unavailable, its quota falls significantly below its peers, the affinity expires, or the route is evicted, the service removes `previous_response_id` and encrypted reasoning content that cannot be transferred across providers, then performs load balancing again instead of failing the entire task.

The following values can be adjusted under **Settings > Advanced** in the management interface:

- **Conversation affinity TTL**: Defaults to `30` minutes and accepts values from `1` to `10080` minutes. For long-running tasks, set it longer than the maximum expected interval between steps.
- **Affinity quota tolerance**: Defaults to `10` percentage points. Affinity may be released when the original provider's quota falls below the peer average by more than this value.
- **Response route limit**: Defaults to `2000` entries and accepts values from `100` to `100000`. Older routes are evicted after the limit is reached.

#### Per-key low-reasoning demotion

**Settings > Advanced > Low-reasoning demotion** can temporarily cap the model quality tier for high-frequency API keys whose completed responses contain little reasoning. The feature is disabled by default and is persisted in `data/advanced_settings.json`.

- The activation gate uses the average daily quota consumption of enabled providers that have observations. It defaults to `18%` and is not per-key daily usage. Setting it to `0` disables the gate; unknown quota data never activates it.
- Each API key is evaluated over a rolling `15`-minute window. Defaults require `≥8 req/min`, reasoning below `10%` of actual output, and at least `5` completed samples for which the upstream reported reasoning usage.
- When matched, the model quality tier is capped at `4` for `10` minutes by default. The state expires by time and is cleared by a settings change or service restart.
- This is a candidate quality cap, not a request model rewrite. Explicit models and key-level provider routes remain intact. If no eligible model is within the cap, selection fails open to a higher-tier model instead of returning no-provider availability.

To consistently reuse an existing Prompt Cache during a long-running task, the client should keep the same `prompt_cache_key` throughout that task and use a different stable identifier for each separate task.

### Health Checks

```http
GET /api/health
GET /api/providers
```

### Request-Density Monitoring

The **Real-time Monitoring** page groups recent activity by API key. It shows total and per-minute requests, completed-request token totals, average tokens per request, the average quality tier of models actually used, output ratio, prose ratio, reasoning ratio, and low/medium/high output and complexity distributions. The page refreshes in the background every `60` seconds and persists the selected time window, account-status filter, and visible columns in the current browser.

```http
GET /api/api-keys/density?window=15m
```

- `window` accepts seconds or a Go duration such as `60`, `5m`, or `1h`; the default is `5m` and the maximum is `1h`.
- **Output ratio** is completed output tokens divided by estimated input tokens. By default, per-request ratios `≤2%` are low output, `>2%` and `≤20%` are medium output, and `>20%` are high output. Administrators can change both boundaries under Settings > Advanced > Output Ratio Thresholds; live monitoring applies saved values immediately. Completed samples without a known input size are excluded from output-ratio tiers.
- The main output-ratio value is the median of per-request ratios so one unusually large context cannot dominate the result. The secondary aggregate value is total output tokens divided by total estimated input tokens in the window.
- **Prose ratio** is estimated user-visible prose tokens divided by actual completed output tokens. Only classifiable response samples are included, so consumers must inspect `prose_samples`; no samples does not mean a real ratio of zero.
- **Reasoning ratio** is upstream-reported reasoning tokens divided by actual completed output tokens. Reasoning is already included in completed output usage and must not be added to the total again.
- Model tier is the request-weighted average `quality_tier` of models actually used. Comparing a high model tier with an extremely low output ratio helps identify high-tier models repeatedly used for low-output work.
- Tool activity is limited to verifiable aggregates: calls, rounds, output tokens, continuation, and repeated-task signals. Legacy OS-tool ratio and tool-type classification fields have been removed.
- Complexity scores `1–3` are low, `4–6` are medium, and `7–10` are high. Missing or invalid scores are treated as low.
- Request density is recorded after classification. Token consumption includes only completed requests with actual usage, so the two populations may differ.
- Recent samples are memory-only and restart with the service. Persistent monthly counters remain in `usage/<key>/YYYY-MM.json`.
- Idle, disabled, and recently deleted keys may be listed, but the response exposes only names and aggregate values, never key IDs, prefixes, or masked keys.
- This is an administrative endpoint available only to authenticated web sessions; regular API keys and MCP keys cannot access it.

## MCP

The service provides one Streamable HTTP endpoint:

```text
http://<host>:<port>/mcp/
```

MCP is enabled by default and uses protocol version `2025-11-25`. It supports `initialize`, `notifications/initialized`, `ping`, `tools/list`, and `tools/call`. The endpoint does not maintain server-initiated SSE, so `GET /mcp/` returns `405 Method Not Allowed` as required; each JSON-RPC message uses a separate HTTP `POST`.

Authenticate with either an API key or a dedicated MCP key issued from **Key Management** in the management interface:

```http
POST /mcp/ HTTP/1.1
Authorization: Bearer <api-key>
Content-Type: application/json
Accept: application/json, text/event-stream

{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"example","version":"1.0.0"}}}
```

API keys can call general REST APIs and MCP. Temporary web login keys cannot call MCP. MCP keys can only call MCP, cannot call Chat, Responses, or other general REST APIs, and do not collect accumulated or monthly usage statistics.

The management interface under **Settings > MCP** allows you to:

- Enable or disable MCP.
- Enable read-only mode, which limits `tools/list` to operations that do not change state.
- Add allowed browser origins. Native MCP clients without an `Origin` header and same-origin browser requests do not need to be added.
- View the effective endpoint, protocol version, and currently exposed tools.

Tools use the existing REST handlers as their single execution source. They cover service status, models, providers, dashboards and usage, general/advanced/notification/MCP settings, benchmarks, system monitoring, system updates, Chat Completions, Responses, and multimodal proxying. API key listing, issuance, modification, activation, deletion, route binding, and usage queries are never exposed through MCP.

## Provider Configuration

Providers are configured in the `providers` section of `data/llm_proxy.json`. The sample configuration sets `enabled` to `false`; change it to `true` and configure the corresponding API key environment variable before production use.

Provider and notification target URLs only allow `http` and `https`. Link-local, unspecified, and multicast addresses are blocked, while private CIDRs and localhost/loopback remain allowed for internal model services.

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

## Selection Strategy

The default strategy is currently `random`:

1. Exclude candidates that are disabled, have no `base_url`, exceed `max_concurrent`, or lack sufficient token capacity.
2. Pass qualified candidates to the strategy layer. The current `random` strategy selects a provider/model at random from the qualified set.
3. The strategy layer generates selection metadata. Response headers include `X-Proxy-Strategy`, `X-Proxy-Provider`, `X-Proxy-Model`, and related values.
4. A `weighted_score` strategy implementation is retained for future cost, latency, capability classification, health, and other policies.
5. When a later Responses request matches an affinity route through `previous_response_id` or `prompt_cache_key`, the original provider and model are preferred. The service falls back to normal load balancing only when the route expires, the provider is unavailable, or the quota difference exceeds the configured tolerance.

## Codex Configuration Integration

Configuration integration helps manage model sources, providers, model catalogs, and related extensions. Available capabilities may vary by environment, deployment method, and granted permissions.

Changes are merged incrementally into the existing `config.toml` while preserving project trust entries, feature flags, and unrelated settings. Required state is retained before changes are applied so that the previous provider, profile, and model selection can be restored when needed.

After applying, restoring, or refreshing the configuration, fully restart Codex App, the CLI, or the VS Code Extension Host to reload the updated settings and account state.

## Local Development

```bash
go mod tidy
go test ./...
go run ./src/cmd/loadbalanceprovider
```

The service can be started after syntax checks pass. If no provider is enabled, `/v1/chat/completions` and `/v1/responses` return `service_unavailable`.
