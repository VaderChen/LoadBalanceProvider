package api

import (
	"bufio"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"LoadBalanceProvider/src/balancer"
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/proxy"
)

type failoverSmokeTransport func(*http.Request) (*http.Response, error)

func (f failoverSmokeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCapacityFailoverKeepsDownstreamSmoke(t *testing.T) {
	for _, mode := range []string{"recover", "exhausted", "tool_started"} {
		t.Run(mode, func(t *testing.T) {
			cfg := &domain.ProxyConfig{RetryCount: 1}
			for _, id := range []string{"a", "b"} {
				cfg.Providers = append(cfg.Providers, domain.LLMProviderConfig{
					ID: id, Name: id, Kind: "openai", Type: "openai", Enabled: true,
					BaseURL: "https://8.8.8.8", MaxConcurrent: 4,
					Models: []domain.LLMModelConfig{{Name: "smoke", MaxInputTokens: 100000, MaxOutputTokens: 8192, Capabilities: []string{"chat", "responses", "tools"}}},
				})
			}
			var mu sync.Mutex
			var providers, requests []string
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			client := proxy.NewClient()
			client.HTTPClient = &http.Client{Transport: failoverSmokeTransport(func(r *http.Request) (*http.Response, error) {
				body, _ := io.ReadAll(r.Body)
				mu.Lock()
				providers = append(providers, r.Header.Get("X-Proxy-Provider"))
				requests = append(requests, string(body))
				attempt := len(providers)
				mu.Unlock()
				reader, writer := io.Pipe()
				go func() {
					defer writer.Close()
					id := "resp_discard"
					if attempt > 1 {
						id = "resp_success"
					}
					io.WriteString(writer, "data: {\"type\":\"response.created\",\"sequence_number\":1,\"response\":{\"id\":\""+id+"\"}}\n\n")
					io.WriteString(writer, "data: {\"type\":\"response.in_progress\",\"sequence_number\":2}\n\n")
					if mode == "tool_started" {
						io.WriteString(writer, "data: {\"type\":\"response.output_item.added\",\"sequence_number\":3,\"item\":{\"type\":\"function_call\",\"id\":\"fc_1\",\"call_id\":\"call_1\",\"name\":\"exec\",\"arguments\":\"\"}}\n\n")
						io.WriteString(writer, "data: {\"type\":\"response.function_call_arguments.delta\",\"delta\":\"{\"}\n\n")
					}
					if attempt == 1 {
						select {
						case <-release:
						case <-r.Context().Done():
							return
						}
					}
					if mode == "recover" && attempt > 1 {
						io.WriteString(writer, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"recovered\"}\n\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_success\",\"status\":\"completed\",\"output\":[]}}\n\n")
					} else {
						io.WriteString(writer, "event: error\ndata: {\"type\":\"error\",\"sequence_number\":4,\"message\":\"Our servers are currently overloaded. Please try again later.\"}\n\n")
					}
				}()
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: reader}, nil
			})}
			h := &HTTPAPI{Client: client, Balancer: balancer.NewLoadBalancer(cfg)}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, _ := io.ReadAll(r.Body)
				h.handleResponsesProxy(w, r, body)
			}))
			defer server.Close()
			defer unblock()
			downstream := &http.Client{Timeout: 12 * time.Second}
			resp, err := downstream.Post(server.URL+"/v1/responses", "application/json", strings.NewReader(`{"model":"smoke","stream":true,"input":"keep this request"}`))
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			reader := bufio.NewReader(resp.Body)
			first, err := reader.ReadString('\n')
			if err != nil {
				t.Fatal(err)
			}
			if mode != "tool_started" && !strings.Contains(first, "response.ping") {
				t.Fatalf("initial response leaked: %q", first)
			}
			unblock()
			rest, err := io.ReadAll(reader)
			if err != nil {
				t.Fatal(err)
			}
			output := first + string(rest)
			mu.Lock()
			defer mu.Unlock()
			if mode == "tool_started" {
				if len(providers) != 1 || !strings.Contains(output, "response.failed") || !strings.Contains(output, `"id":"resp_discard"`) {
					t.Fatalf("tool request was replayed or unterminated: %v %s", providers, output)
				}
			} else {
				if len(providers) != 2 || providers[0] == providers[1] || requests[0] != requests[1] {
					t.Fatalf("invalid replay: providers=%v requests=%v", providers, requests)
				}
				if strings.Contains(output, "resp_discard") {
					t.Fatalf("failed provider ID leaked: %s", output)
				}
				if mode == "recover" && (!strings.Contains(output, "response.completed") || !strings.Contains(output, "recovered") || strings.Contains(output, "overloaded")) {
					t.Fatalf("failed recovery: %s", output)
				}
			}
			if mode != "recover" && (!strings.Contains(output, "response.failed") || strings.Contains(output, "response.completed") || !strings.Contains(output, "overloaded")) {
				t.Fatalf("failure disguised as success: %s", output)
			}
		})
	}
}
