package proxy

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"time"

	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/security"
)

type providerResponseBody struct {
	io.ReadCloser
	cancel context.CancelCauseFunc
	timer  *time.Timer
}

func (b *providerResponseBody) Close() error {
	if b.timer != nil {
		b.timer.Stop()
	}
	b.cancel(nil)
	return b.ReadCloser.Close()
}

// 僅限制等待回應標頭的階段；已開始的串流交由無進展逾時管理。
func doProviderHTTPRequest(client *http.Client, req *http.Request, timeout time.Duration, providers ...*domain.LLMProviderConfig) (*http.Response, error) {
	// 每次上游嘗試先錯開 1–2 秒，且允許下游在等待期間取消。
	delay := time.NewTimer(time.Second + time.Duration(rand.Int64N(int64(time.Second))))
	defer delay.Stop()
	select {
	case <-req.Context().Done():
		return nil, context.Cause(req.Context())
	case <-delay.C:
	}
	if err := context.Cause(req.Context()); err != nil {
		return nil, err
	}
	var release func()
	if len(providers) > 0 {
		var err error
		release, err = acquireProviderDispatch(req.Context(), providers[0])
		if err != nil {
			return nil, err
		}
		defer func() {
			if release != nil {
				release()
			}
		}()
	}
	ctx, cancel := context.WithCancelCause(req.Context())
	expired := make(chan struct{})
	timer := time.AfterFunc(timeout, func() {
		cancel(context.DeadlineExceeded)
		close(expired)
	})
	resp, err := security.GuardedHTTPClient(client).Do(req.Clone(ctx))
	if !timer.Stop() {
		<-expired
	}
	if cause := context.Cause(ctx); cause != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		cancel(nil)
		return nil, fmt.Errorf("等待上游回應標頭失敗: %w", cause)
	}
	if err != nil {
		cancel(nil)
		return nil, err
	}
	body := &providerResponseBody{ReadCloser: resp.Body, cancel: cancel}
	if release != nil {
		body.ReadCloser = &dispatchResponseBody{ReadCloser: resp.Body, release: release}
		release = nil
	}
	// 錯誤回應也可能只送標頭後停住，讀取錯誤本文不得無限等待。
	if resp.StatusCode >= http.StatusBadRequest {
		body.timer = time.AfterFunc(timeout, func() { cancel(context.DeadlineExceeded) })
	}
	resp.Body = body
	return resp, nil
}
