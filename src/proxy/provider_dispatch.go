package proxy

import (
	"LoadBalanceProvider/src/domain"
	"LoadBalanceProvider/src/providerdispatch"
	"context"
	"io"
	"net/http"
)

func acquireProviderDispatch(ctx context.Context, p *domain.LLMProviderConfig) (func(), error) {
	return providerdispatch.Acquire(ctx, p)
}

func ConfigureDispatchProtection(settings domain.AdvancedSettingsConfig) {
	providerdispatch.Configure(settings)
}

func dispatchProviderHTTP(client *http.Client, req *http.Request, p *domain.LLMProviderConfig) (*http.Response, error) {
	return providerdispatch.Do(client, req, p)
}

type dispatchResponseBody struct {
	io.ReadCloser
	release func()
}

func (b *dispatchResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.release()
	return err
}
