package api

import (
	"errors"
	"strings"
	"sync"

	"LoadBalanceProvider/src/domain"
)

const providerMaxWaiting = 32

var errProviderWaitQueueFull = errors.New("原 Provider 等待佇列已滿，未送往上游；請稍後重試")

// 只計等待中的請求；不佔用上游併發名額，也不改變對話配對。
type providerWaitQueue struct {
	sync.Mutex
	waiting map[string]int
}

func (q *providerWaitQueue) acquire(req domain.ChatCompletionRequest) (func(), error) {
	key := strings.ToLower(strings.TrimSpace(req.ProviderID))
	if key == "" {
		key = strings.ToLower(strings.TrimSpace(req.Provider))
	}
	if key == "" {
		return func() {}, nil
	}
	q.Lock()
	defer q.Unlock()
	if q.waiting[key] >= providerMaxWaiting {
		return nil, errProviderWaitQueueFull
	}
	if q.waiting == nil {
		q.waiting = make(map[string]int)
	}
	q.waiting[key]++
	var once sync.Once
	return func() {
		once.Do(func() {
			q.Lock()
			defer q.Unlock()
			q.waiting[key]--
			if q.waiting[key] == 0 {
				delete(q.waiting, key)
			}
		})
	}, nil
}
