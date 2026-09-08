package proxy

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type smokeClosedDownstream struct{ *httptest.ResponseRecorder }

func (smokeClosedDownstream) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

type smokePipeBody struct {
	*io.PipeReader
	closes atomic.Int32
}

func (b *smokePipeBody) Close() error {
	b.closes.Add(1)
	return b.PipeReader.Close()
}

func TestRepeatedStreamCleanupSmoke(t *testing.T) {
	baseline := activeProviderStreams.Load()
	for iteration := 0; iteration < 100; iteration++ {
		for _, rejected := range []bool{false, true} {
			event := "data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n"
			if rejected {
				event = "data: {\"type\":\"error\",\"message\":\"Selected model is at capacity\"}\n\n"
			}
			body := &countedStreamBody{Reader: strings.NewReader(event)}
			_, err := streamCopyWithProviderIdleTimeout(httptest.NewRecorder(), body, time.Now(), true, nil, nil, nil, ResponsesRefusalTerminal, responsesStreamFailureTerminal)
			if (err != nil) != rejected || body.closed.Load() != 1 || activeProviderStreams.Load() != baseline {
				t.Fatalf("串流收尾不完整: round=%d rejected=%t closes=%d active=%d err=%v", iteration, rejected, body.closed.Load(), activeProviderStreams.Load(), err)
			}
		}
		r, upstream := io.Pipe()
		body := &smokePipeBody{PipeReader: r}
		producer := make(chan struct{})
		go func() {
			defer close(producer)
			defer upstream.Close()
			for {
				if _, err := io.WriteString(upstream, "data: {\"type\":\"response.output_text.delta\",\"delta\":\"text\"}\n\n"); err != nil {
					return
				}
			}
		}()
		_, err := streamCopyWithProviderIdleTimeout(smokeClosedDownstream{httptest.NewRecorder()}, body, time.Now(), true, nil, nil, nil, ResponsesRefusalTerminal, responsesStreamFailureTerminal)
		if !errors.Is(err, io.ErrClosedPipe) || body.closes.Load() != 1 || activeProviderStreams.Load() != baseline {
			t.Fatalf("下游斷線未回收上游: round=%d closes=%d active=%d err=%v", iteration, body.closes.Load(), activeProviderStreams.Load(), err)
		}
		select {
		case <-producer:
		case <-time.After(time.Second):
			t.Fatal("上游產生串流的 goroutine 未停止")
		}
	}
}
