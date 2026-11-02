package dalgo2athena

import (
	"context"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func waitIdle(t *testing.T, c *Client) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for {
		c.mu.Lock()
		empty := len(c.active) == 0
		c.mu.Unlock()
		if empty {
			return
		}
		select {
		case <-deadline.C:
			t.Fatal("owned physical work did not settle")
		default:
			runtime.Gosched()
		}
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type drainContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *drainContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}
func TestCallerAndDeadlineAfterDispatchStopCapturedAuthorityAndDrain(t *testing.T) {
	for _, mode := range []string{"caller", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			dispatched, disconnected, stopped, stopEOF := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			var identities atomic.Int64
			c, f := newFixture(t, func(f *fixture, w http.ResponseWriter, r *http.Request, op string, body map[string]any) bool {
				if op == "GetQueryExecution" {
					close(dispatched)
					select {
					case <-r.Context().Done():
						close(disconnected)
					case <-f.stop:
					}
					return true
				}
				if op == "StopQueryExecution" {
					if body["QueryExecutionId"] != "owned-query-1" || !strings.Contains(r.Header.Get("Authorization"), "Credential="+testCredentials.AccessKeyID+"/") {
						t.Error("stop changed owned ID/submit authority")
					}
					w.Header().Set("Content-Type", "application/x-amz-json-1.1")
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					close(stopped)
					select {
					case <-stopEOF:
					case <-f.stop:
					}
					return true
				}
				return false
			}, func(o *Options) {
				o.MaxConcurrent = 1
				o.Identity = func(context.Context) (Identity, error) {
					credentials := testCredentials
					if identities.Add(1) > 2 {
						credentials = rotatedCredentials
					}
					return AttestedIdentity(testAccount, "reader", "generation1", credentials), nil
				}
				if mode == "deadline" {
					o.Timeout = 500 * time.Millisecond
				}
			})
			t.Cleanup(func() { closeSignal(stopEOF) })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := c.QueryPage(ctx, query(), ReadOptions{}); done <- e }()
			wait(t, dispatched)
			if mode == "caller" {
				cancel()
			}
			e := await(t, done)
			expected := context.Canceled
			if mode == "deadline" {
				expected = context.DeadlineExceeded
			}
			if !errors.Is(e, expected) {
				t.Fatal("wrong original error", e)
			}
			wait(t, disconnected)
			wait(t, stopped)
			waitContext := &drainContext{Context: context.Background(), entered: make(chan struct{})}
			drained := make(chan error, 1)
			go func() { drained <- c.Shutdown(waitContext) }()
			wait(t, waitContext.entered)
			select {
			case e := <-drained:
				t.Fatal("drain returned before actual stop EOF", e)
			default:
			}
			close(stopEOF)
			if e := await(t, drained); e != nil {
				t.Fatal(e)
			}
			if f.count("StartQueryExecution") != 1 || f.count("StopQueryExecution") != 1 || identities.Load() != 3 {
				t.Fatal("submit/stop retry or stop credential refresh")
			}
			if !errors.Is(e, expected) {
				t.Fatal("cleanup replaced original error")
			}
		})
	}
}
func TestGlobalQuotaRefusalAndPendingRetryNeverEvictOtherCursor(t *testing.T) {
	c, f := newFixture(t, twoPageHook, func(o *Options) {
		o.MaxPageBytes = 4096
		o.MaxControlBytes = 4096
		o.MaxPageNodes = 64
		o.MaxColumns = 3
		o.MaxCursorNodes = 800
		o.MaxCursorBytes = 256 << 10
	})
	issued := []Cursor{}
	var stoppedAt int
	for i := 0; i < 8; i++ {
		before := len(f.snapshot())
		page, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 2})
		if e != nil {
			if len(f.snapshot()) != before {
				t.Fatal("quota refusal dispatched")
			}
			stoppedAt = i
			break
		}
		issued = append(issued, page.NextCursor)
	}
	if stoppedAt < 2 || len(issued) < 2 {
		t.Fatal("tiny valid pool did not hold distinct journeys", stoppedAt)
	}
	first := issued[0]
	before := f.count("GetQueryResults")
	_, e := c.NextPageWith(context.Background(), first, func(NativePage) error { return errors.New("deliberate bounded delivery failure") })
	if e == nil || f.count("GetQueryResults") != before+1 {
		t.Fatal("pending delivery fixture did not execute")
	}
	if e = c.CloseCursor(issued[1]); e != nil {
		t.Fatal(e)
	}
	page, e := c.NextPage(context.Background(), first)
	if e != nil || page.OrdinalStart != 1 || *page.Cells[0][0] != "b" || f.count("GetQueryResults") != before+1 {
		t.Fatal("pending retry lost native authority", e)
	}
	for _, cursor := range issued[2:] {
		if e = c.CloseCursor(cursor); e != nil {
			t.Fatal(e)
		}
	}
	c.mu.Lock()
	bytes, nodes, jobs := c.retainedBytes, c.retainedNodes, len(c.journeys)
	c.mu.Unlock()
	if bytes != 0 || nodes != 0 || jobs != 0 {
		t.Fatal("quota refund not exact", bytes, nodes, jobs)
	}
}
func TestOriginalDeadlineRefusesTerminalACKAfterPreparation(t *testing.T) {
	c, f := newFixture(t, twoPageHook, func(o *Options) { o.Timeout = 500 * time.Millisecond })
	first, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	entered := false
	_, e = c.NextPageWith(context.Background(), first.NextCursor, func(page NativePage) error {
		entered = true
		if !page.Receipt.ExpiresAt.Equal(first.Receipt.ExpiresAt) {
			t.Error("expiry renewed")
		}
		timer := time.NewTimer(time.Until(first.Receipt.ExpiresAt))
		defer timer.Stop()
		<-timer.C
		return nil
	})
	var problem *Error
	if !entered || !errors.Is(e, context.DeadlineExceeded) || !errors.As(e, &problem) || problem.Receipt().RowsAcknowledged != 1 || problem.Receipt().Complete {
		t.Fatal("late terminal ACK succeeded", e)
	}
	before := len(f.snapshot())
	if _, e = c.NextPage(context.Background(), first.NextCursor); e == nil || len(f.snapshot()) != before {
		t.Fatal("expired authority renewed")
	}
}
func TestCallerPageAbortKeepsPendingNativePageForRetry(t *testing.T) {
	c, f := newFixture(t, twoPageHook, nil)
	first, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, e = c.NextPageWith(ctx, first.NextCursor, func(NativePage) error { cancel(); return nil })
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	before := f.count("GetQueryResults")
	page, e := c.NextPage(context.Background(), first.NextCursor)
	if e != nil || page.OrdinalStart != 1 || *page.Cells[0][0] != "b" || f.count("GetQueryResults") != before || f.count("StopQueryExecution") != 0 {
		t.Fatal("page abort consumed/refetched/stopped completed query", e)
	}
}
func TestIgnoredCredentialRetainsBusyAndQuotaUntilActualSettlement(t *testing.T) {
	entered, release, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	c, f := newFixture(t, nil, func(o *Options) {
		o.MaxConcurrent = 1
		o.Identity = func(context.Context) (Identity, error) {
			if calls.Add(1) == 1 {
				close(entered)
				<-release
				close(returned)
			}
			return AttestedIdentity(testAccount, "reader", "generation1", testCredentials), nil
		}
	})
	t.Cleanup(func() { closeSignal(release) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.QueryPage(ctx, query(), ReadOptions{}); done <- e }()
	wait(t, entered)
	cancel()
	if e := await(t, done); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	c.mu.Lock()
	charged, busy := c.retainedBytes, len(c.active)
	c.mu.Unlock()
	if charged <= 1024 || busy != 1 {
		t.Fatal("ignored work refunded early")
	}
	if _, e := c.QueryPage(context.Background(), query(), ReadOptions{}); e == nil || calls.Load() != 1 || len(f.snapshot()) != 0 {
		t.Fatal("physical admission bypass")
	}
	close(release)
	wait(t, returned)
	waitIdle(t, c)
	c.mu.Lock()
	charged = c.retainedBytes
	c.mu.Unlock()
	if charged != 0 {
		t.Fatal("late settlement leaked quota")
	}
	if _, e := c.QueryPage(context.Background(), query(), ReadOptions{}); e != nil {
		t.Fatal("reopen after physical settlement", e)
	}
}

type heldBody struct {
	entered, closed, release chan struct{}
	once, closeOnce          sync.Once
}

func (b *heldBody) Read([]byte) (int, error) {
	b.once.Do(func() { close(b.entered) })
	<-b.release
	return 0, io.EOF
}
func (b *heldBody) Close() error { b.closeOnce.Do(func() { close(b.closed) }); <-b.release; return nil }
func TestIgnoredBodyAndCloseKeepLeaseAndShutdownBounded(t *testing.T) {
	c, _ := newFixture(t, nil, func(o *Options) { o.MaxConcurrent = 1 })
	body := &heldBody{entered: make(chan struct{}), closed: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { closeSignal(body.release) })
	c.transport = roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: body, ContentLength: -1}, nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.QueryPage(ctx, query(), ReadOptions{}); done <- e }()
	wait(t, body.entered)
	cancel()
	if e := await(t, done); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	wait(t, body.closed)
	waitCtx, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	if e := c.Shutdown(waitCtx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("hostile body/Close escaped drain bound", e)
	}
	c.mu.Lock()
	active := len(c.active)
	c.mu.Unlock()
	if active != 1 {
		t.Fatal("drain forcibly released physical work")
	}
	close(body.release)
	if e := c.Shutdown(context.Background()); e != nil {
		t.Fatal(e)
	}
}
func TestConcurrentSameCursorRefusesWhileProviderReadIsPending(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	c, f := newFixture(t, func(f *fixture, w http.ResponseWriter, r *http.Request, op string, body map[string]any) bool {
		if op == "GetQueryResults" && body["NextToken"] != nil {
			close(entered)
			select {
			case <-release:
				writeJSON(t, w, 200, nativeRows([][]*string{{text("b")}}, "", false))
			case <-f.stop:
			}
			return true
		}
		return twoPageHook(f, w, r, op, body)
	}, nil)
	t.Cleanup(func() { closeSignal(release) })
	first, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := c.NextPage(context.Background(), first.NextCursor); done <- e }()
	wait(t, entered)
	before := f.count("GetQueryResults")
	if _, e = c.NextPage(context.Background(), first.NextCursor); e == nil || f.count("GetQueryResults") != before {
		t.Fatal("concurrent same-cursor dispatch")
	}
	close(release)
	if e := await(t, done); e != nil {
		t.Fatal(e)
	}
}
func TestLateHeadersRetainPhysicalAdmissionAndCloseOwnedBody(t *testing.T) {
	entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
	c, f := newFixture(t, nil, func(o *Options) { o.MaxConcurrent = 1 })
	original := c.transport
	var calls atomic.Int64
	c.transport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: &ackBody{Reader: strings.NewReader(`{}`), closed: closed}}, nil
		}
		return original.RoundTrip(r)
	})
	t.Cleanup(func() { closeSignal(release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, e := c.QueryPage(ctx, query(), ReadOptions{}); done <- e }()
	wait(t, entered)
	cancel()
	if e := await(t, done); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if _, e := c.QueryPage(context.Background(), query(), ReadOptions{}); e == nil || calls.Load() != 1 || len(f.snapshot()) != 0 {
		t.Fatal("late headers released physical admission")
	}
	close(release)
	wait(t, closed)
	waitIdle(t, c)
	if _, e := c.QueryPage(context.Background(), query(), ReadOptions{}); e != nil {
		t.Fatal("late body cleanup prevented reuse", e)
	}
}

type ackBody struct {
	io.Reader
	closed chan struct{}
	once   sync.Once
}

func (b *ackBody) Close() error { b.once.Do(func() { close(b.closed) }); return nil }
func TestFailedSuccessorDeliveryRetainsGenerationAndPendingPage(t *testing.T) {
	c, f := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, body map[string]any) bool {
		if op != "GetQueryResults" {
			return false
		}
		switch body["NextToken"] {
		case nil:
			writeJSON(t, w, 200, nativeRows([][]*string{header(), {text("a")}}, "second", true))
		case "second":
			writeJSON(t, w, 200, nativeRows([][]*string{{text("b")}}, "third", false))
		case "third":
			writeJSON(t, w, 200, nativeRows([][]*string{{text("c")}}, "", false))
		default:
			t.Error("unexpected native token")
			w.WriteHeader(400)
		}
		return true
	}, nil)
	first, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, e = c.NextPageWith(ctx, first.NextCursor, func(page NativePage) error {
		if !page.HasMore || page.NextCursor == "" || page.OrdinalStart != 1 {
			t.Error("successor delivery fixture invalid")
		}
		cancel()
		return nil
	})
	if !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	before := f.count("GetQueryResults")
	second, e := c.NextPage(context.Background(), first.NextCursor)
	if e != nil || !second.HasMore || second.OrdinalStart != 1 || *second.Cells[0][0] != "b" || second.Receipt.RowsAcknowledged != 2 || f.count("GetQueryResults") != before || !second.Receipt.ExpiresAt.Equal(first.Receipt.ExpiresAt) {
		t.Fatal("successor failure consumed or renewed original authority", e)
	}
	last, e := c.NextPage(context.Background(), second.NextCursor)
	if e != nil || last.HasMore || last.OrdinalStart != 2 || last.Receipt.RowsAcknowledged != 3 || *last.Cells[0][0] != "c" {
		t.Fatal("pending recovery did not preserve contiguous ordinals", e)
	}
}
func TestStopAcknowledgementRequiresExactlyEmpty200AndKeepsOriginalCancellation(t *testing.T) {
	for _, sample := range []struct {
		name          string
		status        int
		body, outcome string
	}{{"empty", 200, "", "cancel_requested"}, {"object", 200, "{}", "unknown"}, {"whitespace", 200, " ", "unknown"}, {"denied", 403, `{"message":"PRIVATE"}`, "unknown"}} {
		t.Run(sample.name, func(t *testing.T) {
			dispatched, stopped := make(chan struct{}), make(chan struct{})
			c, f := newFixture(t, func(f *fixture, w http.ResponseWriter, r *http.Request, op string, body map[string]any) bool {
				switch op {
				case "GetQueryExecution":
					close(dispatched)
					select {
					case <-r.Context().Done():
					case <-f.stop:
					}
					return true
				case "StopQueryExecution":
					if body["QueryExecutionId"] != "owned-query-1" || !strings.Contains(r.Header.Get("Authorization"), "Credential="+testCredentials.AccessKeyID+"/") {
						t.Error("stop used foreign ID or principal")
					}
					w.WriteHeader(sample.status)
					if sample.body != "" {
						_, _ = io.WriteString(w, sample.body)
					}
					close(stopped)
					return true
				}
				return false
			}, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := c.QueryPage(ctx, query(), ReadOptions{}); done <- e }()
			wait(t, dispatched)
			c.mu.Lock()
			var owned *journey
			for state := range c.journeys {
				if state.queryID == "owned-query-1" {
					owned = state
				}
			}
			c.mu.Unlock()
			if owned == nil {
				t.Fatal("owned submitted journey not captured")
			}
			cancel()
			original := await(t, done)
			if !errors.Is(original, context.Canceled) || strings.Contains(original.Error(), "PRIVATE") {
				t.Fatal("stop replaced or disclosed original cancellation", original)
			}
			wait(t, stopped)
			waitIdle(t, c)
			c.mu.Lock()
			outcome, state := owned.cancelOutcome, owned.state
			c.mu.Unlock()
			if outcome != sample.outcome || state == "CANCELLED" || f.count("StopQueryExecution") != 1 || f.count("StartQueryExecution") != 1 {
				t.Fatal("invalid stop ACK classification/retry", outcome, state)
			}
			if e := c.Shutdown(context.Background()); e != nil {
				t.Fatal(e)
			}
			if !errors.Is(original, context.Canceled) {
				t.Fatal("drain changed original cancellation")
			}
		})
	}
}
