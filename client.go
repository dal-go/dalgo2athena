package dalgo2athena

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

type Client struct {
	options                      Options
	endpoint, sourceDigest       string
	transport                    http.RoundTripper
	owned                        *http.Transport
	mu                           sync.Mutex
	closed                       bool
	active                       map[*operation]struct{}
	journeys                     map[*journey]struct{}
	handles                      map[Cursor]*journey
	retainedBytes, retainedNodes int
	stateChanged                 chan struct{}
}
type journey struct {
	plan                                                                                                                              plan
	deadline, expires                                                                                                                 time.Time
	principal                                                                                                                         string
	credential                                                                                                                        Identity
	queryID, state, cancelOutcome                                                                                                     string
	started, stopped, retired, busy, complete, collecting, delivered                                                                  bool
	handle                                                                                                                            Cursor
	token                                                                                                                             string
	seen                                                                                                                              map[string]bool
	header                                                                                                                            bool
	columns                                                                                                                           []Column
	pending                                                                                                                           *pendingPage
	generation, ordinal, acknowledged, requests, requestBytes, responseBytes, nodes, pages, fetched, deliveries, polls, executionRows int
	bytes, slots, reserveBytes, reserveNodes                                                                                          int
	materialized                                                                                                                      [][]*string
	materializedBytes, materializedNodes                                                                                              int
	planBytes, planNodes, credentialBytes, credentialNodes                                                                            int
	scanned                                                                                                                           string
}
type pendingPage struct {
	columns      []Column
	cells        [][]*string
	next         string
	header       bool
	bytes, nodes int
}
type operation struct {
	client      *Client
	j           *journey
	parent, ctx context.Context
	cancel      context.CancelFunc
	deadline    time.Time
	physical    int
	finished    bool
}

var regionPattern = regexp.MustCompile(`^[a-z]{2}(-gov)?-[a-z]+-[0-9]+$`)
var accountPattern = regexp.MustCompile(`^[0-9]{12}$`)
var workgroupPattern = regexp.MustCompile(`^[a-zA-Z0-9._-]{1,128}$`)

func name(s string) bool {
	return s != "" && len(s) <= 4096 && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func bounded(v *int, def, max int) bool {
	if *v == 0 {
		*v = def
	}
	return *v > 0 && *v <= max
}
func randomText() (string, error) {
	var b [32]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", fail("entropy")
	}
	return hex.EncodeToString(b[:]), nil
}
func hashScope(domain string, fields ...string) string {
	h := sha256.New()
	for _, v := range append([]string{"dalgo-athena-native-v1", domain}, fields...) {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(v)))
		_, _ = h.Write(n[:])
		_, _ = h.Write([]byte(v))
	}
	return hex.EncodeToString(h.Sum(nil))
}
func safeContext(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		if e == context.DeadlineExceeded {
			return context.DeadlineExceeded
		}
		return context.Canceled
	}
	return nil
}

// New is lazy. It performs neither credential discovery nor AWS requests.
func New(options Options) (*Client, error) {
	if !regionPattern.MatchString(options.Region) || !accountPattern.MatchString(options.AccountID) || !accountPattern.MatchString(options.ExpectedBucketOwner) || !name(options.SourceID) || !name(options.PolicyRevision) || !workgroupPattern.MatchString(options.WorkGroup) || !name(options.Catalog) || !name(options.Database) || options.Identity == nil {
		return nil, fail("configuration")
	}
	if options.Partition == "" {
		options.Partition = "aws"
	}
	suffix := "amazonaws.com"
	switch options.Partition {
	case "aws":
		if strings.HasPrefix(options.Region, "cn-") || strings.HasPrefix(options.Region, "us-gov-") {
			return nil, fail("configuration")
		}
	case "aws-cn":
		if !strings.HasPrefix(options.Region, "cn-") {
			return nil, fail("configuration")
		}
		suffix = "amazonaws.com.cn"
	case "aws-us-gov":
		if !strings.HasPrefix(options.Region, "us-gov-") {
			return nil, fail("configuration")
		}
	default:
		return nil, fail("configuration")
	}
	u, e := url.Parse(options.OutputLocation)
	if e != nil || u.Scheme != "s3" || u.Host == "" || u.Host != u.Hostname() || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.RawPath != "" || !strings.HasSuffix(u.Path, "/") || len(options.OutputLocation) > 2048 {
		return nil, fail("configuration")
	}
	if options.MaxScanBytes < 10000000 || (options.Encryption.Option != "SSE_S3" && options.Encryption.Option != "SSE_KMS") || options.Encryption.Option == "SSE_KMS" && !name(options.Encryption.KMSKey) || options.Encryption.Option == "SSE_S3" && options.Encryption.KMSKey != "" {
		return nil, fail("configuration")
	}
	if options.Timeout == 0 {
		options.Timeout = 30 * time.Second
	}
	if options.CursorTTL == 0 {
		options.CursorTTL = 120 * time.Second
	}
	if options.Timeout <= 0 || options.Timeout > 120*time.Second || options.CursorTTL <= 0 || options.CursorTTL > 120*time.Second {
		return nil, fail("configuration")
	}
	if options.PollInterval == 0 && !options.ImmediatePolling {
		options.PollInterval = 100 * time.Millisecond
	}
	if options.PollInterval < 0 || options.PollInterval > options.Timeout {
		return nil, fail("configuration")
	}
	if !bounded(&options.MaxConcurrent, 4, 16) || !bounded(&options.MaxKeys, 100, 1000) || !bounded(&options.MaxRows, 10000, 100000) || !bounded(&options.MaxRequests, 512, 2048) || !bounded(&options.MaxPages, 100, 1000) || !bounded(&options.MaxPolls, 100, 1000) || !bounded(&options.MaxColumns, 256, 256) || !bounded(&options.MaxJourneys, 1000, 1000) || !bounded(&options.MaxRequestBytes, 65536, 262144) || !bounded(&options.MaxResponseBytes, 32<<20, 32<<20) || !bounded(&options.MaxControlBytes, 4<<20, 4<<20) || !bounded(&options.MaxPageBytes, 1<<20, 4<<20) || !bounded(&options.MaxNodes, 65536, 262144) || !bounded(&options.MaxPageNodes, 8192, 32768) || !bounded(&options.MaxCursorBytes, 16<<20, 16<<20) || !bounded(&options.MaxCursorNodes, 65536, 65536) {
		return nil, fail("configuration")
	}
	reserveBytes, reserveNodes := pageReservation(options)
	if reserveBytes+options.MaxPageBytes+32*options.MaxPageNodes+1056*options.MaxPages+1024 > options.MaxCursorBytes || reserveNodes+options.MaxPageNodes+2*options.MaxPages+64 > options.MaxCursorNodes {
		return nil, fail("configuration")
	}
	if len(options.Tables) == 0 || len(options.Tables) > 256 {
		return nil, fail("configuration")
	}
	tables := make(map[string]Table, len(options.Tables))
	configBytes := 0
	for alias, t := range options.Tables {
		if !name(alias) || !name(t.Name) || len(t.Columns) == 0 || len(t.Columns) > options.MaxColumns {
			return nil, fail("configuration")
		}
		configBytes += len(alias) + len(t.Name) + len(t.IDColumn)
		seen := map[string]bool{}
		hasID := t.IDColumn == ""
		for _, col := range t.Columns {
			if !name(col.Name) || !name(col.Type) || seen[col.Name] || col.Label != "" || col.Nullable != "" || col.CatalogName != "" || col.SchemaName != "" || col.TableName != "" || col.Precision != nil || col.Scale != nil || col.CaseSensitive != nil {
				return nil, fail("configuration")
			}
			seen[col.Name] = true
			if col.Name == t.IDColumn {
				if col.Type != "varchar" && col.Type != "string" && col.Type != "char" {
					return nil, fail("configuration")
				}
				hasID = true
			}
			configBytes += len(col.Name) + len(col.Type) + 128
			if configBytes > 65536 {
				return nil, fail("configuration")
			}
		}
		if !hasID {
			return nil, fail("configuration")
		}
		t.Columns = cloneColumns(t.Columns)
		tables[alias] = t
	}
	options.Tables = tables
	// A valid tight pool must fit one worst pending reservation, a full original
	// schema/token journal, a bounded plan and the captured signing principal.
	if reserveBytes+options.MaxPageBytes+32*options.MaxPageNodes+1056*options.MaxPages+1024+options.MaxRequestBytes+configBytes+20000 > options.MaxCursorBytes || reserveNodes+options.MaxPageNodes+2*options.MaxPages+64+options.MaxColumns*13+80 > options.MaxCursorNodes {
		return nil, fail("configuration")
	}
	var transport *http.Transport
	if options.Transport == nil {
		transport = &http.Transport{DialContext: (&net.Dialer{Timeout: options.Timeout}).DialContext}
	} else {
		transport = options.Transport.Clone()
	}
	if transport.DialTLS != nil || transport.DialTLSContext != nil {
		return nil, fail("configuration")
	}
	host := "athena." + options.Region + "." + suffix
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
		if transport.TLSClientConfig.RootCAs != nil {
			transport.TLSClientConfig.RootCAs = transport.TLSClientConfig.RootCAs.Clone()
		}
	}
	if transport.TLSClientConfig.InsecureSkipVerify || transport.TLSClientConfig.ServerName != "" && transport.TLSClientConfig.ServerName != host {
		return nil, fail("configuration")
	}
	transport.TLSClientConfig.MinVersion = max(tls.VersionTLS12, transport.TLSClientConfig.MinVersion)
	transport.TLSClientConfig.NextProtos = []string{"http/1.1"}
	transport.Proxy = nil
	transport.DisableKeepAlives = true
	transport.DisableCompression = true
	transport.ForceAttemptHTTP2 = false
	transport.Protocols = new(http.Protocols)
	transport.Protocols.SetHTTP1(true)
	transport.MaxResponseHeaderBytes = 65536
	transport.MaxConnsPerHost = options.MaxConcurrent * 2
	transport.TLSHandshakeTimeout = options.Timeout
	transport.ResponseHeaderTimeout = options.Timeout
	// Only closed bounded trusted data is marshalled; never credential/provider.
	config, _ := json.Marshal(struct {
		Partition, Region, Account, Source, Revision, WorkGroup, Catalog, Database, Output, Owner string
		Encryption                                                                                Encryption
		Tables                                                                                    map[string]Table
		MaxScanBytes                                                                              int64
	}{options.Partition, options.Region, options.AccountID, options.SourceID, options.PolicyRevision, options.WorkGroup, options.Catalog, options.Database, options.OutputLocation, options.ExpectedBucketOwner, options.Encryption, options.Tables, options.MaxScanBytes})
	return &Client{options: options, endpoint: "https://" + host + "/", sourceDigest: hashScope("source", string(config)), transport: transport, owned: transport, active: map[*operation]struct{}{}, journeys: map[*journey]struct{}{}, handles: map[Cursor]*journey{}, stateChanged: make(chan struct{})}, nil
}
func pageReservation(o Options) (int, int) {
	return 3*o.MaxPageBytes + 64*o.MaxPageNodes + 512*o.MaxColumns + 4096, 2*o.MaxPageNodes + 32*o.MaxColumns + 128
}
func (c *Client) adjustLocked(j *journey, bytes, nodes int) error {
	if bytes > c.options.MaxCursorBytes-c.retainedBytes || nodes > c.options.MaxCursorNodes-c.retainedNodes {
		return fail("cursor-quota")
	}
	j.bytes += bytes
	j.slots += nodes
	c.retainedBytes += bytes
	c.retainedNodes += nodes
	return nil
}
func (o *operation) bindPlanLocked(p plan) error {
	bytes, nodes := nativeSize(p.table.Columns, nil)
	bytes += len(p.sql) + len(p.collection) + len(p.digest)
	nodes += 8 + len(p.expected) + len(p.parameters)
	for _, s := range p.expected {
		bytes += len(s) + 32
	}
	for _, s := range p.parameters {
		bytes += len(s) + 32
	}
	j := o.j
	if e := o.client.adjustLocked(j, bytes-j.planBytes, nodes-j.planNodes); e != nil {
		return e
	}
	j.planBytes = bytes
	j.planNodes = nodes
	j.plan = p
	return nil
}
func (c *Client) retireLocked(j *journey) {
	j.retired = true
	if j.handle != "" {
		delete(c.handles, j.handle)
		j.handle = ""
	}
	c.refundLocked(j)
}
func (c *Client) refundLocked(j *journey) {
	if j.retired && !j.busy {
		if _, ok := c.journeys[j]; ok {
			delete(c.journeys, j)
			c.retainedBytes -= j.bytes
			c.retainedNodes -= j.slots
			j.bytes = 0
			j.slots = 0
			j.pending = nil
			j.materialized = nil
			j.columns = nil
			j.seen = nil
		}
	}
}
func (c *Client) pruneLocked() {
	now := time.Now()
	for j := range c.journeys {
		if !now.Before(j.expires) {
			c.retireLocked(j)
		}
	}
}
func (c *Client) begin(parent context.Context, cursor Cursor) (*operation, error) {
	if parent == nil {
		return nil, fail("input")
	}
	if e := safeContext(parent); e != nil {
		return nil, e
	}
	now := time.Now()
	callerDeadline, hasDeadline := parent.Deadline()
	if e := safeContext(parent); e != nil {
		return nil, e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneLocked()
	if c.closed {
		return nil, fail("closed")
	}
	if len(c.active) >= c.options.MaxConcurrent {
		return nil, fail("busy")
	}
	var j *journey
	first := cursor == ""
	if first {
		if len(c.journeys) >= c.options.MaxJourneys {
			return nil, fail("cursor-quota")
		}
		deadline := now.Add(c.options.Timeout)
		if hasDeadline && callerDeadline.Before(deadline) {
			deadline = callerDeadline
		}
		expiry := minTime(deadline, now.Add(c.options.CursorTTL))
		j = &journey{deadline: deadline, expires: expiry, seen: map[string]bool{}, busy: true, cancelOutcome: "not_requested"}
		if e := c.adjustLocked(j, 1024, 64); e != nil {
			return nil, e
		}
		c.journeys[j] = struct{}{}
	} else {
		j = c.handles[cursor]
		if j == nil || j.retired || j.handle != cursor {
			return nil, fail("cursor")
		}
		if j.busy {
			return nil, fail("busy")
		}
		j.busy = true
	}
	j.deliveries++
	if j.deliveries > c.options.MaxRequests {
		j.busy = false
		if first {
			c.retireLocked(j)
		}
		return nil, fail("bounds")
	}
	deadline := minTime(j.deadline, j.expires)
	if hasDeadline && callerDeadline.Before(deadline) {
		deadline = callerDeadline
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	o := &operation{client: c, j: j, parent: parent, ctx: ctx, cancel: cancel, deadline: deadline}
	c.active[o] = struct{}{}
	return o, nil
}
func minTime(a, b time.Time) time.Time {
	if a.Before(b) {
		return a
	}
	return b
}
func (o *operation) check() error {
	if e := safeContext(o.parent); e != nil {
		return e
	}
	if !time.Now().Before(o.deadline) {
		return context.DeadlineExceeded
	}
	o.client.mu.Lock()
	closed := o.client.closed
	retired := o.j.retired
	o.client.mu.Unlock()
	if closed {
		return fail("closed")
	}
	if retired {
		return fail("expired")
	}
	return safeContext(o.ctx)
}
func (o *operation) track() { o.client.mu.Lock(); o.physical++; o.client.mu.Unlock() }
func (o *operation) releaseLocked() {
	if o.finished && o.physical == 0 {
		if _, ok := o.client.active[o]; ok {
			delete(o.client.active, o)
			j := o.j
			if j.pending == nil && j.reserveBytes != 0 {
				_ = o.client.adjustLocked(j, -j.reserveBytes, -j.reserveNodes)
				j.reserveBytes = 0
				j.reserveNodes = 0
			}
			j.busy = false
			if !time.Now().Before(j.expires) {
				o.client.retireLocked(j)
			} else {
				o.client.refundLocked(j)
			}
			close(o.client.stateChanged)
			o.client.stateChanged = make(chan struct{})
		}
	}
}
func (o *operation) settled() {
	o.client.mu.Lock()
	o.physical--
	o.releaseLocked()
	o.client.mu.Unlock()
}
func (o *operation) finish() {
	o.client.mu.Lock()
	o.finished = true
	o.releaseLocked()
	o.client.mu.Unlock()
	o.cancel()
}

type foreignResult[T any] struct {
	value T
	err   error
}

func foreign[T any](o *operation, ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	o.track()
	ch := make(chan foreignResult[T], 1)
	go func() { v, e := fn(); o.settled(); ch <- foreignResult[T]{v, e} }()
	select {
	case <-ctx.Done():
		return zero, safeContext(ctx)
	case r := <-ch:
		return r.value, r.err
	}
}
func observe[T any](o *operation, fn func() T) (T, error) {
	var zero T
	if e := o.check(); e != nil {
		return zero, e
	}
	v := fn()
	if e := o.check(); e != nil {
		return zero, e
	}
	return v, nil
}
func (o *operation) receiptLocked() Receipt {
	j := o.j
	return Receipt{QueryID: j.queryID, SourceDigest: o.client.sourceDigest, PlanDigest: j.plan.digest, PrincipalDigest: j.principal, ScopeDigest: o.scopeLocked(j.columns, j.ordinal, j.generation), ExpiresAt: j.expires, State: j.state, CancelOutcome: j.cancelOutcome, Requests: j.requests, PagesFetched: j.pages, RowsFetched: j.fetched, RowsAcknowledged: j.acknowledged, ResponseBytes: j.responseBytes, Nodes: j.nodes, Deliveries: j.deliveries, DataScannedInBytes: j.scanned, Complete: j.complete && j.delivered}
}

// finalLocked only observes the private standard-library child context and
// owned state. No application method runs under the final commit lock.
func (o *operation) finalLocked() error {
	select {
	case <-o.ctx.Done():
		return safeContext(o.ctx)
	default:
	}
	if !time.Now().Before(o.deadline) || !time.Now().Before(o.j.expires) {
		return context.DeadlineExceeded
	}
	if o.client.closed {
		return fail("closed")
	}
	if o.j.retired {
		return fail("expired")
	}
	return nil
}
func (o *operation) markDelivered() (Receipt, error) {
	if e := o.check(); e != nil {
		return Receipt{}, e
	}
	o.client.mu.Lock()
	defer o.client.mu.Unlock()
	receipt := o.receiptLocked()
	receipt.RowsAcknowledged = o.j.ordinal
	receipt.Complete = o.j.complete
	if e := o.finalLocked(); e != nil {
		return Receipt{}, e
	}
	o.j.acknowledged = o.j.ordinal
	o.j.delivered = true
	return receipt, nil
}
func (o *operation) scopeLocked(columns []Column, ordinal, generation int) string {
	j := o.j
	fields := []string{o.client.sourceDigest, j.plan.digest, j.principal, j.queryID, strconv.Itoa(j.plan.pageSize), strconv.Itoa(ordinal), strconv.Itoa(generation), strconv.FormatInt(j.expires.UnixNano(), 10)}
	for _, col := range columns {
		fields = append(fields, col.Name, col.Type, col.Label, col.Nullable, col.CatalogName, col.SchemaName, col.TableName)
		for _, v := range []*int64{col.Precision, col.Scale} {
			if v == nil {
				fields = append(fields, "unknown")
			} else {
				fields = append(fields, strconv.FormatInt(*v, 10))
			}
		}
		if col.CaseSensitive == nil {
			fields = append(fields, "unknown")
		} else {
			fields = append(fields, strconv.FormatBool(*col.CaseSensitive))
		}
	}
	return hashScope("journey-generation", fields...)
}
func (o *operation) error(err error, first bool) error {
	if err == nil {
		return nil
	}
	if fresh := o.check(); fresh != nil {
		err = fresh
	}
	o.stop()
	o.client.mu.Lock()
	if first || !time.Now().Before(o.j.expires) || o.client.closed {
		o.client.retireLocked(o.j)
	}
	receipt := o.receiptLocked()
	o.client.mu.Unlock()
	if errors.Is(err, dal.ErrNotSupported) {
		return &Error{Code: "not-implemented", sentinel: dal.ErrNotSupported, receipt: receipt}
	}
	var safe *Error
	if errors.As(err, &safe) {
		return &Error{Code: safe.Code, Status: safe.Status, receipt: receipt, sentinel: safe.sentinel}
	}
	if errors.Is(err, context.Canceled) {
		return &Error{Code: "cancelled", sentinel: context.Canceled, receipt: receipt}
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return &Error{Code: "deadline", sentinel: context.DeadlineExceeded, receipt: receipt}
	}
	return &Error{Code: "operation", receipt: receipt}
}
func (c *Client) Close() error {
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		for j := range c.journeys {
			c.retireLocked(j)
		}
		for o := range c.active {
			o.cancel()
		}
	}
	c.mu.Unlock()
	c.owned.CloseIdleConnections()
	return nil
}

// CloseCursor abandons one local completed-query journey without touching its
// result S3 objects or sending another query. A bridge must authorize its owner
// before exposing this local lifecycle action. Other cursors remain valid.
func (c *Client) CloseCursor(cursor Cursor) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	j := c.handles[cursor]
	if j == nil || j.handle != cursor {
		return fail("cursor")
	}
	c.retireLocked(j)
	return nil
}

// Shutdown retains the existing nonblocking Close and drains complete logical
// scopes, including unclosed readers/late work, for at most two seconds.
func (c *Client) Shutdown(ctx context.Context) error {
	deadline := time.Now().Add(2 * time.Second)
	if ctx != nil {
		if d, ok := ctx.Deadline(); ok {
			deadline = minTime(deadline, d)
		}
	}
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	_ = c.Close()
	if ctx == nil {
		return fail("input")
	}
	check := func() error {
		if e := safeContext(ctx); e != nil {
			return e
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		return nil
	}
	for {
		if e := check(); e != nil {
			return e
		}
		c.mu.Lock()
		empty := len(c.active) == 0
		changed := c.stateChanged
		c.mu.Unlock()
		if e := check(); e != nil {
			return e
		}
		if empty {
			return nil
		}
		select {
		case <-ctx.Done():
		case <-timer.C:
		case <-changed:
		}
	}
}
