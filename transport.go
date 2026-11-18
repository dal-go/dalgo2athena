package dalgo2athena

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
)

// wireSize only accepts closed adapter-owned JSON. It counts every occurrence
// before encoding/json can allocate an escaped copy. No custom marshaler runs.
func wireSize(v any, remaining int) (int, error) {
	charge := func(n int) (int, error) {
		if n > remaining {
			return 0, fail("bounds")
		}
		return n, nil
	}
	switch x := v.(type) {
	case nil:
		return charge(4)
	case bool:
		return charge(5)
	case int:
		return charge(24)
	case int64:
		return charge(24)
	case json.Number:
		if len(x) > 128 || !numberPattern.MatchString(string(x)) {
			return 0, fail("input")
		}
		return charge(len(x))
	case string:
		n, e := jsonStringSize(x)
		if e != nil {
			return 0, e
		}
		return charge(n)
	case []string:
		n := 2
		for _, s := range x {
			cost, e := wireSize(s, remaining-n-1)
			if e != nil {
				return 0, e
			}
			n += cost + 1
		}
		return charge(n)
	case []any:
		n := 2
		for _, s := range x {
			cost, e := wireSize(s, remaining-n-1)
			if e != nil {
				return 0, e
			}
			n += cost + 1
		}
		return charge(n)
	case map[string]any:
		n := 2
		for k, v := range x {
			key, e := wireSize(k, remaining-n-2)
			if e != nil {
				return 0, e
			}
			n += key + 2
			cost, e := wireSize(v, remaining-n)
			if e != nil {
				return 0, e
			}
			n += cost
		}
		return charge(n)
	default:
		return 0, fail("input")
	}
}
func (o *operation) identity() (Identity, error) {
	if e := o.check(); e != nil {
		return Identity{}, e
	}
	id, e := foreign(o, o.ctx, func() (Identity, error) {
		if e := o.check(); e != nil {
			return Identity{}, e
		}
		return o.client.options.Identity(o.ctx)
	})
	if final := o.check(); final != nil {
		return Identity{}, final
	}
	if e != nil {
		return Identity{}, fail("authentication")
	}
	if id.account != o.client.options.AccountID || !name(id.subject) || len(id.subject) > 512 || !name(id.generation) || len(id.generation) > 128 || id.credentials.AccessKeyID == "" || id.credentials.SecretAccessKey == "" || len(id.credentials.AccessKeyID) > 256 || len(id.credentials.SecretAccessKey) > 256 || len(id.credentials.SessionToken) > 16384 {
		return Identity{}, fail("authentication")
	}
	for _, s := range []string{id.credentials.AccessKeyID, id.credentials.SecretAccessKey, id.credentials.SessionToken} {
		for _, r := range s {
			if r < 33 || r > 126 {
				return Identity{}, fail("authentication")
			}
		}
	}
	if id.credentials.CanExpire && !time.Now().Before(id.credentials.Expires) {
		return Identity{}, fail("authentication")
	}
	// Keep only the bounded signing fields. Credential Source/history metadata
	// is not needed by SigV4 and must not become an unaccounted retained graph.
	id.credentials = aws.Credentials{AccessKeyID: strings.Clone(id.credentials.AccessKeyID), SecretAccessKey: strings.Clone(id.credentials.SecretAccessKey), SessionToken: strings.Clone(id.credentials.SessionToken), CanExpire: id.credentials.CanExpire, Expires: id.credentials.Expires}
	id.account = strings.Clone(id.account)
	id.subject = strings.Clone(id.subject)
	id.generation = strings.Clone(id.generation)
	principal := hashScope("principal", id.account, id.subject, id.generation)
	o.client.mu.Lock()
	defer o.client.mu.Unlock()
	if o.j.principal != "" && o.j.principal != principal {
		return Identity{}, fail("principal")
	}
	o.j.principal = principal
	return id, nil
}

type wireReply struct {
	status  int
	payload map[string]any
}

func (o *operation) http(ctx context.Context, target string, body []byte, id Identity, maxBytes, maxNodes int, cleanup bool) (wireReply, error) {
	var reply wireReply
	if e := safeContext(ctx); e != nil {
		return reply, e
	}
	request, e := http.NewRequestWithContext(ctx, http.MethodPost, o.client.endpoint, bytes.NewReader(body))
	if e != nil {
		return reply, fail("transport")
	}
	request.Header.Set("Content-Type", "application/x-amz-json-1.1")
	request.Header.Set("X-Amz-Target", "AmazonAthena."+target)
	request.Header.Set("Accept-Encoding", "identity")
	sum := sha256.Sum256(body)
	if e = v4.NewSigner().SignHTTP(ctx, id.credentials, request, hex.EncodeToString(sum[:]), "athena", o.client.options.Region, time.Now()); e != nil {
		return reply, fail("signing")
	}
	if e = safeContext(ctx); e != nil {
		return reply, e
	}
	if target == "StartQueryExecution" {
		o.client.mu.Lock()
		if o.j.started {
			o.client.mu.Unlock()
			return reply, fail("submission-unknown")
		}
		credentialBytes := 128 + len(id.account) + len(id.subject) + len(id.generation) + len(id.credentials.AccessKeyID) + len(id.credentials.SecretAccessKey) + len(id.credentials.SessionToken)
		if e = o.client.adjustLocked(o.j, credentialBytes-o.j.credentialBytes, 8-o.j.credentialNodes); e != nil {
			o.client.mu.Unlock()
			return reply, e
		}
		o.j.credentialBytes = credentialBytes
		o.j.credentialNodes = 8
		o.j.started = true
		o.j.credential = id
		o.client.mu.Unlock()
	}
	response, e := o.client.transport.RoundTrip(request)
	if e != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return reply, fail("transport")
	}
	if response == nil || response.Body == nil {
		return reply, fail("protocol")
	}
	var once sync.Once
	var closeErr error
	closeBody := func() { once.Do(func() { closeErr = response.Body.Close() }) }
	done, settled := make(chan struct{}), make(chan struct{})
	o.track()
	go func() {
		defer o.settled()
		defer close(settled)
		select {
		case <-ctx.Done():
			if !cleanup {
				o.stop()
			}
			closeBody()
		case <-done:
		}
	}()
	defer func() { close(done); <-settled }()
	defer closeBody()
	if e = safeContext(ctx); e != nil {
		return reply, e
	}
	reply.status = response.StatusCode
	if response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" {
		return reply, fail("protocol")
	}
	headers, count := 0, 0
	for k, vs := range response.Header {
		for _, v := range vs {
			headers += len(k) + len(v)
			count++
			if headers > 65536 || count > 1024 {
				return reply, fail("bounds")
			}
		}
	}
	if maxBytes <= 0 || maxNodes <= 0 || response.ContentLength > int64(maxBytes) {
		return reply, fail("bounds")
	}
	var buffer bytes.Buffer
	chunk := make([]byte, 4096)
	for {
		if e = safeContext(ctx); e != nil {
			return reply, e
		}
		n, readErr := response.Body.Read(chunk[:min(len(chunk), maxBytes-buffer.Len()+1)])
		if n > 0 {
			_, _ = buffer.Write(chunk[:n])
			if !cleanup {
				o.client.mu.Lock()
				o.j.responseBytes += n
				exceeded := o.j.responseBytes > o.client.options.MaxResponseBytes
				o.client.mu.Unlock()
				if exceeded {
					return reply, fail("bounds")
				}
			}
			if buffer.Len() > maxBytes {
				return reply, fail("bounds")
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return reply, fail("transport")
		}
		if n == 0 {
			select {
			case <-ctx.Done():
				return reply, safeContext(ctx)
			default:
			}
		}
	}
	closeBody()
	if closeErr != nil {
		return reply, fail("transport")
	}
	if e = safeContext(ctx); e != nil {
		return reply, e
	}
	raw := buffer.Bytes()
	if cleanup {
		if reply.status != 200 {
			return reply, &Error{Code: "remote", Status: reply.status}
		}
		if len(raw) != 0 {
			return reply, fail("protocol")
		}
		reply.payload = map[string]any{}
		return reply, nil
	}
	value, nodes, e := strictJSON(raw, maxNodes, func() error { return safeContext(ctx) })
	if !cleanup {
		o.client.mu.Lock()
		o.j.nodes += nodes
		over := o.j.nodes > o.client.options.MaxNodes
		o.client.mu.Unlock()
		if over {
			return reply, fail("bounds")
		}
	}
	if e != nil {
		return reply, e
	}
	payload, ok := value.(map[string]any)
	if !ok {
		return reply, fail("protocol")
	}
	reply.payload = payload
	// Capture only a validated ID from THIS submitted request before the public
	// final cancellation check. Late unknown submits do not trigger unbounded stop.
	if target == "StartQueryExecution" && reply.status == 200 {
		if queryID, ok := payload["QueryExecutionId"].(string); ok && validQueryID(queryID) {
			o.client.mu.Lock()
			if o.j.queryID == "" {
				o.j.queryID = queryID
				o.j.state = "QUEUED"
			}
			o.client.mu.Unlock()
		}
	}
	if reply.status != 200 {
		return reply, &Error{Code: "remote", Status: reply.status}
	}
	return reply, nil
}
func (o *operation) rpc(target string, args map[string]any, page bool) (map[string]any, error) {
	if e := o.check(); e != nil {
		return nil, e
	}
	o.client.mu.Lock()
	remaining := o.client.options.MaxRequestBytes - o.j.requestBytes
	if o.j.requests >= o.client.options.MaxRequests {
		o.client.mu.Unlock()
		return nil, fail("bounds")
	}
	o.client.mu.Unlock()
	if _, e := wireSize(args, remaining); e != nil {
		return nil, e
	}
	body, e := json.Marshal(args)
	if e != nil {
		return nil, fail("input")
	}
	o.client.mu.Lock()
	if len(body) > o.client.options.MaxRequestBytes-o.j.requestBytes {
		o.client.mu.Unlock()
		return nil, fail("bounds")
	}
	o.j.requestBytes += len(body)
	o.j.requests++
	maxBytes := min(o.client.options.MaxControlBytes, o.client.options.MaxResponseBytes-o.j.responseBytes)
	maxNodes := o.client.options.MaxNodes - o.j.nodes
	if page {
		maxBytes = min(maxBytes, o.client.options.MaxPageBytes)
		maxNodes = min(maxNodes, o.client.options.MaxPageNodes)
	}
	o.client.mu.Unlock()
	if maxBytes <= 0 || maxNodes <= 0 {
		return nil, fail("bounds")
	}
	id, e := o.identity()
	if e != nil {
		return nil, e
	}
	reply, e := foreign(o, o.ctx, func() (wireReply, error) { return o.http(o.ctx, target, body, id, maxBytes, maxNodes, false) })
	if final := o.check(); final != nil {
		return nil, final
	}
	return reply.payload, e
}
func (o *operation) stop() {
	o.client.mu.Lock()
	j := o.j
	if !j.started || j.stopped || j.state == "SUCCEEDED" || j.state == "FAILED" || j.state == "CANCELLED" {
		o.client.mu.Unlock()
		return
	}
	if j.queryID == "" {
		j.state = "SUBMISSION_UNKNOWN"
		j.cancelOutcome = "unknown"
		o.client.mu.Unlock()
		return
	}
	j.stopped = true
	j.cancelOutcome = "unknown"
	id, credential := j.queryID, j.credential
	o.client.mu.Unlock()
	o.track()
	go func() {
		defer o.settled()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		body, _ := json.Marshal(map[string]any{"QueryExecutionId": id})
		reply, e := foreign(o, ctx, func() (wireReply, error) {
			return o.http(ctx, "StopQueryExecution", body, credential, 16384, 1024, true)
		})
		if e == nil && reply.status == 200 {
			o.client.mu.Lock()
			j.cancelOutcome = "cancel_requested"
			o.client.mu.Unlock()
		}
	}()
}
func validQueryID(s string) bool {
	if s == "" || len(s) > 128 {
		return false
	}
	for _, r := range s {
		if r <= 32 || r > 126 {
			return false
		}
	}
	return true
}
func object(v any) (map[string]any, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, fail("protocol")
	}
	return m, nil
}
func integer(v any) (int64, error) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, fail("protocol")
	}
	i, e := strconv.ParseInt(string(n), 10, 64)
	if e != nil {
		return 0, fail("protocol")
	}
	return i, nil
}
func optionalInt(m map[string]any, k string) (*int64, error) {
	if v, ok := m[k]; ok && v != nil {
		n, e := integer(v)
		if e != nil {
			return nil, e
		}
		return &n, nil
	}
	return nil, nil
}
func (o *operation) resultConfiguration() map[string]any {
	enc := map[string]any{"EncryptionOption": o.client.options.Encryption.Option}
	if o.client.options.Encryption.KMSKey != "" {
		enc["KmsKey"] = o.client.options.Encryption.KMSKey
	}
	return map[string]any{"OutputLocation": o.client.options.OutputLocation, "ExpectedBucketOwner": o.client.options.ExpectedBucketOwner, "EncryptionConfiguration": enc}
}
func (o *operation) verifyOutput(v any, execution bool) error {
	m, e := object(v)
	if e != nil {
		return e
	}
	output, ok := m["OutputLocation"].(string)
	expected := o.client.options.OutputLocation
	if !ok || !execution && output != expected || execution && !strings.HasPrefix(output, expected) || m["ExpectedBucketOwner"] != o.client.options.ExpectedBucketOwner {
		return fail("source-policy")
	}
	encryption, e := object(m["EncryptionConfiguration"])
	if e != nil || encryption["EncryptionOption"] != o.client.options.Encryption.Option {
		return fail("source-policy")
	}
	key, _ := encryption["KmsKey"].(string)
	if key != o.client.options.Encryption.KMSKey {
		return fail("source-policy")
	}
	return nil
}
func enabled(v any, key string) bool { m, ok := v.(map[string]any); return ok && m[key] == true }
func profileDisabled(m map[string]any, objectKey, flag string) error {
	v, present := m[objectKey]
	if !present {
		return nil
	}
	obj, e := object(v)
	if e != nil {
		return e
	}
	if value, present := obj[flag]; present {
		b, ok := value.(bool)
		if !ok {
			return fail("protocol")
		}
		if b {
			return fail("source-policy")
		}
	}
	return nil
}
func engine(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	s, _ := m["EffectiveEngineVersion"].(string)
	return s == "Athena engine version 2" || s == "Athena engine version 3"
}
func (o *operation) submitArgs(p plan, token string) map[string]any {
	args := map[string]any{"QueryString": p.sql, "ClientRequestToken": token, "WorkGroup": o.client.options.WorkGroup, "QueryExecutionContext": map[string]any{"Catalog": o.client.options.Catalog, "Database": o.client.options.Database}, "ResultConfiguration": o.resultConfiguration(), "ResultReuseConfiguration": map[string]any{"ResultReuseByAgeConfiguration": map[string]any{"Enabled": false}}}
	if len(p.parameters) > 0 {
		args["ExecutionParameters"] = p.parameters
	}
	return args
}
func (o *operation) start() error {
	// Admit the entire original escaped Start envelope before credentials or
	// even the read-only workgroup preflight. The random token has a fixed64
	// character representation. rpc still checks the actual remaining budget.
	if e := o.check(); e != nil {
		return e
	}
	if _, e := o.pointCost(o.j.plan); e != nil {
		return e
	}
	if e := o.check(); e != nil {
		return e
	}
	wg, e := o.rpc("GetWorkGroup", map[string]any{"WorkGroup": o.client.options.WorkGroup}, false)
	if e != nil {
		return e
	}
	w, e := object(wg["WorkGroup"])
	if e != nil {
		return e
	}
	cfg, e := object(w["Configuration"])
	if e != nil {
		return e
	}
	cutoff, e := integer(cfg["BytesScannedCutoffPerQuery"])
	if e != nil || w["Name"] != o.client.options.WorkGroup || w["State"] != "ENABLED" || cfg["EnforceWorkGroupConfiguration"] != true || cutoff < 10000000 || cutoff > o.client.options.MaxScanBytes || !engine(cfg["EngineVersion"]) {
		return fail("source-policy")
	}
	for objectKey, flag := range map[string]string{"ManagedQueryResultsConfiguration": "Enabled", "IdentityCenterConfiguration": "EnableIdentityCenter", "QueryResultsS3AccessGrantsConfiguration": "EnableS3AccessGrants"} {
		if e = profileDisabled(cfg, objectKey, flag); e != nil {
			return e
		}
	}
	if e = o.verifyOutput(cfg["ResultConfiguration"], false); e != nil {
		return e
	}
	token, e := randomText()
	if e != nil {
		return e
	}
	p := o.j.plan
	args := o.submitArgs(p, token)
	result, e := o.rpc("StartQueryExecution", args, false)
	if e != nil {
		return e
	}
	id, ok := result["QueryExecutionId"].(string)
	if !ok || !validQueryID(id) {
		return fail("protocol")
	}
	o.client.mu.Lock()
	owned := o.j.queryID
	o.client.mu.Unlock()
	if owned != id {
		return fail("protocol")
	}
	for {
		o.client.mu.Lock()
		o.j.polls++
		polls := o.j.polls
		o.client.mu.Unlock()
		if polls > o.client.options.MaxPolls {
			return fail("bounds")
		}
		reply, e := o.rpc("GetQueryExecution", map[string]any{"QueryExecutionId": id}, false)
		if e != nil {
			return e
		}
		q, e := object(reply["QueryExecution"])
		if e != nil || q["QueryExecutionId"] != id {
			return fail("protocol")
		}
		status, e := object(q["Status"])
		if e != nil {
			return e
		}
		state, ok := status["State"].(string)
		if !ok {
			return fail("protocol")
		}
		switch state {
		case "QUEUED", "RUNNING", "SUCCEEDED", "FAILED", "CANCELLED":
		default:
			return fail("protocol")
		}
		o.client.mu.Lock()
		o.j.state = state
		o.client.mu.Unlock()
		context, e := object(q["QueryExecutionContext"])
		if e != nil || q["WorkGroup"] != o.client.options.WorkGroup || context["Catalog"] != o.client.options.Catalog || context["Database"] != o.client.options.Database || q["Query"] != p.sql || !engine(q["EngineVersion"]) {
			return fail("source-policy")
		}
		if e = profileDisabled(q, "ManagedQueryResultsConfiguration", "Enabled"); e != nil {
			return e
		}
		if value, present := q["StatementType"]; present && value != "DML" {
			return fail("source-policy")
		}
		if value, present := q["SubstatementType"]; present && value != "SELECT" {
			return fail("source-policy")
		}
		if e = o.verifyOutput(q["ResultConfiguration"], true); e != nil {
			return e
		}
		if statsValue, present := q["Statistics"]; present {
			stats, e := object(statsValue)
			if e != nil {
				return e
			}
			if e = profileDisabled(stats, "ResultReuseInformation", "ReusedPreviousResult"); e != nil {
				return e
			}
			if v, present := stats["DataScannedInBytes"]; present {
				n, e := integer(v)
				if e != nil || n < 0 {
					return fail("protocol")
				}
				o.client.mu.Lock()
				o.j.scanned = strconv.FormatInt(n, 10)
				o.client.mu.Unlock()
			}
		}
		if state == "SUCCEEDED" {
			return o.check()
		}
		if state == "FAILED" || state == "CANCELLED" {
			return fail("execution")
		}
		timer := time.NewTimer(o.client.options.PollInterval)
		select {
		case <-o.ctx.Done():
			timer.Stop()
			return o.check()
		case <-timer.C:
		}
	}
}
