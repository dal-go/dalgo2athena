package dalgo2athena

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/dal-go/dalgo/dal"
)

const testAccount = "123456789012"
const testHost = "athena.us-east-1.amazonaws.com"
const testOutput = "s3://owned-fixture-results/native/"

var testCredentials = aws.Credentials{AccessKeyID: "OWNEDACCESSKEY", SecretAccessKey: "owned-synthetic-secret", SessionToken: "owned-synthetic-session"}
var rotatedCredentials = aws.Credentials{AccessKeyID: "OWNEDACCESSKEY2", SecretAccessKey: "rotated-owned-secret", SessionToken: "rotated-owned-session"}

type wireCall struct {
	operation     string
	body          map[string]any
	authorization string
}
type fixture struct {
	mu      sync.Mutex
	calls   []wireCall
	queries map[string]map[string]any
	tokens  map[string]bool
	next    int
	stop    chan struct{}
}

func (f *fixture) snapshot() []wireCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]wireCall(nil), f.calls...)
}
func (f *fixture) count(operation string) int {
	n := 0
	for _, call := range f.snapshot() {
		if call.operation == operation {
			n++
		}
	}
	return n
}
func outputConfig() map[string]any {
	return map[string]any{"OutputLocation": testOutput, "ExpectedBucketOwner": testAccount, "EncryptionConfiguration": map[string]any{"EncryptionOption": "SSE_S3"}}
}
func workgroup() map[string]any {
	return map[string]any{"WorkGroup": map[string]any{"Name": "owned", "State": "ENABLED", "Configuration": map[string]any{"EnforceWorkGroupConfiguration": true, "BytesScannedCutoffPerQuery": 10000000, "EngineVersion": map[string]any{"EffectiveEngineVersion": "Athena engine version 3"}, "ResultConfiguration": outputConfig()}}}
}
func schema() map[string]any {
	return map[string]any{"ColumnInfo": []any{map[string]any{"Name": "ID", "Label": "ID", "Type": "varchar", "Nullable": "NULLABLE", "CaseSensitive": true}, map[string]any{"Name": "AMOUNT", "Type": "decimal", "Precision": 38, "Scale": 9, "Nullable": "UNKNOWN"}, map[string]any{"Name": "FLAG", "Type": "boolean"}}}
}
func nativeRows(rows [][]*string, token string, metadata bool) map[string]any {
	values := make([]any, len(rows))
	for i, row := range rows {
		data := make([]any, len(row))
		for j, v := range row {
			if v == nil {
				data[j] = map[string]any{}
			} else {
				data[j] = map[string]any{"VarCharValue": *v}
			}
		}
		values[i] = map[string]any{"Data": data}
	}
	set := map[string]any{"Rows": values}
	if metadata {
		set["ResultSetMetadata"] = schema()
	}
	result := map[string]any{"ResultSet": set}
	if token != "" {
		result["NextToken"] = token
	}
	return result
}
func text(s string) *string { return &s }
func header() []*string     { return []*string{text("ID"), text("AMOUNT"), text("FLAG")} }
func writeJSON(t *testing.T, w http.ResponseWriter, status int, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	w.WriteHeader(status)
	if e := json.NewEncoder(w).Encode(value); e != nil {
		t.Error("owned JSON response write failed")
	}
}
func newFixture(t *testing.T, hook func(*fixture, http.ResponseWriter, *http.Request, string, map[string]any) bool, configure func(*Options)) (*Client, *fixture) {
	t.Helper()
	f := &fixture{queries: map[string]map[string]any{}, tokens: map[string]bool{}, stop: make(chan struct{})}
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	certSpec := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: testHost}, DNSNames: []string{testHost}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, e := x509.CreateCertificate(rand.Reader, certSpec, certSpec, &key.PublicKey, key)
	if e != nil {
		t.Fatal(e)
	}
	cert, e := x509.ParseCertificate(der)
	if e != nil {
		t.Fatal(e)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, e := io.ReadAll(io.LimitReader(r.Body, 262145))
		_ = r.Body.Close()
		if e != nil || len(raw) > 262144 || r.TLS == nil || r.Host != testHost || r.Method != "POST" || r.ProtoMajor != 1 || !r.Close || r.URL.Path != "/" || r.URL.RawQuery != "" || r.Header.Get("Content-Type") != "application/x-amz-json-1.1" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("owned wire profile changed")
			w.WriteHeader(400)
			return
		}
		target := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AmazonAthena.")
		if target == r.Header.Get("X-Amz-Target") {
			t.Error("wrong Athena target prefix")
			w.WriteHeader(400)
			return
		}
		signed := r.Clone(context.Background())
		signed.URL.Scheme = "https"
		signed.URL.Host = testHost
		signed.RequestURI = ""
		signed.Header = make(http.Header)
		stamp, e := time.Parse("20060102T150405Z", r.Header.Get("X-Amz-Date"))
		if e != nil {
			t.Error("missing official SigV4 date")
			w.WriteHeader(400)
			return
		}
		expected := r.Header.Get("Authorization")
		parts := strings.Split(expected, "SignedHeaders=")
		if len(parts) != 2 {
			t.Error("missing signed header list")
			w.WriteHeader(400)
			return
		}
		for _, header := range strings.Split(strings.SplitN(parts[1], ",", 2)[0], ";") {
			if header != "host" {
				signed.Header[http.CanonicalHeaderKey(header)] = append([]string(nil), r.Header.Values(header)...)
			}
		}
		credential := testCredentials
		if strings.Contains(expected, "Credential="+rotatedCredentials.AccessKeyID+"/") {
			credential = rotatedCredentials
		}
		sum := sha256.Sum256(raw)
		if e = v4.NewSigner().SignHTTP(context.Background(), credential, signed, hex.EncodeToString(sum[:]), "athena", "us-east-1", stamp); e != nil || subtle.ConstantTimeCompare([]byte(expected), []byte(signed.Header.Get("Authorization"))) != 1 || r.Header.Get("X-Amz-Security-Token") != credential.SessionToken {
			t.Error("official signed authority changed")
			w.WriteHeader(403)
			return
		}
		parsed, _, e := strictJSON(raw, 65536, func() error { return nil })
		if e != nil {
			t.Error("invalid signed JSON body")
			w.WriteHeader(400)
			return
		}
		body, ok := parsed.(map[string]any)
		if !ok {
			t.Error("wire body not object")
			w.WriteHeader(400)
			return
		}
		f.mu.Lock()
		f.calls = append(f.calls, wireCall{target, body, expected})
		f.mu.Unlock()
		if hook != nil && hook(f, w, r, target, body) {
			return
		}
		switch target {
		case "GetWorkGroup":
			writeJSON(t, w, 200, workgroup())
		case "StartQueryExecution":
			token, ok := body["ClientRequestToken"].(string)
			if !ok || len(token) < 32 || len(token) > 128 {
				t.Error("missing explicit idempotency token")
			}
			if body["WorkGroup"] != "owned" {
				t.Error("workgroup changed")
			}
			f.mu.Lock()
			if f.tokens[token] {
				t.Error("new execution reused token")
			}
			f.tokens[token] = true
			f.next++
			id := "owned-query-" + strconv.Itoa(f.next)
			f.queries[id] = body
			f.mu.Unlock()
			writeJSON(t, w, 200, map[string]any{"QueryExecutionId": id})
		case "GetQueryExecution":
			id, _ := body["QueryExecutionId"].(string)
			f.mu.Lock()
			submitted := f.queries[id]
			f.mu.Unlock()
			if submitted == nil {
				t.Error("foreign query adoption")
				w.WriteHeader(404)
				return
			}
			cfg := outputConfig()
			cfg["OutputLocation"] = testOutput + id + ".csv"
			writeJSON(t, w, 200, map[string]any{"QueryExecution": map[string]any{"QueryExecutionId": id, "Query": submitted["QueryString"], "WorkGroup": "owned", "QueryExecutionContext": map[string]any{"Catalog": "AwsDataCatalog", "Database": "native"}, "EngineVersion": map[string]any{"EffectiveEngineVersion": "Athena engine version 3"}, "ResultConfiguration": cfg, "Status": map[string]any{"State": "SUCCEEDED"}, "Statistics": map[string]any{"DataScannedInBytes": 12345, "ResultReuseInformation": map[string]any{"ReusedPreviousResult": false}}}})
		case "GetQueryResults":
			id, _ := body["QueryExecutionId"].(string)
			f.mu.Lock()
			submitted := f.queries[id]
			f.mu.Unlock()
			if submitted == nil {
				t.Error("foreign result identity")
				w.WriteHeader(404)
				return
			}
			value := "a"
			if params, ok := submitted["ExecutionParameters"].([]any); ok && len(params) > 0 {
				p, _ := params[0].(string)
				value = strings.ReplaceAll(strings.TrimSuffix(strings.TrimPrefix(p, "'"), "'"), "''", "'")
			}
			rows := [][]*string{header()}
			if value != "missing" {
				rows = append(rows, []*string{text(value), text("9223372036854775807.000000001"), nil})
			}
			writeJSON(t, w, 200, nativeRows(rows, "", true))
		case "StopQueryExecution":
			w.WriteHeader(200)
		default:
			t.Error("unexpected AWS operation")
			w.WriteHeader(400)
		}
	}))
	server.TLS = &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	server.StartTLS()
	dialer := &net.Dialer{Timeout: time.Second}
	options := Options{Region: "us-east-1", Partition: "aws", AccountID: testAccount, SourceID: "owned-source", PolicyRevision: "v1", WorkGroup: "owned", Catalog: "AwsDataCatalog", Database: "native", OutputLocation: testOutput, ExpectedBucketOwner: testAccount, Encryption: Encryption{Option: "SSE_S3"}, MaxScanBytes: 10000000, Tables: map[string]Table{"rows": {Name: "EXISTING_ROWS", IDColumn: "ID", Columns: []Column{{Name: "ID", Type: "varchar"}, {Name: "AMOUNT", Type: "decimal"}, {Name: "FLAG", Type: "boolean"}}}}, Identity: func(context.Context) (Identity, error) {
		return AttestedIdentity(testAccount, "reader", "generation1", testCredentials), nil
	}, Timeout: 5 * time.Second, ImmediatePolling: true, Transport: &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots}, DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr != testHost+":443" {
			t.Error("foreign dial authority")
		}
		return dialer.DialContext(ctx, network, server.Listener.Addr().String())
	}}}
	if configure != nil {
		configure(&options)
	}
	c, e := New(options)
	if e != nil {
		server.Close()
		t.Fatal(e)
	}
	t.Cleanup(func() { closeSignal(f.stop); _ = c.Close(); server.Close() })
	return c, f
}
func query() dal.StructuredQuery {
	return dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where().SelectIntoRecordset()
}
func closeSignal(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}
func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("owned positive acknowledgement missing")
	}
}
func await(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case e := <-ch:
		return e
	case <-time.After(3 * time.Second):
		t.Fatal("bounded operation did not finish")
		return nil
	}
}
