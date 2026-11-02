package dalgo2athena

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	athenasdk "github.com/aws/aws-sdk-go-v2/service/athena"
)

func TestNativePointBatchAndConfiguredMetadata(t *testing.T) {
	c, f := newFixture(t, nil, nil)
	doc, e := c.Get(context.Background(), "rows", "9007199254740993")
	if e != nil {
		t.Fatal(e)
	}
	if doc == nil || doc.ID != "9007199254740993" || *doc.Data["AMOUNT"] != "9223372036854775807.000000001" || doc.Data["FLAG"] != nil || doc.Columns[1].Precision == nil || *doc.Columns[1].Precision != 38 {
		t.Fatal("native text/null metadata changed")
	}
	missing, e := c.Exists(context.Background(), "rows", "missing")
	if e != nil || missing {
		t.Fatal("missing native SELECT", e)
	}
	docs, e := c.GetMany(context.Background(), "rows", []string{"b", "missing", "a", "a"})
	if e != nil {
		t.Fatal(e)
	}
	if len(docs) != 4 || docs[0].ID != "b" || docs[1] != nil || docs[2].ID != "a" || docs[3].ID != "a" {
		t.Fatal("batch order/duplicates/missing")
	}
	*docs[2].Data["ID"] = "changed"
	if *docs[3].Data["ID"] != "a" {
		t.Fatal("duplicate data aliases")
	}
	before := len(f.snapshot())
	empty, e := c.GetMany(context.Background(), "rows", nil)
	if e != nil || len(empty) != 0 || len(f.snapshot()) != before {
		t.Fatal("empty batch dispatched", e)
	}
	meta := c.ConfiguredMetadata()
	table := meta.Tables["rows"]
	table.Columns[0].Name = "changed"
	delete(meta.Tables, "rows")
	if c.ConfiguredMetadata().Tables["rows"].Columns[0].Name != "ID" || meta.DynamicDiscovery {
		t.Fatal("metadata escaped ownership")
	}
	if f.count("StartQueryExecution") != 6 {
		t.Fatal("unexpected starts")
	}
}
func TestPublicSameQueryPagingHeaderOnceEmptyAndDuplicateRows(t *testing.T) {
	c, f := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, body map[string]any) bool {
		if op != "GetQueryResults" {
			return false
		}
		if body["MaxResults"] != json.Number("1") {
			t.Error("page size changed")
		}
		switch body["NextToken"] {
		case nil:
			writeJSON(t, w, 200, nativeRows([][]*string{header()}, "service-one", true))
		case "service-one":
			writeJSON(t, w, 200, nativeRows(nil, "service-two", false))
		case "service-two":
			writeJSON(t, w, 200, nativeRows([][]*string{header()}, "service-three", false))
		case "service-three":
			writeJSON(t, w, 200, nativeRows([][]*string{header()}, "service-four", true))
		case "service-four":
			writeJSON(t, w, 200, nativeRows([][]*string{{text("a"), text("")}}, "", false))
		default:
			t.Error("unexpected service token")
			w.WriteHeader(400)
		}
		return true
	}, nil)
	page, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 1})
	if e != nil {
		t.Fatal(e)
	}
	expiry := page.Receipt.ExpiresAt
	cursor := page.NextCursor
	all := [][]*string{}
	ordinal := 0
	for i := 0; ; i++ {
		if page.OrdinalStart != ordinal || !page.Receipt.ExpiresAt.Equal(expiry) {
			t.Fatal("ordinal or original expiry renewed")
		}
		all = append(all, page.Cells...)
		ordinal += len(page.Cells)
		if !page.HasMore {
			if page.NextCursor != "" || !page.Receipt.Complete || page.Receipt.RowsAcknowledged != 3 || page.Receipt.PagesFetched != 5 {
				t.Fatal("terminal receipt incorrect")
			}
			break
		}
		old := page.NextCursor
		page, e = c.NextPage(context.Background(), old)
		if e != nil {
			t.Fatal(e)
		}
		before := len(f.snapshot())
		if _, e = c.NextPage(context.Background(), old); e == nil || len(f.snapshot()) != before {
			t.Fatal("old cursor replay dispatched")
		}
		if i > 5 {
			t.Fatal("unbounded page loop")
		}
	}
	if len(all) != 3 || !reflect.DeepEqual(all[0], header()) || !reflect.DeepEqual(all[1], header()) || all[2][0] == nil || *all[2][0] != "a" || all[2][1] == nil || *all[2][1] != "" || all[2][2] != nil {
		t.Fatal("header/data duplicates/null/empty changed")
	}
	if f.count("StartQueryExecution") != 1 || f.count("GetQueryResults") != 5 {
		t.Fatal("paging resubmitted query")
	}
	before := len(f.snapshot())
	if _, e = c.NextPage(context.Background(), cursor); e == nil || len(f.snapshot()) != before {
		t.Fatal("consumed initial cursor regained authority")
	}
}
func TestPendingPageFailureRetriesSameNativeStageBeforeACK(t *testing.T) {
	c, f := newFixture(t, twoPageHook, nil)
	first, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	old := first.NextCursor
	_, e = c.NextPageWith(context.Background(), old, func(page NativePage) error { *page.Cells[0][0] = "MUTATED_COPY"; return errors.New("SECRET_CALLBACK") })
	if e == nil || strings.Contains(e.Error(), "SECRET") || f.count("GetQueryResults") != 2 {
		t.Fatal("failed delivery unsafe", e)
	}
	c.mu.Lock()
	j := c.handles[old]
	pending := j.pending != nil
	acked := j.ordinal
	retained := c.retainedBytes
	c.mu.Unlock()
	if !pending || acked != 1 || retained <= 1024 {
		t.Fatal("native stage/ordinal/quota not retained")
	}
	page, e := c.NextPage(context.Background(), old)
	if e != nil || page.OrdinalStart != 1 || *page.Cells[0][0] != "b" || page.HasMore || f.count("GetQueryResults") != 2 {
		t.Fatal("retry refetched/mutated/dropped pending row", e)
	}
	c.mu.Lock()
	bytes, nodes, jobs := c.retainedBytes, c.retainedNodes, len(c.journeys)
	c.mu.Unlock()
	if bytes != 0 || nodes != 0 || jobs != 0 {
		t.Fatal("terminal page leaked quota", bytes, nodes, jobs)
	}
}
func twoPageHook(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, body map[string]any) bool {
	if op != "GetQueryResults" {
		return false
	}
	if body["NextToken"] == nil {
		writeJSONDummy(w, nativeRows([][]*string{header(), {text("a"), text("exact"), nil}}, "second-service-token", true))
	} else {
		writeJSONDummy(w, nativeRows([][]*string{{text("b"), text("second"), nil}}, "", false))
	}
	return true
}
func writeJSONDummy(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	_ = json.NewEncoder(w).Encode(value)
}
func TestCursorSourcePrincipalAndForgeryRefuseWithoutHTTP(t *testing.T) {
	var changed atomic.Bool
	c, f := newFixture(t, twoPageHook, func(o *Options) {
		o.Identity = func(context.Context) (Identity, error) {
			generation := "generation1"
			if changed.Load() {
				generation = "generation2"
			}
			return AttestedIdentity(testAccount, "reader", generation, testCredentials), nil
		}
	})
	page, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	other, otherWire := newFixture(t, nil, nil)
	if _, e = other.NextPage(context.Background(), page.NextCursor); e == nil || len(otherWire.snapshot()) != 0 {
		t.Fatal("cross-client cursor admitted")
	}
	before := len(f.snapshot())
	if _, e = c.NextPage(context.Background(), Cursor("second-service-token")); e == nil || len(f.snapshot()) != before {
		t.Fatal("raw service token admitted")
	}
	changed.Store(true)
	if _, e = c.NextPage(context.Background(), page.NextCursor); e == nil || len(f.snapshot()) != before {
		t.Fatal("changed principal dispatched")
	}
	changed.Store(false)
	if _, e = c.NextPage(context.Background(), page.NextCursor); e != nil {
		t.Fatal("original authority could not retry", e)
	}
}

type sdkHTTP func(*http.Request) (*http.Response, error)

func (f sdkHTTP) Do(r *http.Request) (*http.Response, error) { return f(r) }
func TestOfficialSDKDatumNullEmptyAndAbsentWitness(t *testing.T) {
	// Actual pinned official serde, no AWS network or default credential chain.
	sdk := athenasdk.New(athenasdk.Options{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return testCredentials, nil }), Retryer: aws.NopRetryer{}, HTTPClient: sdkHTTP(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("X-Amz-Target") != "AmazonAthena.GetQueryResults" {
			t.Error("official wire target differs")
		}
		raw := `{"ResultSet":{"ResultSetMetadata":{"ColumnInfo":[{"Name":"a","Type":"varchar"}]},"Rows":[{"Data":[{}, {"VarCharValue":null},{"VarCharValue":""},{"VarCharValue":"null"}]}]}}`
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/x-amz-json-1.1"}}, Body: io.NopCloser(strings.NewReader(raw))}, nil
	})})
	result, e := sdk.GetQueryResults(context.Background(), &athenasdk.GetQueryResultsInput{QueryExecutionId: aws.String("owned-query")})
	if e != nil {
		t.Fatal(e)
	}
	values := result.ResultSet.Rows[0].Data
	if len(values) != 4 || values[0].VarCharValue != nil || values[1].VarCharValue != nil || values[2].VarCharValue == nil || *values[2].VarCharValue != "" || values[3].VarCharValue == nil || *values[3].VarCharValue != "null" {
		t.Fatal("official nullable text contract differs")
	}
}
func TestOfficialSDKOptionalRowsAndDataOmissionWitness(t *testing.T) {
	for _, sample := range []struct {
		name, raw string
		rows      int
	}{{"omitted-rows", `{"ResultSet":{"ResultSetMetadata":{"ColumnInfo":[{"Name":"a","Type":"varchar"}]}},"NextToken":"owned-next"}`, 0}, {"omitted-data", `{"ResultSet":{"ResultSetMetadata":{"ColumnInfo":[{"Name":"a","Type":"varchar"}]},"Rows":[{}]}}`, 1}} {
		t.Run(sample.name, func(t *testing.T) {
			sdk := athenasdk.New(athenasdk.Options{Region: "us-east-1", Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) { return testCredentials, nil }), Retryer: aws.NopRetryer{}, HTTPClient: sdkHTTP(func(r *http.Request) (*http.Response, error) {
				if r.Header.Get("X-Amz-Target") != "AmazonAthena.GetQueryResults" {
					t.Error("official wire target differs")
				}
				return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/x-amz-json-1.1"}}, Body: io.NopCloser(strings.NewReader(sample.raw))}, nil
			})})
			value, e := sdk.GetQueryResults(context.Background(), &athenasdk.GetQueryResultsInput{QueryExecutionId: aws.String("owned-query")})
			if e != nil || value.ResultSet == nil || len(value.ResultSet.Rows) != sample.rows {
				t.Fatal("official optional arrays differ", e)
			}
			if sample.rows == 0 {
				if value.NextToken == nil || *value.NextToken != "owned-next" {
					t.Fatal("omitted rows lost continuation")
				}
			} else if len(value.ResultSet.Rows[0].Data) != 0 {
				t.Fatal("omitted Data invented cells")
			}
		})
	}
}
func TestOptionalNativeArraysPreserveEmptyContinuationAndAllNullRow(t *testing.T) {
	c, f := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, body map[string]any) bool {
		if op != "GetQueryResults" {
			return false
		}
		if body["NextToken"] == nil {
			writeJSON(t, w, 200, map[string]any{"ResultSet": map[string]any{"ResultSetMetadata": schema()}, "NextToken": "omitted-rows-next"})
		} else {
			payload := nativeRows([][]*string{header(), nil, {text(""), text("null")}}, "", true)
			payload["ResultSet"].(map[string]any)["Rows"].([]any)[1] = map[string]any{}
			writeJSON(t, w, 200, payload)
		}
		return true
	}, nil)
	first, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 3})
	if e != nil || len(first.Cells) != 0 || !first.HasMore || first.OrdinalStart != 0 {
		t.Fatal("omitted Rows lost nonterminal page", e)
	}
	last, e := c.NextPage(context.Background(), first.NextCursor)
	if e != nil || last.HasMore || len(last.Cells) != 2 || last.OrdinalStart != 0 || last.Receipt.RowsAcknowledged != 2 || last.Cells[0][0] != nil || last.Cells[0][1] != nil || last.Cells[0][2] != nil || last.Cells[1][0] == nil || *last.Cells[1][0] != "" || last.Cells[1][1] == nil || *last.Cells[1][1] != "null" || last.Cells[1][2] != nil || f.count("StartQueryExecution") != 1 {
		t.Fatal("all-NULL/empty/literal-null fidelity changed", e)
	}
}
func TestPresentMalformedOptionalArraysAndMissingHeaderStillRefuseWhole(t *testing.T) {
	for _, sample := range []struct {
		name   string
		change func(map[string]any)
	}{{"rows-null", func(set map[string]any) { set["Rows"] = nil }}, {"rows-object", func(set map[string]any) { set["Rows"] = map[string]any{} }}, {"data-null", func(set map[string]any) { set["Rows"].([]any)[1].(map[string]any)["Data"] = nil }}, {"data-string", func(set map[string]any) { set["Rows"].([]any)[1].(map[string]any)["Data"] = "" }}, {"missing-header-data", func(set map[string]any) { set["Rows"] = []any{map[string]any{}} }}} {
		t.Run(sample.name, func(t *testing.T) {
			c, _ := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, _ map[string]any) bool {
				if op != "GetQueryResults" {
					return false
				}
				payload := nativeRows([][]*string{header(), {text("a")}}, "", true)
				sample.change(payload["ResultSet"].(map[string]any))
				writeJSON(t, w, 200, payload)
				return true
			}, nil)
			page, e := c.QueryPage(context.Background(), query(), ReadOptions{})
			if e == nil || len(page.Cells) != 0 || page.NextCursor != "" {
				t.Fatal("malformed present optional array/header delivered")
			}
		})
	}
}
