package dalgo2athena

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestWorkgroupPolicyRefusalsNeverSubmit(t *testing.T) {
	cases := map[string]func(map[string]any){"disabled": func(w map[string]any) { w["State"] = "DISABLED" }, "not-enforced": func(w map[string]any) { w["Configuration"].(map[string]any)["EnforceWorkGroupConfiguration"] = false }, "small-cutoff": func(w map[string]any) { w["Configuration"].(map[string]any)["BytesScannedCutoffPerQuery"] = 9999999 }, "large-cutoff": func(w map[string]any) { w["Configuration"].(map[string]any)["BytesScannedCutoffPerQuery"] = 10000001 }, "managed": func(w map[string]any) {
		w["Configuration"].(map[string]any)["ManagedQueryResultsConfiguration"] = map[string]any{"Enabled": true}
	}, "wrong-managed-type": func(w map[string]any) {
		w["Configuration"].(map[string]any)["ManagedQueryResultsConfiguration"] = "false"
	}, "spark": func(w map[string]any) {
		w["Configuration"].(map[string]any)["EngineVersion"] = map[string]any{"EffectiveEngineVersion": "PySpark engine version 3"}
	}, "identity-center": func(w map[string]any) {
		w["Configuration"].(map[string]any)["IdentityCenterConfiguration"] = map[string]any{"EnableIdentityCenter": true}
	}, "output": func(w map[string]any) {
		w["Configuration"].(map[string]any)["ResultConfiguration"].(map[string]any)["OutputLocation"] = "s3://foreign/"
	}, "owner": func(w map[string]any) {
		w["Configuration"].(map[string]any)["ResultConfiguration"].(map[string]any)["ExpectedBucketOwner"] = "000000000000"
	}}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			c, f := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, _ map[string]any) bool {
				if op != "GetWorkGroup" {
					return false
				}
				value := workgroup()
				change(value["WorkGroup"].(map[string]any))
				writeJSON(t, w, 200, value)
				return true
			}, nil)
			if page, e := c.QueryPage(context.Background(), query(), ReadOptions{}); e == nil || page.NextCursor != "" || len(page.Cells) > 0 || f.count("StartQueryExecution") != 0 || f.count("GetWorkGroup") != 1 {
				t.Fatal("unsafe workgroup submitted", e)
			}
		})
	}
}
func TestStartFailureIsSingleAttemptAndNeverAdoptsForeignID(t *testing.T) {
	for _, status := range []int{301, 403, 429, 500, 503} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			c, f := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, _ map[string]any) bool {
				if op != "StartQueryExecution" {
					return false
				}
				w.Header().Set("Location", "https://foreign.invalid/SECRET")
				writeJSON(t, w, status, map[string]any{"message": "SECRET_SQL_AND_TOKEN", "QueryExecutionId": "foreign-id"})
				return true
			}, nil)
			page, e := c.QueryPage(context.Background(), query(), ReadOptions{})
			if e == nil || strings.Contains(e.Error(), "SECRET") || page.NextCursor != "" || f.count("StartQueryExecution") != 1 || f.count("GetQueryExecution") != 0 || f.count("StopQueryExecution") != 0 {
				t.Fatal("unknown submit retried/adopted or leaked", e)
			}
		})
	}
}
func TestWholeNativePageRejectsWrongShapeWidthHeaderAndSchema(t *testing.T) {
	cases := map[string]func(map[string]any){"width": func(p map[string]any) {
		p["ResultSet"].(map[string]any)["Rows"] = []any{map[string]any{"Data": []any{map[string]any{"VarCharValue": "ID"}, map[string]any{"VarCharValue": "AMOUNT"}, map[string]any{"VarCharValue": "FLAG"}, map[string]any{}}}}
	}, "wrong-header": func(p map[string]any) {
		p["ResultSet"].(map[string]any)["Rows"] = []any{map[string]any{"Data": []any{map[string]any{"VarCharValue": "wrong"}}}}
	}, "numeric-cell": func(p map[string]any) {
		p["ResultSet"].(map[string]any)["Rows"].([]any)[1].(map[string]any)["Data"].([]any)[1] = map[string]any{"VarCharValue": 9007199254740993}
	}, "metadata": func(p map[string]any) {
		p["ResultSet"].(map[string]any)["ResultSetMetadata"].(map[string]any)["ColumnInfo"].([]any)[0].(map[string]any)["Name"] = "changed"
	}, "empty-token": func(p map[string]any) { p["NextToken"] = "" }, "oversize-token": func(p map[string]any) { p["NextToken"] = strings.Repeat("x", 1025) }}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			c, _ := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, _ map[string]any) bool {
				if op != "GetQueryResults" {
					return false
				}
				payload := nativeRows([][]*string{header(), {text("a"), text("exact"), nil}}, "", true)
				change(payload)
				writeJSON(t, w, 200, payload)
				return true
			}, nil)
			page, e := c.QueryPage(context.Background(), query(), ReadOptions{})
			if e == nil || len(page.Cells) > 0 || page.NextCursor != "" {
				t.Fatal("malformed full page delivered")
			}
		})
	}
}
func TestRepeatedTokenRefusalPreservesPriorAuthorityAndWork(t *testing.T) {
	var progressing atomic.Bool
	c, f := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, body map[string]any) bool {
		if op != "GetQueryResults" {
			return false
		}
		if body["NextToken"] == nil {
			writeJSON(t, w, 200, nativeRows([][]*string{header()}, "same-token", true))
		} else if progressing.Load() {
			writeJSON(t, w, 200, nativeRows([][]*string{{text("a")}}, "", false))
		} else {
			writeJSON(t, w, 200, nativeRows(nil, "same-token", false))
		}
		return true
	}, nil)
	first, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 1})
	if e != nil {
		t.Fatal(e)
	}
	_, e = c.NextPage(context.Background(), first.NextCursor)
	if e == nil {
		t.Fatal("repeated token accepted")
	}
	var problem *Error
	if !errors.As(e, &problem) || problem.Receipt().RowsAcknowledged != 0 || problem.Receipt().PagesFetched != 2 {
		t.Fatal("failure lost work/ACK receipt", e)
	}
	progressing.Store(true)
	last, e := c.NextPage(context.Background(), first.NextCursor)
	if e != nil || last.OrdinalStart != 0 || len(last.Cells) != 1 || last.HasMore || f.count("StartQueryExecution") != 1 || f.count("GetQueryResults") != 3 {
		t.Fatal("refusal consumed old authority", e)
	}
}
func TestLaterPageLimitsAreIncompleteNeverTruncated(t *testing.T) {
	q := dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where().Limit(1).SelectIntoRecordset()
	c, f := newFixture(t, twoPageHook, nil)
	first, e := c.QueryPage(context.Background(), q, ReadOptions{PageSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	page, e := c.NextPage(context.Background(), first.NextCursor)
	var problem *Error
	if page.NextCursor != "" || len(page.Cells) > 0 || !errors.As(e, &problem) || problem.Receipt().Complete || problem.Receipt().RowsAcknowledged != 1 || problem.Receipt().RowsFetched != 2 || f.count("StartQueryExecution") != 1 {
		t.Fatal("later overflow truncated or erased ACKed rows", e)
	}
}
func TestStrictJSONRejectsDuplicateUnicodeTrailingDepthAndNodes(t *testing.T) {
	for _, raw := range []string{`{"x":1,"x":2}`, `{"x":"\ud800"}`, `{} {}`, strings.Repeat("[", 18) + "0" + strings.Repeat("]", 18)} {
		if _, _, e := strictJSON([]byte(raw), 100, func() error { return nil }); e == nil {
			t.Fatal("malformed JSON accepted")
		}
	}
	if _, _, e := strictJSON([]byte(`[1,2,3]`), 2, func() error { return nil }); e == nil {
		t.Fatal("nodes unbounded")
	}
	value, _, e := strictJSON([]byte(`{"__proto__":null,"wide":9223372036854775807}`), 10, func() error { return nil })
	if e != nil || value.(map[string]any)["wide"] != json.Number("9223372036854775807") {
		t.Fatal("native metadata parser coerced number", e)
	}
}
