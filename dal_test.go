package dalgo2athena

import (
	"context"
	"errors"
	"net/http"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

type callbackRecord struct {
	record.Record
	key  func() *record.Key
	data func() any
	set  func(error)
}

func (r *callbackRecord) Key() *record.Key {
	if r.key != nil {
		return r.key()
	}
	return r.Record.Key()
}
func (r *callbackRecord) Data() any {
	if r.data != nil {
		return r.data()
	}
	return r.Record.Data()
}
func (r *callbackRecord) SetError(e error) record.Record {
	r.Record.SetError(e)
	if r.set != nil {
		r.set(e)
	}
	return r
}
func TestDALNativeMapsMissingAndCopiedBatchKeys(t *testing.T) {
	c, _ := newFixture(t, nil, nil)
	aData, bData := map[string]any{}, map[string]any{}
	a := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("rows", "a"), &aData)}
	b := record.NewRecordWithData(record.NewKeyWithID("rows", "b"), &bData)
	replacementData := map[string]any{}
	replacement := record.NewRecordWithData(record.NewKeyWithID("rows", "replacement"), &replacementData)
	records := []record.Record{a, b}
	a.set = func(e error) {
		if e == nil {
			records[1] = replacement
			b.Key().ID = "changed"
		}
	}
	if e := c.DB().GetMulti(context.Background(), records); e != nil {
		t.Fatal(e)
	}
	if aData["ID"] != "a" || bData["ID"] != "b" || aData["FLAG"] != nil || aData["AMOUNT"] != "9223372036854775807.000000001" || len(replacementData) != 0 {
		t.Fatal("core map/native snapshot changed")
	}
	missing := record.NewRecordWithData(record.NewKeyWithID("rows", "missing"), &map[string]any{})
	if e := c.DB().Get(context.Background(), missing); e == nil || !record.IsNotFound(e) || missing.Exists() {
		t.Fatal("ordinary not-found state", e)
	}
}
func TestDALSetErrorCancellationNeverCallsDataOrReturnsSuccess(t *testing.T) {
	c, _ := newFixture(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var data atomic.Bool
	target := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("rows", "a"), &map[string]any{}), set: func(e error) {
		if e == nil {
			cancel()
		}
	}, data: func() any { data.Store(true); return nil }}
	if e := c.DB().Get(ctx, target); !errors.Is(e, context.Canceled) || data.Load() || !errors.Is(target.Error(), context.Canceled) {
		t.Fatal("SetError boundary passed cancellation", e)
	}
}
func TestDALDataCancellationAndFinalClassificationCallerPriority(t *testing.T) {
	c, _ := newFixture(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	data := map[string]any{}
	target := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("rows", "a"), &data), data: func() any { cancel(); return &data }}
	if e := c.DB().Get(ctx, target); !errors.Is(e, context.Canceled) || len(data) != 0 || !errors.Is(target.Error(), context.Canceled) {
		t.Fatal("Data cancellation followed by mutation", e)
	}
	other, _ := newFixture(t, nil, nil)
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	state := map[string]any{}
	var closed, caller atomic.Bool
	r := &callbackRecord{Record: record.NewRecordWithData(record.NewKeyWithID("rows", "a"), &state)}
	r.set = func(e error) {
		if e == nil && state["ID"] == "a" {
			closed.Store(true)
			_ = other.Close()
		}
		var problem *Error
		if errors.As(e, &problem) && problem.Code == "closed" {
			caller.Store(true)
			stop()
		}
	}
	if e := other.DB().Get(parent, r); !errors.Is(e, context.Canceled) || !closed.Load() || !caller.Load() || !errors.Is(r.Error(), context.Canceled) {
		t.Fatal("final classification callback lost caller priority", e)
	}
}
func TestCoreReadersZeroMultipleRowsNullableAndIndependentColumns(t *testing.T) {
	for _, size := range []int{0, 2} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			want := [][]*string{}
			if size == 2 {
				want = [][]*string{{text("a"), text("9223372036854775807.000000001"), nil}, {text("b"), nil, text("null")}}
			}
			c, _ := newFixture(t, func(_ *fixture, w http.ResponseWriter, _ *http.Request, op string, _ map[string]any) bool {
				if op != "GetQueryResults" {
					return false
				}
				writeJSON(t, w, 200, nativeRows(append([][]*string{header()}, want...), "", true))
				return true
			}, nil)
			first, e := c.DB().ExecuteQueryToRecordsetReader(context.Background(), query(), recordset.WithName("observed"))
			if e != nil {
				t.Fatal(e)
			}
			defer first.Close()
			second, e := c.DB().ExecuteQueryToRecordsetReader(context.Background(), query())
			if e != nil {
				t.Fatal(e)
			}
			defer second.Close()
			rs := first.Recordset()
			if rs.RowsCount() != size || rs.ColumnsCount() != 3 || rs.Name() != "observed" || rs.GetColumnByName("AMOUNT").DbType() != "decimal" {
				t.Fatal("native recordset metadata changed")
			}
			for _, col := range rs.Columns() {
				if len(col.Values()) != size {
					t.Fatal("nil wrapper prevented row allocation")
				}
			}
			for i, expected := range want {
				row, set, e := first.Next()
				if e != nil {
					t.Fatal(e)
				}
				got, e := row.Data(set)
				if e != nil {
					t.Fatal(e)
				}
				values := make([]any, len(expected))
				for j, v := range expected {
					if v != nil {
						values[j] = *v
					}
				}
				if !reflect.DeepEqual(got, values) {
					t.Fatal("native cells coerced", i)
				}
			}
			if row, _, e := first.Next(); row != nil || !errors.Is(e, dal.ErrNoMoreRecords) {
				t.Fatal("incorrect native EOF", e)
			}
			columns := first.(interface{ Columns() []Column }).Columns()
			if columns[1].Precision == nil || *columns[1].Precision != 38 || columns[1].Scale == nil || *columns[1].Scale != 9 || columns[1].Nullable != "UNKNOWN" {
				t.Fatal("precision/nullable labels changed")
			}
			*columns[1].Precision = 1
			if *second.(interface{ Columns() []Column }).Columns()[1].Precision != 38 {
				t.Fatal("descriptor ownership aliased")
			}
			if size > 0 {
				if e = rs.GetRow(0).SetValueByName("AMOUNT", "changed", rs); e != nil {
					t.Fatal(e)
				}
				row, set, e := second.Next()
				if e != nil {
					t.Fatal(e)
				}
				values, e := row.Data(set)
				if e != nil || values[1] != "9223372036854775807.000000001" {
					t.Fatal("independent reader backing aliased", e)
				}
			}
		})
	}
}
func TestReaderFactoryAndLifetimeCancellationHaveNoAcknowledgedPrefix(t *testing.T) {
	c, _ := newFixture(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	var factory atomic.Bool
	q := dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where().SelectIntoRecord(func() record.Record { factory.Store(true); cancel(); return nil })
	reader, e := c.DB().ExecuteQueryToRecordsReader(ctx, q)
	if reader != nil || !errors.Is(e, context.Canceled) || !factory.Load() {
		t.Fatal("factory cancellation opened reader", e)
	}
	waitIdle(t, c)
	parent, stop := context.WithCancel(context.Background())
	reader, e = c.DB().ExecuteQueryToRecordsReader(parent, query())
	if e != nil {
		t.Fatal(e)
	}
	defer reader.Close()
	stop()
	if row, e := reader.Next(); row != nil || !errors.Is(e, context.Canceled) {
		t.Fatal("cancelled reader yielded row", e)
	}
}
func TestShutdownRequiresActualReaderCloseAndNeverFakeQuiescence(t *testing.T) {
	c, _ := newFixture(t, nil, nil)
	reader, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), query())
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if e = c.Shutdown(ctx); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatal("unclosed logical reader forced away", e)
	}
	if e = reader.Close(); e != nil {
		t.Fatal(e)
	}
	if e = c.Shutdown(context.Background()); e != nil {
		t.Fatal(e)
	}
}
