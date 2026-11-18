package dalgo2athena

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dal-go/dalgo/dal"
)

func TestClosedCompilerQuotesTypedParametersIdentityAndOffsetOnce(t *testing.T) {
	c, _ := newFixture(t, nil, func(o *Options) {
		table := o.Tables["rows"]
		table.Name = `table"; DROP TABLE x; --`
		table.Columns = append(table.Columns, Column{Name: "__name__", Type: "varchar"})
		o.Tables["rows"] = table
	})
	o, e := c.begin(context.Background(), "")
	if e != nil {
		t.Fatal(e)
	}
	defer o.finish()
	q := dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where(dal.NewGroupCondition(dal.And, dal.Comparison{Left: dal.DocumentID(), Operator: dal.Equal, Right: dal.Constant{Value: "x' OR 1=1 --"}}, dal.Comparison{Left: dal.Field("AMOUNT"), Operator: dal.GreaterOrEqual, Right: dal.Constant{Value: json.Number("12345678901234567890.0001")}}, dal.Comparison{Left: dal.Field("__name__"), Operator: dal.Operator("!="), Right: dal.Constant{Value: nil}}, dal.Comparison{Left: dal.Field("FLAG"), Operator: dal.Equal, Right: dal.Constant{Value: true}})).OrderBy(dal.DescendingField("AMOUNT")).Offset(7).Limit(2).SelectIntoRecordset()
	p, e := c.compile(o, q, false)
	if e != nil {
		t.Fatal(e)
	}
	for _, part := range []string{`FROM "AwsDataCatalog"."native"."table""; DROP TABLE x; --"`, `"ID" = ?`, `"__name__" IS NOT NULL`, `ORDER BY "AMOUNT" DESC NULLS LAST, "ID" ASC NULLS LAST OFFSET 7 ROWS LIMIT 2`} {
		if !strings.Contains(p.sql, part) {
			t.Fatal("closed SQL differs", part)
		}
	}
	if strings.Contains(p.sql, "OR 1=1") || len(p.parameters) != 3 || p.parameters[0] != "'x'' OR 1=1 --'" || p.parameters[1] != "12345678901234567890.0001" || p.parameters[2] != "TRUE" {
		t.Fatal("unsafe parameter conversion")
	}
}

type forbiddenCondition struct{}

func (forbiddenCondition) String() string { panic("arbitrary condition must never render") }
func TestCompilerRefusesUnknownUnboundedOrMalformedBeforeCredentials(t *testing.T) {
	var auth atomic.Int64
	c, f := newFixture(t, nil, func(o *Options) {
		o.Identity = func(context.Context) (Identity, error) {
			auth.Add(1)
			return AttestedIdentity(testAccount, "reader", "generation1", testCredentials), nil
		}
	})
	cases := []dal.Query{dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where(forbiddenCondition{}).SelectIntoRecordset(), dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().WhereField("AMOUNT", dal.In, []any{}).SelectIntoRecordset(), dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().WhereField("AMOUNT", dal.In, []any{nil}).SelectIntoRecordset(), dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().WhereField("AMOUNT", dal.Equal, math.Inf(1)).SelectIntoRecordset(), dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().WhereField("AMOUNT", dal.Equal, strings.Repeat("<", 1024)).SelectIntoRecordset(), dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where().GroupBy(dal.Field("ID")).SelectIntoRecordset(), dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where().Offset(100001).SelectIntoRecordset(), dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where().Limit(-1).SelectIntoRecordset(), dal.From(dal.NewRootCollectionRef("rows", "")).NewQuery().Where().StartAfter("foreign-cursor").SelectIntoRecordset()}
	for i, q := range cases {
		if page, e := c.QueryPage(context.Background(), q, ReadOptions{}); e == nil || len(page.Cells) > 0 || page.NextCursor != "" {
			t.Fatal("invalid query yielded authority", i)
		}
	}
	if auth.Load() != 0 || len(f.snapshot()) != 0 {
		t.Fatal("refusal invoked auth or wire")
	}
}

type cancellingQuery struct {
	dal.StructuredQuery
	cancel context.CancelFunc
	later  *atomic.Bool
}

func (q cancellingQuery) Columns() []dal.Column { q.cancel(); return nil }
func (q cancellingQuery) Where() dal.Condition  { q.later.Store(true); return q.StructuredQuery.Where() }
func TestCompilerCallbackCancellationPreventsLaterGetterAndDispatch(t *testing.T) {
	c, f := newFixture(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var later atomic.Bool
	page, e := c.QueryPage(ctx, cancellingQuery{query(), cancel, &later}, ReadOptions{})
	if !errors.Is(e, context.Canceled) || len(page.Cells) != 0 || later.Load() || len(f.snapshot()) != 0 {
		t.Fatal("cancelled snapshot crossed callback boundary", e)
	}
}
func TestOrdinalTablesNeverInventCoreKeys(t *testing.T) {
	c, f := newFixture(t, nil, func(o *Options) { table := o.Tables["rows"]; table.IDColumn = ""; o.Tables["rows"] = table })
	if reader, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), query()); reader != nil || !errors.Is(e, dal.ErrNotSupported) || len(f.snapshot()) != 0 {
		t.Fatal("ordinal query invented key", e)
	}
	if _, e := c.Get(context.Background(), "rows", "a"); !errors.Is(e, dal.ErrNotSupported) {
		t.Fatal("unconfigured key accepted", e)
	}
	result, e := c.Query(context.Background(), query())
	if e != nil || len(result.Cells) != 1 {
		t.Fatal("ordinal source not queryable", e)
	}
}
