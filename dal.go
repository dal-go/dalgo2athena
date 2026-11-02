package dalgo2athena

import (
	"context"
	"errors"
	"reflect"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

type backend struct {
	dal.NoConcurrency
	client *Client
}

var _ dal.Backend = (*backend)(nil)

func (c *Client) DB() dal.DB            { return dal.NewDB(&backend{client: c}) }
func (b *backend) ID() string           { return b.client.options.Database }
func (b *backend) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2athena", "0.1.0") }
func (b *backend) Schema() dal.Schema   { return nil }
func (*backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (*backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}

func contextRank(e error) int {
	if errors.Is(e, context.Canceled) {
		return 3
	}
	if errors.Is(e, context.DeadlineExceeded) {
		return 2
	}
	var x *Error
	if errors.As(e, &x) && x.Code == "closed" {
		return 1
	}
	return 0
}

// Classification can only promote closed -> deadline -> caller cancellation.
// Each SetError is checked, including the callback used to reclassify another
// error. No unguarded final restoration callback follows this loop.
func setRecord(o *operation, r record.Record, e error) error {
	if final := o.check(); contextRank(final) > contextRank(e) {
		e = final
	}
	for i := 0; i < 4; i++ {
		r.SetError(e)
		final := o.check()
		if final != nil && contextRank(final) > contextRank(e) {
			e = final
			continue
		}
		if final != nil {
			return final
		}
		return e
	}
	return e
}
func finishRecords(o *operation, records []record.Record, e error) error {
	for _, r := range records {
		if r == nil {
			continue
		}
		if final := setRecord(o, r, e); final != nil && contextRank(final) > contextRank(e) {
			e = final
		}
	}
	if final := o.check(); final != nil {
		return final
	}
	return e
}
func identity(o *operation, r record.Record) (*record.Key, string, string, error) {
	if r == nil {
		return nil, "", "", fail("target")
	}
	key, e := observe(o, r.Key)
	if e != nil {
		return nil, "", "", e
	}
	if key == nil || key.Parent() != nil {
		return nil, "", "", dal.ErrNotSupported
	}
	id, ok := key.ID.(string)
	if !ok || id == "" || key.Validate() != nil {
		return nil, "", "", dal.ErrNotSupported
	}
	collection := key.Collection()
	return record.NewKeyWithID(collection, id), collection, id, nil
}
func (b *backend) Exists(ctx context.Context, key *record.Key) (bool, error) {
	o, e := b.client.begin(ctx, "")
	if e != nil {
		return false, e
	}
	defer func() { o.client.mu.Lock(); o.client.retireLocked(o.j); o.client.mu.Unlock(); o.finish() }()
	if key == nil || key.Parent() != nil {
		return false, dal.ErrNotSupported
	}
	id, ok := key.ID.(string)
	if !ok {
		return false, dal.ErrNotSupported
	}
	p, e := b.client.pointPlan(o, key.Collection(), id)
	if e != nil {
		return false, e
	}
	row, e := b.client.point(o, p, id)
	if final := o.check(); final != nil {
		return false, final
	}
	return row != nil, e
}
func (b *backend) Get(ctx context.Context, r record.Record) error {
	return b.GetMulti(ctx, []record.Record{r})
}
func (b *backend) GetMulti(ctx context.Context, records []record.Record) error {
	o, e := b.client.begin(ctx, "")
	if e != nil {
		return e
	}
	defer func() { o.client.mu.Lock(); o.client.retireLocked(o.j); o.client.mu.Unlock(); o.finish() }()
	if len(records) > b.client.options.MaxKeys {
		return fail("bounds")
	}
	records = append([]record.Record(nil), records...)
	keys := make([]*record.Key, len(records))
	ids := make([]string, len(records))
	plans := make([]plan, len(records))
	cost := 0
	for i, r := range records {
		key, collection, id, e := identity(o, r)
		if e != nil {
			return finishRecords(o, records, e)
		}
		keys[i] = key
		ids[i] = id
		p, e := b.client.pointPlan(o, collection, id)
		if e != nil {
			return finishRecords(o, records, e)
		}
		bodySize, e := o.pointCost(p)
		if e != nil {
			return finishRecords(o, records, e)
		}
		if bodySize > b.client.options.MaxRequestBytes-cost {
			return finishRecords(o, records, fail("bounds"))
		}
		cost += bodySize
		plans[i] = p
	}
	var missing error
	rows := make([]*Document, len(records))
	for i, id := range ids {
		rows[i], e = b.client.point(o, plans[i], id)
		if e != nil {
			return finishRecords(o, records, e)
		}
	}
	for i, r := range records {
		if e := o.check(); e != nil {
			return finishRecords(o, records, e)
		}
		if rows[i] == nil {
			notFound := dal.NewErrNotFoundByKey(keys[i], nil)
			if missing == nil {
				missing = notFound
			}
			r.SetError(notFound)
			if e := o.check(); e != nil {
				return finishRecords(o, records, e)
			}
			continue
		}
		if e := setRecord(o, r, nil); e != nil {
			return finishRecords(o, records, e)
		}
		data, e := observe(o, r.Data)
		if e != nil {
			return finishRecords(o, records, e)
		}
		if e = mapTarget(data, rows[i].Data); e != nil {
			return finishRecords(o, records, e)
		}
		if e = o.check(); e != nil {
			return finishRecords(o, records, e)
		}
		if e = setRecord(o, r, nil); e != nil {
			return finishRecords(o, records, e)
		}
	}
	if e := o.check(); e != nil {
		return e
	}
	return missing
}

// Explicit native maps and Row targets avoid JSON/float/struct coercion. Struct
// codecs are not implemented by this initial native STRING/null read facade.
func mapTarget(target any, row Row) error {
	switch t := target.(type) {
	case *Row:
		if t == nil {
			return fail("target")
		}
		*t = copyRow(row)
		return nil
	case Row:
		if t == nil {
			return fail("target")
		}
		clear(t)
		for k, v := range copyRow(row) {
			t[k] = v
		}
		return nil
	case *map[string]any:
		if t == nil {
			return fail("target")
		}
		m := make(map[string]any, len(row))
		for k, v := range row {
			if v == nil {
				m[k] = nil
			} else {
				m[k] = *v
			}
		}
		*t = m
		return nil
	case map[string]any:
		if t == nil {
			return fail("target")
		}
		clear(t)
		for k, v := range row {
			if v == nil {
				t[k] = nil
			} else {
				t[k] = *v
			}
		}
		return nil
	default:
		return fail("target")
	}
}

type recordReader struct {
	op                   *operation
	collection, idColumn string
	result               *Result
	index                int
	closed               bool
	factory              func() record.Record
	first                record.Record
}

var _ dal.RecordsReader = (*recordReader)(nil)

type identifiedRecord struct {
	record.Record
	key *record.Key
}

func (r identifiedRecord) Key() *record.Key     { return r.key }
func (r *recordReader) Columns() []Column       { return cloneColumns(r.result.Columns) }
func (r *recordReader) Cursor() (string, error) { return "", dal.ErrNotSupported }
func (r *recordReader) Close() error {
	if !r.closed {
		r.closed = true
		r.op.client.mu.Lock()
		r.op.client.retireLocked(r.op.j)
		r.op.client.mu.Unlock()
		r.op.finish()
		r.first = nil
		r.result.Cells = nil
	}
	return nil
}
func (r *recordReader) Next() (record.Record, error) {
	if r.closed {
		return nil, fail("closed")
	}
	if e := r.op.check(); e != nil {
		return nil, e
	}
	if r.index >= len(r.result.Cells) {
		return nil, dal.ErrNoMoreRecords
	}
	row := rowMap(r.result.Columns, r.result.Cells[r.index])
	id := row[r.idColumn]
	if id == nil || *id == "" {
		return nil, fail("identity")
	}
	var target record.Record
	var e error
	if r.index == 0 {
		target = r.first
	} else if r.factory != nil {
		target, e = observe(r.op, r.factory)
		if e != nil {
			return nil, e
		}
	}
	key := record.NewKeyWithID(r.collection, *id)
	if key.Validate() != nil {
		return nil, dal.ErrNotSupported
	}
	if target == nil {
		data := make(map[string]any)
		target = record.NewRecordWithData(key, &data)
	}
	if e = setRecord(r.op, target, nil); e != nil {
		return nil, e
	}
	data, e := observe(r.op, target.Data)
	if e != nil {
		return nil, e
	}
	if e = mapTarget(data, row); e != nil {
		return nil, setRecord(r.op, target, e)
	}
	if e = setRecord(r.op, target, nil); e != nil {
		return nil, e
	}
	if e = r.op.check(); e != nil {
		return nil, e
	}
	result := identifiedRecord{Record: target, key: key}
	r.index++
	r.first = nil
	return result, nil
}
func (b *backend) ExecuteQueryToRecordsReader(ctx context.Context, query dal.Query) (dal.RecordsReader, error) {
	o, e := b.client.begin(ctx, "")
	if e != nil {
		return nil, e
	}
	success := false
	defer func() {
		if !success {
			o.stop()
			o.client.mu.Lock()
			o.client.retireLocked(o.j)
			o.client.mu.Unlock()
			o.finish()
		}
	}()
	p, e := b.client.compile(o, query, true)
	if e != nil {
		return nil, e
	}
	q := query.(dal.StructuredQuery)
	kind, e := observe(o, q.IDKind)
	if e != nil {
		return nil, e
	}
	if kind != reflect.Invalid && kind != reflect.String {
		return nil, dal.ErrNotSupported
	}
	result, e := b.client.runPlan(o, p)
	if e != nil {
		return nil, e
	}
	r := &recordReader{op: o, collection: p.collection, idColumn: p.table.IDColumn, result: result, factory: q.IntoRecord}
	if len(result.Cells) > 0 {
		r.first, e = observe(o, r.factory)
		if e != nil {
			return nil, e
		}
	}
	if e = o.check(); e != nil {
		return nil, e
	}
	success = true
	return r, nil
}

type recordsetReader struct {
	op           *operation
	rs           recordset.Recordset
	columns      []Column
	index, total int
	closed       bool
}

var _ dal.RecordsetReader = (*recordsetReader)(nil)

func (r *recordsetReader) Columns() []Column              { return cloneColumns(r.columns) }
func (r *recordsetReader) Recordset() recordset.Recordset { return r.rs }
func (r *recordsetReader) Cursor() (string, error)        { return "", dal.ErrNotSupported }
func (r *recordsetReader) Close() error {
	if !r.closed {
		r.closed = true
		r.op.client.mu.Lock()
		r.op.client.retireLocked(r.op.j)
		r.op.client.mu.Unlock()
		r.op.finish()
	}
	return nil
}
func (r *recordsetReader) Next() (recordset.Row, recordset.Recordset, error) {
	if r.closed {
		return nil, nil, fail("closed")
	}
	if e := r.op.check(); e != nil {
		return nil, nil, e
	}
	if r.index >= r.total {
		return nil, r.rs, dal.ErrNoMoreRecords
	}
	row := r.rs.GetRow(r.index)
	if e := r.op.check(); e != nil {
		return nil, nil, e
	}
	r.index++
	return row, r.rs, nil
}
func (b *backend) ExecuteQueryToRecordsetReader(ctx context.Context, query dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	o, e := b.client.begin(ctx, "")
	if e != nil {
		return nil, e
	}
	success := false
	defer func() {
		if !success {
			o.stop()
			o.client.mu.Lock()
			o.client.retireLocked(o.j)
			o.client.mu.Unlock()
			o.finish()
		}
	}()
	p, e := b.client.compile(o, query, false)
	if e != nil {
		return nil, e
	}
	recordsetName := p.collection
	if len(options) > 32 {
		return nil, fail("bounds")
	}
	options = append([]recordset.Option(nil), options...)
	for _, option := range options {
		if option == nil {
			return nil, fail("input")
		}
		if e = o.check(); e != nil {
			return nil, e
		}
		resolved := recordset.NewOptions(option)
		if e = o.check(); e != nil {
			return nil, e
		}
		recordsetName = resolved.Name()
	}
	// Direct typed any columns accept nil. Core0.97 NewColumn wraps any in a
	// type-asserting adapter that rejects Add(nil); NewRow ignores that error.
	result, e := b.client.runPlan(o, p)
	if e != nil {
		return nil, e
	}
	cols := make([]recordset.Column[any], len(result.Columns))
	for i, c := range result.Columns {
		cols[i] = recordset.NewTypedColumn[any](c.Name, nil, recordset.ColDbType(c.Type))
	}
	rs := recordset.NewColumnarRecordset(recordsetName, cols...)
	for _, data := range result.Cells {
		if e = o.check(); e != nil {
			return nil, e
		}
		row := rs.NewRow()
		for i := range result.Columns {
			var value any
			if v := data[i]; v != nil {
				value = *v
			}
			if e = row.SetValueByIndex(i, value, rs); e != nil {
				return nil, fail("target")
			}
		}
	}
	if e = o.check(); e != nil {
		return nil, e
	}
	success = true
	return &recordsetReader{op: o, rs: rs, columns: cloneColumns(result.Columns), total: len(result.Cells)}, nil
}
