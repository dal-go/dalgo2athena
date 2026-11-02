package dalgo2athena

import (
	"context"
	"reflect"
	"strconv"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

func nativeSize(columns []Column, cells [][]*string) (int, int) {
	nodes := 8 + 12*len(columns) + len(cells)
	bytes := nodes * 32
	for _, col := range columns {
		for _, s := range []string{col.Name, col.Label, col.Type, col.Nullable, col.CatalogName, col.SchemaName, col.TableName} {
			bytes += len(s)
		}
	}
	for _, row := range cells {
		nodes += len(row)
		bytes += 32 * len(row)
		for _, v := range row {
			if v != nil {
				bytes += len(*v)
			}
		}
	}
	return bytes, nodes
}
func (o *operation) reservePage() error {
	if e := o.check(); e != nil {
		return e
	}
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	j := o.j
	if j.pending != nil || j.reserveBytes != 0 {
		return nil
	}
	if j.pages >= c.options.MaxPages {
		return fail("bounds")
	}
	bytes, nodes := pageReservation(c.options)
	if e := c.adjustLocked(j, bytes, nodes); e != nil {
		return e
	}
	j.reserveBytes = bytes
	j.reserveNodes = nodes
	return nil
}
func decodeColumns(v any, expected []string, max int) ([]Column, error) {
	values, ok := v.([]any)
	if !ok || len(values) == 0 || len(values) > max || len(values) != len(expected) {
		return nil, fail("protocol")
	}
	out := make([]Column, len(values))
	seen := map[string]bool{}
	for i, value := range values {
		m, e := object(value)
		if e != nil {
			return nil, e
		}
		col := Column{}
		for key, target := range map[string]*string{"Name": &col.Name, "Label": &col.Label, "Type": &col.Type, "Nullable": &col.Nullable, "CatalogName": &col.CatalogName, "SchemaName": &col.SchemaName, "TableName": &col.TableName} {
			if v, present := m[key]; present {
				if v == nil {
					continue
				}
				s, ok := v.(string)
				if !ok || len(s) > 4096 || !utf8.ValidString(s) {
					return nil, fail("protocol")
				}
				*target = s
			}
		}
		if col.Name != expected[i] || !name(col.Name) || !name(col.Type) || seen[col.Name] {
			return nil, fail("schema")
		}
		seen[col.Name] = true
		col.Precision, e = optionalInt(m, "Precision")
		if e != nil {
			return nil, e
		}
		col.Scale, e = optionalInt(m, "Scale")
		if e != nil {
			return nil, e
		}
		if v, present := m["CaseSensitive"]; present && v != nil {
			b, ok := v.(bool)
			if !ok {
				return nil, fail("protocol")
			}
			col.CaseSensitive = &b
		}
		out[i] = col
	}
	return out, nil
}
func (o *operation) decodePage(payload map[string]any) (*pendingPage, error) {
	set, e := object(payload["ResultSet"])
	if e != nil {
		return nil, e
	}
	o.client.mu.Lock()
	columns := cloneColumns(o.j.columns)
	header := o.j.header
	incoming := o.j.token
	o.client.mu.Unlock()
	if meta, present := set["ResultSetMetadata"]; present {
		m, e := object(meta)
		if e != nil {
			return nil, e
		}
		observed, e := decodeColumns(m["ColumnInfo"], o.j.plan.expected, o.client.options.MaxColumns)
		if e != nil {
			return nil, e
		}
		declared := map[string]string{}
		for _, col := range o.j.plan.table.Columns {
			declared[col.Name] = col.Type
		}
		for _, col := range observed {
			if col.Type != declared[col.Name] {
				return nil, fail("schema")
			}
		}
		if len(columns) > 0 && !reflect.DeepEqual(columns, observed) {
			return nil, fail("schema")
		}
		columns = observed
	}
	if len(columns) == 0 {
		return nil, fail("schema")
	}
	var rows []any
	if value, present := set["Rows"]; present {
		var ok bool
		rows, ok = value.([]any)
		if !ok {
			return nil, fail("protocol")
		}
	}
	if len(rows) > o.j.plan.pageSize {
		return nil, fail("protocol")
	}
	next := ""
	if value, present := payload["NextToken"]; present {
		token, ok := value.(string)
		if !ok || token == "" || len(token) > 1024 || !utf8.ValidString(token) {
			return nil, fail("protocol")
		}
		next = token
	}
	o.client.mu.Lock()
	repeated := next != "" && (next == incoming || o.j.seen[next])
	o.client.mu.Unlock()
	if repeated {
		return nil, fail("page-progress")
	}
	cells := make([][]*string, 0, len(rows))
	for _, raw := range rows {
		if e = o.check(); e != nil {
			return nil, e
		}
		row, e := object(raw)
		if e != nil {
			return nil, e
		}
		var values []any
		if value, present := row["Data"]; present {
			var ok bool
			values, ok = value.([]any)
			if !ok {
				return nil, fail("protocol")
			}
		}
		if len(values) > len(columns) {
			return nil, fail("protocol")
		}
		decoded := make([]*string, len(columns))
		for i, v := range values {
			datum, e := object(v)
			if e != nil {
				return nil, e
			}
			for key := range datum {
				if key != "VarCharValue" {
					return nil, fail("protocol")
				}
			}
			if value, present := datum["VarCharValue"]; present && value != nil {
				s, ok := value.(string)
				if !ok {
					return nil, fail("protocol")
				}
				decoded[i] = &s
			}
		}
		if !header {
			for i, col := range columns {
				if decoded[i] == nil || *decoded[i] != col.Name {
					return nil, fail("header")
				}
			}
			header = true
			continue
		}
		cells = append(cells, decoded)
	}
	if !header && next == "" {
		return nil, fail("header")
	}
	bytes, nodes := nativeSize(columns, cells)
	bytes *= 3
	nodes *= 3
	if next != "" {
		bytes += len(next) + 32
		nodes += 2
	}
	return &pendingPage{columns: columns, cells: cells, next: next, header: header, bytes: bytes, nodes: nodes}, nil
}
func (o *operation) stagePage() error {
	o.client.mu.Lock()
	cached := o.j.pending != nil
	o.client.mu.Unlock()
	if cached {
		return o.check()
	}
	if e := o.reservePage(); e != nil {
		return e
	}
	o.client.mu.Lock()
	j := o.j
	token, id, pageSize := j.token, j.queryID, j.plan.pageSize
	o.client.mu.Unlock()
	args := map[string]any{"QueryExecutionId": id, "MaxResults": pageSize, "QueryResultType": "DATA_ROWS"}
	if token != "" {
		args["NextToken"] = token
	}
	o.client.mu.Lock()
	j.pages++
	o.client.mu.Unlock()
	payload, e := o.rpc("GetQueryResults", args, true)
	if e != nil {
		return e
	}
	pending, e := o.decodePage(payload)
	if e != nil {
		return e
	}
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	j.fetched += len(pending.cells)
	j.executionRows += len(pending.cells)
	if j.fetched > c.options.MaxRows || j.executionRows > j.plan.limit {
		return fail("bounds")
	}
	if pending.bytes > j.reserveBytes || pending.nodes > j.reserveNodes {
		return fail("cursor-quota")
	}
	if e = c.adjustLocked(j, pending.bytes-j.reserveBytes, pending.nodes-j.reserveNodes); e != nil {
		return e
	}
	j.reserveBytes = 0
	j.reserveNodes = 0
	j.pending = pending
	return nil
}
func (o *operation) readyPage(cursor Cursor) (NativePage, error) {
	if e := o.check(); e != nil {
		return NativePage{}, e
	}
	c := o.client
	c.mu.Lock()
	pending := o.j.pending
	if pending == nil {
		c.mu.Unlock()
		return NativePage{}, fail("protocol")
	}
	receipt := o.receiptLocked()
	receipt.RowsAcknowledged = o.j.ordinal + len(pending.cells)
	receipt.Complete = pending.next == ""
	receipt.ScopeDigest = o.scopeLocked(pending.columns, receipt.RowsAcknowledged, o.j.generation+1)
	page := NativePage{Columns: cloneColumns(pending.columns), Cells: cloneCells(pending.cells), OrdinalStart: o.j.ordinal, HasMore: pending.next != "", NextCursor: cursor, Receipt: receipt}
	c.mu.Unlock()
	return page, nil
}

// ACK has no foreign callback, cleanup, serialization or await after mutation.
// A failed delivery keeps the exact immutable pending native page and old handle.
func (o *operation) ack(incoming, next Cursor, collect bool) error {
	if e := o.check(); e != nil {
		return e
	}
	c := o.client
	c.mu.Lock()
	defer c.mu.Unlock()
	j := o.j
	p := j.pending
	if p == nil {
		return fail("protocol")
	}
	newColumns := j.columns
	schemaBytes, schemaNodes := 0, 0
	if len(newColumns) == 0 {
		newColumns = cloneColumns(p.columns)
		schemaBytes, schemaNodes = nativeSize(newColumns, nil)
	}
	tokenBytes, tokenNodes := 0, 0
	if p.next != "" && !j.seen[p.next] {
		tokenBytes = len(p.next) + 32
		tokenNodes = 2
	}
	keptBytes, keptNodes := 0, 0
	if collect {
		keptBytes, keptNodes = nativeSize(p.columns, p.cells)
		keptBytes *= 3
		keptNodes *= 3
	}
	byteDelta, nodeDelta := schemaBytes+tokenBytes+keptBytes-p.bytes, schemaNodes+tokenNodes+keptNodes-p.nodes
	if byteDelta > c.options.MaxCursorBytes-c.retainedBytes || nodeDelta > c.options.MaxCursorNodes-c.retainedNodes {
		return fail("cursor-quota")
	}
	newSeen := j.seen
	if tokenNodes != 0 {
		newSeen = make(map[string]bool, len(j.seen)+1)
		for token := range j.seen {
			newSeen[token] = true
		}
		newSeen[p.next] = true
	}
	materialized := j.materialized
	if collect {
		materialized = make([][]*string, len(j.materialized)+len(p.cells))
		copy(materialized, j.materialized)
		copy(materialized[len(j.materialized):], p.cells)
	}
	// All allocating/fallible preparation precedes this final authority/clock
	// guard. The lock freezes net quota and membership until the owned commit.
	if e := o.finalLocked(); e != nil {
		return e
	}
	if incoming != "" && (j.handle != incoming || c.handles[incoming] != j) {
		return fail("cursor")
	}
	j.bytes += byteDelta
	j.slots += nodeDelta
	c.retainedBytes += byteDelta
	c.retainedNodes += nodeDelta
	if incoming != "" {
		delete(c.handles, incoming)
	}
	j.columns = newColumns
	j.seen = newSeen
	j.header = p.header
	j.ordinal += len(p.cells)
	j.generation++
	j.token = p.next
	if collect {
		j.materialized = materialized
		j.materializedBytes += keptBytes
		j.materializedNodes += keptNodes
	} else {
		j.acknowledged = j.ordinal
		j.delivered = true
	}
	j.pending = nil
	j.handle = next
	if next != "" {
		c.handles[next] = j
	}
	if p.next == "" {
		j.complete = true
		if !collect {
			c.retireLocked(j)
		}
	}
	return nil
}
func (c *Client) queryPage(ctx context.Context, query dal.Query, options ReadOptions) (page NativePage, err error) {
	o, e := c.begin(ctx, "")
	if e != nil {
		return page, e
	}
	defer func() {
		if err != nil {
			err = o.error(err, true)
		}
		o.finish()
	}()
	p, e := c.compile(o, query, false)
	if e != nil {
		return page, e
	}
	if options.PageSize == 0 {
		options.PageSize = 100
	}
	if options.PageSize < 1 || options.PageSize > 1000 {
		return page, fail("input")
	}
	p.pageSize = options.PageSize
	p.digest = hashScope("paging", p.digest, strconv.Itoa(p.pageSize))
	c.mu.Lock()
	e = o.bindPlanLocked(p)
	c.mu.Unlock()
	if e != nil {
		return page, e
	}
	// Reserve before any AWS call, including workgroup preflight/submit.
	if e = o.reservePage(); e != nil {
		return page, e
	}
	if e = o.start(); e != nil {
		return page, e
	}
	return c.deliverPage(o, "", options.Prepare)
}
func (c *Client) deliverPage(o *operation, incoming Cursor, prepare func(NativePage) error) (NativePage, error) {
	// Even a cached pending retry reauthorizes the original attested authority.
	if _, e := o.identity(); e != nil {
		return NativePage{}, e
	}
	if e := o.stagePage(); e != nil {
		return NativePage{}, e
	}
	o.client.mu.Lock()
	more := o.j.pending.next != ""
	o.client.mu.Unlock()
	var next Cursor
	if more {
		s, e := randomText()
		if e != nil {
			return NativePage{}, e
		}
		next = Cursor(s)
	}
	page, e := o.readyPage(next)
	if e != nil {
		return NativePage{}, e
	}
	if prepare != nil {
		callbackCopy := clonePage(page)
		if e = o.check(); e != nil {
			return NativePage{}, e
		}
		if e = prepare(callbackCopy); e != nil {
			return NativePage{}, fail("delivery")
		}
		if e = o.check(); e != nil {
			return NativePage{}, e
		}
	}
	if e = o.ack(incoming, next, false); e != nil {
		return NativePage{}, e
	}
	return page, nil
}
func (c *Client) QueryPage(ctx context.Context, query dal.Query, options ReadOptions) (NativePage, error) {
	return c.queryPage(ctx, query, options)
}
func (c *Client) NextPage(ctx context.Context, cursor Cursor) (NativePage, error) {
	return c.NextPageWith(ctx, cursor, nil)
}

// NextPageWith lets a bridge finish its factory/serialization before ACK. A
// failing factory cannot consume the old cursor or substitute a different page.
func (c *Client) NextPageWith(ctx context.Context, cursor Cursor, prepare func(NativePage) error) (page NativePage, err error) {
	if cursor == "" {
		return page, fail("cursor")
	}
	o, e := c.begin(ctx, cursor)
	if e != nil {
		return page, e
	}
	defer func() {
		if err != nil {
			err = o.error(err, false)
		}
		o.finish()
	}()
	return c.deliverPage(o, cursor, prepare)
}
