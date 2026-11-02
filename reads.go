package dalgo2athena

import (
	"context"
	"strings"

	"github.com/dal-go/dalgo/dal"
)

func rowMap(columns []Column, cells []*string) Row {
	row := make(Row, len(columns))
	for i, col := range columns {
		row[col.Name] = cells[i]
	}
	return row
}
func (o *operation) pointCost(p plan) (int, error) {
	args := o.submitArgs(p, strings.Repeat("0", 64))
	return wireSize(args, o.client.options.MaxRequestBytes)
}

// runPlan is the same owned page executor used by public QueryPage. It does not
// publish native cursor authority while collecting a whole keyed/core result.
// All retained rows/schema copies remain charged until final public delivery.
func (c *Client) runPlan(o *operation, p plan) (result *Result, err error) {
	defer func() {
		if err != nil {
			o.stop()
		}
	}()
	if e := o.check(); e != nil {
		return nil, e
	}
	c.mu.Lock()
	j := o.j
	if j.pending != nil || j.reserveBytes != 0 {
		c.mu.Unlock()
		return nil, fail("protocol")
	}
	oldBytes, oldNodes := j.credentialBytes, j.credentialNodes
	if len(j.columns) > 0 {
		b, n := nativeSize(j.columns, nil)
		oldBytes += b
		oldNodes += n
	}
	for token := range j.seen {
		oldBytes += len(token) + 32
		oldNodes += 2
	}
	_ = c.adjustLocked(j, -oldBytes, -oldNodes)
	j.credential = Identity{}
	j.credentialBytes = 0
	j.credentialNodes = 0
	j.columns = nil
	j.seen = map[string]bool{}
	j.token = ""
	j.header = false
	j.queryID = ""
	j.state = ""
	j.started = false
	j.stopped = false
	j.complete = false
	j.collecting = true
	j.delivered = false
	j.executionRows = 0
	j.cancelOutcome = "not_requested"
	p.pageSize = 1000
	if e := o.bindPlanLocked(p); e != nil {
		c.mu.Unlock()
		return nil, e
	}
	start := len(j.materialized)
	c.mu.Unlock()
	if e := o.reservePage(); e != nil {
		return nil, e
	}
	if e := o.start(); e != nil {
		return nil, e
	}
	for {
		if e := o.stagePage(); e != nil {
			return nil, e
		}
		c.mu.Lock()
		more := j.pending.next != ""
		c.mu.Unlock()
		if e := o.ack("", "", true); e != nil {
			return nil, e
		}
		if !more {
			break
		}
	}
	if e := o.check(); e != nil {
		return nil, e
	}
	c.mu.Lock()
	result = &Result{Columns: cloneColumns(j.columns), Cells: cloneCells(j.materialized[start:]), Receipt: o.receiptLocked()}
	c.mu.Unlock()
	if p.requireID {
		seen := map[string]bool{}
		for _, cells := range result.Cells {
			row := rowMap(result.Columns, cells)
			id := row[p.table.IDColumn]
			if id == nil || *id == "" || seen[*id] {
				return nil, fail("identity")
			}
			seen[*id] = true
		}
	}
	return result, nil
}
func (c *Client) point(o *operation, p plan, id string) (*Document, error) {
	r, e := c.runPlan(o, p)
	if e != nil {
		return nil, e
	}
	if len(r.Cells) == 0 {
		return nil, nil
	}
	if len(r.Cells) != 1 {
		return nil, fail("identity")
	}
	data := make(map[string]*string, len(r.Columns))
	for i, col := range r.Columns {
		data[col.Name] = r.Cells[0][i]
	}
	nativeID := data[p.table.IDColumn]
	if nativeID == nil || *nativeID != id {
		return nil, fail("identity")
	}
	return &Document{ID: id, Data: data, Columns: r.Columns}, nil
}
func (c *Client) Get(ctx context.Context, collection, id string) (document *Document, err error) {
	o, e := c.begin(ctx, "")
	if e != nil {
		return nil, e
	}
	defer func() {
		if err != nil {
			err = o.error(err, true)
		}
		c.mu.Lock()
		c.retireLocked(o.j)
		c.mu.Unlock()
		o.finish()
	}()
	p, e := c.pointPlan(o, collection, id)
	if e != nil {
		return nil, e
	}
	document, err = c.point(o, p, id)
	if fresh := o.check(); fresh != nil {
		return nil, fresh
	}
	if err == nil {
		if _, e := o.markDelivered(); e != nil {
			return nil, e
		}
	}
	return document, err
}
func (c *Client) Exists(ctx context.Context, collection, id string) (bool, error) {
	document, e := c.Get(ctx, collection, id)
	return document != nil, e
}
func (c *Client) GetMany(ctx context.Context, collection string, ids []string) (documents []*Document, err error) {
	o, e := c.begin(ctx, "")
	if e != nil {
		return nil, e
	}
	defer func() {
		if err != nil {
			err = o.error(err, true)
		}
		c.mu.Lock()
		c.retireLocked(o.j)
		c.mu.Unlock()
		o.finish()
	}()
	if len(ids) > c.options.MaxKeys {
		return nil, fail("bounds")
	}
	ids = append([]string(nil), ids...)
	if _, ok := c.options.Tables[collection]; !ok {
		return nil, fail("mapping")
	}
	plans := make([]plan, len(ids))
	cost := 0
	for i, id := range ids {
		p, e := c.pointPlan(o, collection, id)
		if e != nil {
			return nil, e
		}
		for _, v := range append([]string{p.sql}, p.parameters...) {
			n, e := jsonStringSize(v)
			if e != nil || n+1024 > c.options.MaxRequestBytes-cost {
				return nil, fail("bounds")
			}
			cost += n + 1024
		}
		plans[i] = p
	}
	documents = make([]*Document, len(ids))
	for i, id := range ids {
		documents[i], e = c.point(o, plans[i], id)
		if e != nil {
			return nil, e
		}
	}
	if e := o.check(); e != nil {
		return nil, e
	}
	if _, e := o.markDelivered(); e != nil {
		return nil, e
	}
	return documents, nil
}
func (c *Client) Query(ctx context.Context, query dal.Query) (result *Result, err error) {
	o, e := c.begin(ctx, "")
	if e != nil {
		return nil, e
	}
	defer func() {
		if err != nil {
			err = o.error(err, true)
		}
		c.mu.Lock()
		c.retireLocked(o.j)
		c.mu.Unlock()
		o.finish()
	}()
	p, e := c.compile(o, query, false)
	if e != nil {
		return nil, e
	}
	result, err = c.runPlan(o, p)
	if fresh := o.check(); fresh != nil {
		return nil, fresh
	}
	if err == nil {
		receipt, e := o.markDelivered()
		if e != nil {
			return nil, e
		}
		result.Receipt = receipt
	}
	return result, err
}
