package dalgo2athena

import (
	"encoding/json"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/dal-go/dalgo/dal"
)

type plan struct {
	collection           string
	table                Table
	sql                  string
	parameters, expected []string
	limit, pageSize      int
	requireID            bool
	digest               string
}

var numberPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$`)
var decimalPattern = regexp.MustCompile(`^-?(0|[1-9][0-9]*)(\.[0-9]+)?$`)

func quote(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }
func parameter(v any) (string, error) {
	switch x := v.(type) {
	case string:
		if !utf8.ValidString(x) {
			return "", fail("input")
		}
		return "'" + strings.ReplaceAll(x, "'", "''") + "'", nil
	case bool:
		if x {
			return "TRUE", nil
		}
		return "FALSE", nil
	case int:
		return strconv.FormatInt(int64(x), 10), nil
	case int8:
		return parameter(int64(x))
	case int16:
		return parameter(int64(x))
	case int32:
		return parameter(int64(x))
	case int64:
		return strconv.FormatInt(x, 10), nil
	case uint:
		return parameter(uint64(x))
	case uint8:
		return parameter(uint64(x))
	case uint16:
		return parameter(uint64(x))
	case uint32:
		return parameter(uint64(x))
	case uint64:
		return strconv.FormatUint(x, 10), nil
	case json.Number:
		if len(x) > 128 || !decimalPattern.MatchString(string(x)) {
			return "", dal.ErrNotSupported
		}
		digits := 0
		for _, r := range x {
			if r >= '0' && r <= '9' {
				digits++
			}
		}
		if digits > 38 {
			return "", fail("input")
		}
		return string(x), nil
	case float32:
		if math.IsInf(float64(x), 0) || math.IsNaN(float64(x)) {
			return "", fail("input")
		}
		return "CAST('" + strconv.FormatFloat(float64(x), 'g', -1, 32) + "' AS DOUBLE)", nil
	case float64:
		if math.IsInf(x, 0) || math.IsNaN(x) {
			return "", fail("input")
		}
		return "CAST('" + strconv.FormatFloat(x, 'g', -1, 64) + "' AS DOUBLE)", nil
	default:
		return "", dal.ErrNotSupported
	}
}
func (c *Client) compile(o *operation, input dal.Query, requireID bool) (plan, error) {
	q, ok := input.(dal.StructuredQuery)
	if !ok || q == nil {
		return plan{}, dal.ErrNotSupported
	}
	from, e := observe(o, q.From)
	if e != nil {
		return plan{}, e
	}
	if from == nil {
		return plan{}, dal.ErrNotSupported
	}
	base, e := observe(o, from.Base)
	if e != nil {
		return plan{}, e
	}
	joins, e := observe(o, from.Joins)
	if e != nil {
		return plan{}, e
	}
	ref, ok := base.(dal.CollectionRef)
	if !ok || len(joins) > 0 || ref.Parent() != nil || ref.Schema() != "" || ref.Database() != "" || ref.Alias() != "" || ref.ScanLimit() != 0 || len(ref.ScanOrders()) > 0 {
		return plan{}, dal.ErrNotSupported
	}
	t, ok := c.options.Tables[ref.Name()]
	if !ok {
		return plan{}, fail("mapping")
	}
	p := plan{collection: ref.Name(), table: t, requireID: requireID}
	if requireID && t.IDColumn == "" {
		return p, dal.ErrNotSupported
	}
	groups, e := observe(o, q.GroupBy)
	if e != nil {
		return p, e
	}
	having, e := observe(o, q.Having)
	if e != nil {
		return p, e
	}
	start, e := observe(o, q.StartFrom)
	if e != nil {
		return p, e
	}
	after, e := observe(o, q.StartAfter)
	if e != nil {
		return p, e
	}
	if len(groups) > 0 || having != nil || start != "" || after != "" {
		return p, dal.ErrNotSupported
	}
	limit, e := observe(o, q.Limit)
	if e != nil {
		return p, e
	}
	offset, e := observe(o, q.Offset)
	if e != nil {
		return p, e
	}
	if limit < 0 || limit > c.options.MaxRows || offset < 0 || offset > 100000 {
		return p, fail("bounds")
	}
	p.limit = limit
	if p.limit == 0 {
		p.limit = c.options.MaxRows
	}
	sqlLimit := p.limit
	if limit == 0 {
		sqlLimit++
	}
	allowed := map[string]bool{}
	for _, col := range t.Columns {
		allowed[col.Name] = true
	}
	field := func(expr dal.Expression) (string, error) {
		f, ok := expr.(dal.FieldRef)
		if !ok || f.Source() != "" {
			return "", dal.ErrNotSupported
		}
		n := f.Name()
		if f.IsID() {
			if t.IDColumn == "" {
				return "", dal.ErrNotSupported
			}
			n = t.IDColumn
		}
		if !name(n) || !allowed[n] {
			return "", fail("mapping")
		}
		return n, nil
	}
	columns, e := observe(o, q.Columns)
	if e != nil {
		return p, e
	}
	if len(columns) > c.options.MaxColumns {
		return p, fail("bounds")
	}
	columns = append([]dal.Column(nil), columns...)
	selection := []string{}
	hasID := false
	if len(columns) == 0 {
		for _, col := range t.Columns {
			p.expected = append(p.expected, col.Name)
			selection = append(selection, quote(col.Name))
			hasID = hasID || col.Name == t.IDColumn
		}
	} else {
		seen := map[string]bool{}
		for _, col := range columns {
			n, e := field(col.Expression)
			if e != nil {
				return p, e
			}
			if col.Alias != "" && col.Alias != n {
				return p, dal.ErrNotSupported
			}
			if seen[n] {
				return p, fail("input")
			}
			seen[n] = true
			p.expected = append(p.expected, n)
			selection = append(selection, quote(n))
			hasID = hasID || n == t.IDColumn
		}
	}
	if requireID && !hasID {
		return p, dal.ErrNotSupported
	}
	inputBytes := 0
	bind := func(v any) (string, error) {
		if s, ok := v.(string); ok && len(s) > 4096 {
			return "", fail("bounds")
		}
		s, e := parameter(v)
		if e != nil {
			return "", e
		}
		if len(p.parameters) >= 64 || utf8.RuneCountInString(s) > 1024 {
			return "", fail("bounds")
		}
		cost, e := jsonStringSize(s)
		if e != nil {
			return "", e
		}
		if cost > c.options.MaxRequestBytes-inputBytes {
			return "", fail("bounds")
		}
		inputBytes += cost
		p.parameters = append(p.parameters, s)
		return "?", nil
	}
	condition, e := observe(o, q.Where)
	if e != nil {
		return p, e
	}
	nodes := 0
	var walk func(dal.Condition, int) (string, error)
	walk = func(condition dal.Condition, depth int) (string, error) {
		if e := o.check(); e != nil {
			return "", e
		}
		nodes++
		if nodes > 64 || depth > 16 {
			return "", fail("bounds")
		}
		switch v := condition.(type) {
		case nil:
			return "", nil
		case dal.GroupCondition:
			if v.Operator() != dal.And && v.Operator() != dal.Or {
				return "", dal.ErrNotSupported
			}
			items := v.Conditions()
			if len(items) == 0 || len(items) > 64 {
				return "", fail("input")
			}
			items = append([]dal.Condition(nil), items...)
			terms := []string{}
			for _, item := range items {
				if item == nil {
					return "", fail("input")
				}
				s, e := walk(item, depth+1)
				if e != nil {
					return "", e
				}
				terms = append(terms, s)
			}
			return "(" + strings.Join(terms, " "+string(v.Operator())+" ") + ")", nil
		case dal.Comparison:
			n, e := field(v.Left)
			if e != nil {
				return "", e
			}
			lhs := quote(n)
			if v.Operator == dal.In || v.Operator == dal.NotIn {
				a, ok := v.Right.(dal.Array)
				if !ok {
					return "", dal.ErrNotSupported
				}
				rv := reflect.ValueOf(a.Value)
				if !rv.IsValid() || rv.Kind() != reflect.Slice || rv.Len() == 0 || rv.Len() > 64 {
					return "", fail("input")
				}
				values := make([]any, rv.Len())
				for i := range values {
					values[i] = rv.Index(i).Interface()
				}
				terms := []string{}
				for _, v := range values {
					if v == nil {
						return "", fail("input")
					}
					s, e := bind(v)
					if e != nil {
						return "", e
					}
					terms = append(terms, s)
				}
				op := " IN "
				if v.Operator == dal.NotIn {
					op = " NOT IN "
				}
				return lhs + op + "(" + strings.Join(terms, ", ") + ")", nil
			}
			constant, ok := v.Right.(dal.Constant)
			if !ok {
				return "", dal.ErrNotSupported
			}
			op := ""
			switch v.Operator {
			case dal.Equal:
				op = "="
			case dal.Operator("!="):
				op = "<>"
			case dal.GreaterThen:
				op = ">"
			case dal.GreaterOrEqual:
				op = ">="
			case dal.LessThen:
				op = "<"
			case dal.LessOrEqual:
				op = "<="
			default:
				return "", dal.ErrNotSupported
			}
			if constant.Value == nil {
				if op == "=" {
					return lhs + " IS NULL", nil
				}
				if op == "<>" {
					return lhs + " IS NOT NULL", nil
				}
				return "", fail("input")
			}
			s, e := bind(constant.Value)
			return lhs + " " + op + " " + s, e
		default:
			return "", dal.ErrNotSupported
		}
	}
	where, e := walk(condition, 0)
	if e != nil {
		return p, e
	}
	orders, e := observe(o, q.OrderBy)
	if e != nil {
		return p, e
	}
	if len(orders) > 16 {
		return p, fail("bounds")
	}
	orders = append([]dal.OrderExpression(nil), orders...)
	order, seen := []string{}, map[string]bool{}
	for _, item := range orders {
		if item == nil {
			return p, dal.ErrNotSupported
		}
		expr, e := observe(o, item.Expression)
		if e != nil {
			return p, e
		}
		n, e := field(expr)
		if e != nil {
			return p, e
		}
		desc, e := observe(o, item.Descending)
		if e != nil {
			return p, e
		}
		if seen[n] {
			return p, fail("input")
		}
		seen[n] = true
		direction := " ASC NULLS LAST"
		if desc {
			direction = " DESC NULLS LAST"
		}
		order = append(order, quote(n)+direction)
	}
	if t.IDColumn != "" && !seen[t.IDColumn] {
		order = append(order, quote(t.IDColumn)+" ASC NULLS LAST")
	}
	p.sql = "SELECT " + strings.Join(selection, ", ") + " FROM " + quote(c.options.Catalog) + "." + quote(c.options.Database) + "." + quote(t.Name)
	if where != "" {
		p.sql += " WHERE " + where
	}
	if len(order) > 0 {
		p.sql += " ORDER BY " + strings.Join(order, ", ")
	}
	p.sql += " OFFSET " + strconv.Itoa(offset) + " ROWS LIMIT " + strconv.Itoa(sqlLimit)
	if len(p.sql) > c.options.MaxRequestBytes || len(p.sql)+inputBytes > c.options.MaxRequestBytes {
		return p, fail("bounds")
	}
	p.digest = hashScope("plan", append([]string{c.sourceDigest, p.sql}, p.parameters...)...)
	if e = o.check(); e != nil {
		return p, e
	}
	return p, nil
}
func (c *Client) pointPlan(o *operation, collection, id string) (plan, error) {
	if e := o.check(); e != nil {
		return plan{}, e
	}
	t, ok := c.options.Tables[collection]
	if !ok {
		return plan{}, fail("mapping")
	}
	if t.IDColumn == "" {
		return plan{}, dal.ErrNotSupported
	}
	if id == "" || !utf8.ValidString(id) || len(id) > 1024 {
		return plan{}, fail("key")
	}
	q := dal.From(dal.NewRootCollectionRef(collection, "")).NewQuery().Where(dal.Comparison{Left: dal.DocumentID(), Operator: dal.Equal, Right: dal.Constant{Value: id}}).Limit(2).SelectIntoRecordset()
	return c.compile(o, q, true)
}
