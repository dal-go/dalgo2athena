package dalgo2athena

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dal-go/record"
)

func TestConfigurationAndIdentityAreLazyBoundedAndRedacted(t *testing.T) {
	c, f := newFixture(t, nil, nil)
	baseline := c.options
	for _, change := range []func(*Options){func(o *Options) { o.Region = "us-east-1/foreign" }, func(o *Options) { o.Partition = "aws-cn" }, func(o *Options) { o.MaxCursorNodes = 64 }, func(o *Options) { o.MaxPageNodes = 65536 }, func(o *Options) { o.MaxCursorBytes = 1024 }, func(o *Options) { o.MaxScanBytes = 9999999 }, func(o *Options) {
		o.Tables = map[string]Table{"bad": {Name: "bad", IDColumn: "ID", Columns: []Column{{Name: "ID", Type: "integer"}}}}
	}, func(o *Options) {
		o.Tables = map[string]Table{"bad": {Name: "bad", Columns: []Column{{Name: "ID", Type: "varchar", Label: strings.Repeat("SECRET", 20000)}}}}
	}, func(o *Options) { o.Transport = c.owned.Clone(); o.Transport.TLSClientConfig.InsecureSkipVerify = true }} {
		options := baseline
		change(&options)
		if bad, e := New(options); e == nil || bad != nil || strings.Contains(e.Error(), "SECRET") {
			t.Fatal("invalid configured authority admitted", e)
		}
	}
	if len(f.snapshot()) != 0 {
		t.Fatal("construction performed wire I/O")
	}
	id := AttestedIdentity(testAccount, "reader", "generation1", testCredentials)
	for _, v := range []string{fmt.Sprint(id), fmt.Sprintf("%#v", id), fmt.Sprintf("%+v", id)} {
		if strings.Contains(v, testCredentials.SecretAccessKey) || strings.Contains(v, testCredentials.SessionToken) || strings.Contains(v, testCredentials.AccessKeyID) {
			t.Fatal("credentials formatted")
		}
	}
}
func TestAdmissionSnapshotsBatchAndCountsEscapingBeforeAuthentication(t *testing.T) {
	c, f := newFixture(t, nil, nil)
	for _, ids := range [][]string{{"a", ""}, {"a", strings.Repeat("<", 1024)}, make([]string, 101)} {
		if docs, e := c.GetMany(context.Background(), "rows", ids); e == nil || docs != nil || len(f.snapshot()) != 0 {
			t.Fatal("later malformed/oversize key dispatched", e)
		}
	}
}
func TestCredentialMetadataIsNotRetainedInCursorGraph(t *testing.T) {
	credentials := testCredentials
	credentials.Source = strings.Repeat("not-signing-history", 10000)
	c, _ := newFixture(t, twoPageHook, func(o *Options) {
		o.Identity = func(context.Context) (Identity, error) {
			return AttestedIdentity(testAccount, "reader", "generation1", credentials), nil
		}
	})
	page, e := c.QueryPage(context.Background(), query(), ReadOptions{PageSize: 2})
	if e != nil {
		t.Fatal(e)
	}
	c.mu.Lock()
	j := c.handles[page.NextCursor]
	source := j.credential.credentials.Source
	charged := j.credentialBytes
	c.mu.Unlock()
	if source != "" || charged <= 0 || charged > 20000 {
		t.Fatal("unbounded unused credential history retained")
	}
	if e = c.CloseCursor(page.NextCursor); e != nil {
		t.Fatal(e)
	}
}
func TestFullEscapedSubmitAdmissionPrecedesEveryCredentialAndPreflightPath(t *testing.T) {
	paths := map[string]func(*Client) error{
		"native-page":  func(c *Client) error { _, e := c.QueryPage(context.Background(), query(), ReadOptions{}); return e },
		"native-whole": func(c *Client) error { _, e := c.Query(context.Background(), query()); return e },
		"point":        func(c *Client) error { _, e := c.Get(context.Background(), "rows", "item"); return e },
		"exists":       func(c *Client) error { _, e := c.Exists(context.Background(), "rows", "item"); return e },
		"batch":        func(c *Client) error { _, e := c.GetMany(context.Background(), "rows", []string{"item"}); return e },
		"core-point": func(c *Client) error {
			data := map[string]any{}
			return c.DB().Get(context.Background(), record.NewRecordWithData(record.NewKeyWithID("rows", "item"), &data))
		},
		"core-exists": func(c *Client) error {
			_, e := c.DB().Exists(context.Background(), record.NewKeyWithID("rows", "item"))
			return e
		},
		"records-reader": func(c *Client) error {
			r, e := c.DB().ExecuteQueryToRecordsReader(context.Background(), query())
			if r != nil {
				_ = r.Close()
			}
			return e
		},
		"recordset-reader": func(c *Client) error {
			r, e := c.DB().ExecuteQueryToRecordsetReader(context.Background(), query())
			if r != nil {
				_ = r.Close()
			}
			return e
		},
	}
	for name, run := range paths {
		t.Run(name, func(t *testing.T) {
			auth := 0
			c, f := newFixture(t, nil, func(o *Options) {
				escaped := strings.Repeat("<", 4096)
				o.Catalog = escaped
				o.Database = escaped
				o.Tables = map[string]Table{"rows": {Name: escaped, IDColumn: escaped, Columns: []Column{{Name: escaped, Type: "varchar"}}}}
				o.Identity = func(context.Context) (Identity, error) {
					auth++
					return AttestedIdentity(testAccount, "reader", "generation1", testCredentials), nil
				}
			})
			if e := run(c); e == nil || auth != 0 || len(f.snapshot()) != 0 {
				t.Fatal("full escaped envelope was admitted after credential/preflight work", e, auth, len(f.snapshot()))
			}
		})
	}
	c, f := newFixture(t, nil, func(o *Options) {
		table := o.Tables["rows"]
		table.Name = strings.Repeat("<", 128)
		o.Tables["rows"] = table
	})
	doc, e := c.Get(context.Background(), "rows", "item")
	if e != nil || doc == nil || doc.ID != "item" || f.count("StartQueryExecution") != 1 {
		t.Fatal("bounded escaped identifier positive control failed", e)
	}
}
