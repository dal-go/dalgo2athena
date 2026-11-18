// Package dalgo2athena reads existing Athena tables through bounded, signed SQL
// API calls. Native cells remain exact text or SQL NULL; no AWS account is created.
package dalgo2athena

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// Identity is supplied by trusted application infrastructure. Account, subject
// and generation attest stable authority; an access key alone is not identity.
// Formatting never exposes AWS credentials. There is no default credential chain.
type Identity struct {
	account, subject, generation string
	credentials                  aws.Credentials
}

func AttestedIdentity(account, subject, generation string, credentials aws.Credentials) Identity {
	return Identity{account: account, subject: subject, generation: generation, credentials: credentials}
}
func (Identity) String() string   { return "[Athena attested credential]" }
func (Identity) GoString() string { return "[Athena attested credential]" }

type IdentityProvider func(context.Context) (Identity, error)

type Column struct {
	Name, Label, Type, Nullable, CatalogName, SchemaName, TableName string
	Precision, Scale                                                *int64
	CaseSensitive                                                   *bool
}
type Table struct {
	Name, IDColumn string
	Columns        []Column
}
type Encryption struct{ Option, KMSKey string }
type Options struct {
	Partition, Region, AccountID, SourceID, PolicyRevision            string
	WorkGroup, Catalog, Database, OutputLocation, ExpectedBucketOwner string
	Encryption                                                        Encryption
	MaxScanBytes                                                      int64
	Tables                                                            map[string]Table
	Identity                                                          IdentityProvider
	// A supplied transport is cloned/owned; no proxy, TLS bypass, redirects,
	// connection reuse or SDK retry. DialContext is a trusted operator seam.
	Transport                                                        *http.Transport
	Timeout, CursorTTL, PollInterval                                 time.Duration
	ImmediatePolling                                                 bool
	MaxConcurrent, MaxKeys, MaxRows, MaxRequests, MaxPages, MaxPolls int
	MaxRequestBytes, MaxResponseBytes, MaxControlBytes, MaxPageBytes int
	MaxNodes, MaxPageNodes, MaxColumns, MaxJourneys                  int
	// Logical retained journal/pending payload quotas, not exact Go heap limits.
	MaxCursorBytes, MaxCursorNodes int
}

// Cursor has authority only in the issuing client registry. It is neither an
// AWS NextToken nor a portable job identifier. Its lifetime never renews.
type Cursor string

func (Cursor) String() string   { return "[Athena cursor]" }
func (Cursor) GoString() string { return "[Athena cursor]" }

type ReadOptions struct {
	PageSize int
	// Prepare runs synchronously on a fresh delivery copy BEFORE acknowledgement.
	// Bridges can construct and serialize their response here. Side effects are
	// caller-owned and cannot be rolled back. No caller work runs after ACK.
	Prepare func(NativePage) error
}
type Receipt struct {
	QueryID, SourceDigest, PlanDigest, PrincipalDigest, ScopeDigest, State, CancelOutcome   string
	ExpiresAt                                                                               time.Time
	Requests, PagesFetched, RowsFetched, RowsAcknowledged, ResponseBytes, Nodes, Deliveries int
	DataScannedInBytes                                                                      string
	Complete                                                                                bool
}
type NativePage struct {
	Columns      []Column
	Cells        [][]*string
	OrdinalStart int
	HasMore      bool
	NextCursor   Cursor
	Receipt      Receipt
}
type Row map[string]*string
type Result struct {
	Columns []Column
	Cells   [][]*string
	Receipt Receipt
}
type Document struct {
	ID      string
	Data    map[string]*string
	Columns []Column
}
type Metadata struct {
	Tables           map[string]Table
	Provenance       string
	DynamicDiscovery bool
}

// Error contains only a closed safe code/status, optional redacted receipt and
// a context SENTINEL. Raw AWS causes, SQL, parameters, S3 and credentials escape
// neither Error nor Unwrap. Failed pages never carry data or a new cursor.
type Error struct {
	Code     string
	Status   int
	receipt  Receipt
	sentinel error
}

func (e *Error) Error() string {
	if e.Status != 0 {
		return fmt.Sprintf("athena: %s (HTTP %d)", e.Code, e.Status)
	}
	return "athena: " + e.Code
}
func (e *Error) Unwrap() error    { return e.sentinel }
func (e *Error) Receipt() Receipt { return e.receipt }
func fail(code string) error      { return &Error{Code: code} }
func cloneColumns(in []Column) []Column {
	out := append([]Column(nil), in...)
	for i := range out {
		if out[i].Precision != nil {
			v := *out[i].Precision
			out[i].Precision = &v
		}
		if out[i].Scale != nil {
			v := *out[i].Scale
			out[i].Scale = &v
		}
		if out[i].CaseSensitive != nil {
			v := *out[i].CaseSensitive
			out[i].CaseSensitive = &v
		}
	}
	return out
}
func cloneCells(in [][]*string) [][]*string {
	out := make([][]*string, len(in))
	for i, row := range in {
		out[i] = make([]*string, len(row))
		for k, v := range row {
			if v != nil {
				x := *v
				out[i][k] = &x
			}
		}
	}
	return out
}
func clonePage(p NativePage) NativePage {
	p.Columns = cloneColumns(p.Columns)
	p.Cells = cloneCells(p.Cells)
	return p
}
func (c *Client) ConfiguredMetadata() Metadata {
	tables := make(map[string]Table, len(c.options.Tables))
	for k, t := range c.options.Tables {
		t.Columns = cloneColumns(t.Columns)
		tables[k] = t
	}
	return Metadata{Tables: tables, Provenance: "trusted configuration, not live catalog/IAM or spend proof"}
}

func copyRow(row Row) Row {
	out := make(Row, len(row))
	for k, p := range row {
		if p != nil {
			v := *p
			out[k] = &v
		} else {
			out[k] = nil
		}
	}
	return out
}
