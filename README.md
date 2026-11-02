# DALgo native Athena reads

Read configured existing Athena SQL tables with a closed DALgo SELECT compiler,
original string/null cells, native column metadata and public same-query pages.
There is no managed document envelope, arbitrary SQL executor, default AWS
credential chain, account creation, workgroup update or direct S3 result reader.

```go
client, err := dalgo2athena.New(dalgo2athena.Options{
    Partition: "aws", Region: "us-east-1", AccountID: "123456789012",
    SourceID: "reviewed-source", PolicyRevision: "v1",
    WorkGroup: "existing-readers", Catalog: "AwsDataCatalog", Database: "existing",
    OutputLocation: "s3://existing-results/reviewed-prefix/",
    ExpectedBucketOwner: "123456789012",
    Encryption: dalgo2athena.Encryption{Option: "SSE_S3"},
    MaxScanBytes: 10000000,
    Tables: map[string]dalgo2athena.Table{
        "items": {Name: "existing_items", IDColumn: "id", Columns: []dalgo2athena.Column{
            {Name: "id", Type: "varchar"}, {Name: "amount", Type: "decimal"},
        }},
    },
    Identity: func(ctx context.Context) (dalgo2athena.Identity, error) {
        // Trusted infrastructure supplies explicit credentials and attests the
        // SAME account/subject/generation. Respect ctx; no SDK default discovery.
        credentials, err := trustedReadCredentials(ctx)
        return dalgo2athena.AttestedIdentity("123456789012", "reader", "generation1", credentials), err
    },
})
if err != nil { return err }
defer client.Close()
q := dal.From(dal.NewRootCollectionRef("items", "")).NewQuery().Where().Limit(100).SelectIntoRecordset()
page, err := client.QueryPage(ctx, q, dalgo2athena.ReadOptions{PageSize: 10})
for err == nil {
    consume(page.Columns, page.Cells) // string pointers; nil means SQL NULL
    if !page.HasMore { break }
    page, err = client.NextPage(ctx, page.NextCursor)
}
```

## Authority and source profile

Construction is lazy. Fixed configuration selects AWS partition/region and its
canonical HTTPS Athena endpoint, expected account, workgroup/catalog/database,
source/policy revision, exact existing tables/allowed columns, and customer-S3
output prefix/expected bucket owner/encryption. The prefix must end with `/`.
SSE_S3 and explicit SSE_KMS are supported; managed result storage, Spark,
identity-center and S3 access-grant profiles are not implemented. Other profiles
must be qualified separately, not silently mapped to this one.

The identity provider is trusted application infrastructure, not a credential
selector supplied by a query. An access key is not principal attestation.
Account/subject/generation is checked throughout the journey; credentials may
rotate only within that same attested authority. A changed principal is refused,
not rebound to an existing cursor. Credentials have redacted formatting and are
never carried in page/receipt/error JSON. No IMDS, profiles, STS/default-chain,
anonymous or administrative fallback occurs.

Before each original SELECT, GetWorkGroup must report ENABLED, enforced SQL
engine2/3, cutoff at least10,000,000 and no higher than the configured maximum,
and matching output/owner/encryption. StartQueryExecution explicitly disables
result reuse, passes the fixed resource context and a new random64-character
idempotency token. There is exactly one submit attempt: throttle/transport or
permission errors do not resubmit, reuse another query ID or refresh authority
for a hidden fallback. A prior idempotency token must never be used as permission
proof. Every status/result request uses only this owned validated execution ID.

Returned execution context/workgroup/query/effective output/profile is checked.
This cannot eliminate the preflight-to-execution policy race or certify IAM,
Glue/Lake Formation/S3/KMS permissions. In particular, S3 GetObject can expose
results even when Athena GetQueryResults is denied. The adapter never bypasses
GetQueryResults with a direct S3 download. Protect result storage independently.

## Native query and keyed APIs

QueryPage/NextPage are native **ordinal** pages. Equal-valued rows on different
ordinals are retained; no deduplication invents keys. Configured IDColumn enables
Get/Exists/GetMany and keyed DALgo records. Its string identity is operator
declared, not a uniqueness constraint enforced by ordinary Athena tables. Point
SELECTs probe two rows and reject duplicates/mismatches. Keyed query readers
require the actual ID projection and unique/non-null original string identities.
Without IDColumn, native ordinal queries and recordset readers remain available;
keyed APIs are not implemented. GetMany snapshots all keys before callbacks,
preserves ordered independent duplicates/misses and shares original budgets.

Client.Query materializes the same bounded native page executor into a whole
Result. Get/GetMany and the DALgo backend do not create a second SQL executor.
Whole-call collection keeps all staged result data charged until delivery;
failure returns no successful row prefix. Public page calls differ: earlier
ACKed pages remain acknowledged when a later page fails, and the error receipt
explicitly reports an incomplete journey.

The compiler accepts one configured root collection, bounded plain projections,
scalar comparisons/null tests/IN/NOT IN/AND/OR, and ASC/DESC NULLS LAST ordering.
An optional ID tie-breaker is added only when configured. It quotes catalog,
database, table and fields as exact identifiers; identifier dots are literal.
Values become generated WHERE `?` positions and separately generated literal
expressions, never raw SQL interpolation. Strings double internal apostrophes;
bools use TRUE/FALSE, integers/closed decimal json.Number keep exact text, finite
float input uses a generated DOUBLE cast and retains its pre-existing precision.
Exponent-form exact-decimal inputs, date/custom codecs and arbitrary expressions
are not implemented. Empty/null-containing membership is refused.

There are at most64 parameters, each generated expression at most1024 Unicode
characters.64 is an adapter cap, not an AWS25-parameter API claim. Conditions
have64 nodes/depth16, projections at most256 configured columns and16 order terms.
OFFSET0..100000 is rendered **once**, before LIMIT in the original SELECT; it is
never reapplied on resume or implemented by dropping fetched rows. OFFSET is not
a scan or billing cap or a cross-call snapshot. A missing limit selects the local
maxRows+1 overflow probe. Explicit semantic LIMIT is never implemented by slicing
a service page; over-return fails whole instead.

Cells preserve VarCharValue STRING or NULL, including decimal/int64/exponent,
temporal, binary and complex textual spellings. Empty string and text `null` are
distinct from NULL. Missing VarCharValue and missing trailing Data cells become
nil; excess cells/wrong scalar shapes fail. Explicit JSON-null VarCharValue is
covered by an actual pinned official Athena SDK serde test, not guessed from a
handwritten DTO. That test must execute successfully before qualification.
Precisely omitted optional ResultSet.Rows and Row.Data arrays mean an empty
page and an all-NULL padded row respectively. Present malformed array values
remain errors; omitting a header's Data cannot manufacture a valid header.
The pinned SDK witness also covers these optional-field omissions.

The SELECT header row is validated/consumed exactly once. A real data row equal
to the column-name row remains data. Header-only MaxResults1 and empty/short
nonterminal pages continue while a NextToken exists. Initial metadata is
required; later metadata may be omitted and is otherwise compared in full/order.
Column name/label/type/nullable/case sensitivity/catalog/schema/table and optional
precision/scale remain native metadata; no numeric/time/binary parsing is inferred.

`DB()` supplies real DALgo Get/Exists/GetMulti/records/recordset readers. Explicit
Row/*Row and map[string]any/*map[string]any targets retain string/nil. Struct
mapping and wider typed key modes are not implemented. Core record0.1.5's key
path cannot represent `%`; direct native point IDs preserve it. Readers retain
the original deadline/client lifetime, use direct typed-any core columns for
nil-safe allocation, expose copied Columns metadata, and must be closed. They
are for sequential use; portable Reader.Cursor is not implemented.

## Public page acknowledgement and resources

Each cursor is a random handle in this client's private registry, bound to
immutable source/plan/config/pageSize/principal/query/schema/generation/ordinal
and original expiry. It is not an AWS NextToken, query-ID adoption capability,
portable token or cross-process cursor. A consumed cursor cannot replay. A
concurrent call on the same busy handle refuses without another dispatch.

Original whole-query deadline and fixed cursor TTL span every page/retry/auth
rotation; neither is renewed. A complete owned native page is staged before
delivery, with its prospective service token/schema and retained quota. A failing
Prepare callback or caller cancellation returns no page/new handle, keeps the
old handle and pending native page for retry, and does not refetch changed data.
Retry receives a fresh copy; a mutable callback object is never cached authority.
Protocol/read failure before a valid stage still spends its attempted work.

ReadOptions.Prepare and NextPageWith let a bridge finish its factory/serialization
BEFORE final cleanup/lifetime/quota/membership checks and atomic ACK. After ACK
there is no foreign callback or await. A bridge must fix/authenticate source,
principal, Origin and policy on every request; raw SQL/queryID/NextToken is not an
input. Network loss after server ACK is unknown delivery, not exactly-once HTTP.
Caller callback side effects cannot be rolled back and should be idempotent.

Default/hard shared retained pool is16MiB and65,536 logical nodes, plus at most1000
journeys. Fixed journal slots, plan/parameter snapshots, captured credentials,
schema/service-token membership, pending native data and staged materialization
are charged. Before page I/O, worst-case bytes/nodes are reserved; replace with
actual stage cost and perform atomic net replacement at ACK. Per-page parse nodes
default8192, distinct from whole-query65,536; invalid incompatible reservations
are refused, not admitted through a65536+overhead default. These are logical
admission counts, not exact Go heap/RSS guarantees.

Unused reservations and retired state refund exactly once only after public and
physical work settles. CloseCursor abandons one local completed-query journey;
Close fences the whole client. Expiry is pruned on admission, and busy expired or
closed work retains credit until actual settlement. There is no silent eviction,
quota raise, refund of observed work or fabricated completion. Idle unconsumed
cursors can exhaust quota; close abandoned cursors/client or await original expiry.

Other defaults (maxima): timeout30s (120s), cursor TTL120s (120s), concurrent4 (16),
keys100 (1000), rows10000 (100000), requests512 (2048), pages100 (1000), polls100
(1000) at100ms, request bodies64KiB (262144), aggregate response32MiB (32MiB),
control response4MiB (4MiB), result page1MiB (4MiB), columns256 (256), whole JSON
nodes65536 (262144), page nodes8192 (32768 only if global reservation fits),
JSON depth16, headers64KiB/1024. Limits combine; they do not guarantee every
maximum can be used simultaneously under the shared pool.

Receipt request/page/row/node/byte/delivery counters are local admission/work
observations. DataScannedInBytes is exact provider-reported text for the current
owned execution, not a local reservation or sum/estimate of warehouse charges.
Workgroup cutoff, LIMIT, OFFSET, result caps and cancellation do NOT promise
zero cost, a dry run, maximumBytesBilled, scan/credit/remote-memory or total process
RAM bounds. Cancelled queries can incur charges. No Google paid-ledger/approval
semantics are imported or relabelled.

## Cancellation, transport and validation

The scope starts before input/auth/signing/requests/callbacks. Caller cancellation
precedes elapsed deadline, then client Close; every callback/final delivery is
freshly checked. Arbitrary synchronous caller code cannot safely be preempted;
no abandoned worker mutates caller records. Ignored auth/sign/send/read/Close/stop
retains physical admission and busy/quota until true settlement.
The entire escaped Start envelope, including fixed resource fields and the
64-character nonce, is admitted before credential or workgroup preflight work;
the actual request still checks its remaining shared budget before dispatch.

Only official AWS SigV4 signs explicit supplied credentials. The owned verified
HTTP1 transport is cloned, has no proxy/redirect/compression/connection-reuse
retry, and never destroys a borrowed global pool. Trusted DialContext may map
the canonical account endpoint to owned TLS fixtures without disabling TLS.
All errors use safe codes/status and optional redacted receipts; no underlying
AWS cause/body, SQL/parameters, S3 URL or credentials are returned. Only safe
context/core-not-implemented sentinels are unwrapped.

Abandoning an owned pending query requests at most one StopQueryExecution with
the CAPTURED submit ID/credentials, under an independent private2s scope. No
credential refresh/adoption/permission fallback occurs. HTTP200 empty is only
cancel_requested; nonempty200 (including `{}` or whitespace), denial and a stalled
stop remain unknown. A rejected/unknown/late unowned submit stays unknown. Completed
query page cancellation does not pretend to stop a running query. Shutdown(ctx)
preserves nonblocking Close and waits at most2s for the ENTIRE logical+physical
operation map; caller must close readers first. Nil means local quiescence, not
hosted termination. A future short-lived CLI must drain with a separate cleanup
context and preserve the original read error/empty stdout.

Account-free tests use owned verified TLS, the official signer and a pinned
official SDK null-decoding witness. They cover compiler/native pages/ACK and
quotas, core readers, real dispatch/disconnect/Stop/drain and hostile lifetime
boundaries. These tests are authored source until root qualification executes.
No real Athena account/query/spend, S3/IAM/KMS/Lake Formation/browser/CORS,
installed CLI/registry release/main/App/deployment evidence is claimed.

Writes, transactions, schema discovery, joins/groups/aggregates/computed aliases,
raw SQL, durable portable cursors and arbitrary job adoption are adapter
NOT_IMPLEMENTED, not declarations that Athena intrinsically lacks them.
