# PRE-6: cross-venue parent identity and execution scope

Status: approved for implementation on 25 October 2026, including D6/D7 independent-review additions. Offline verification and pre-landing review are required before merge. No actual trades are authorized.

Issue: https://linear.app/synpath/issue/PRE-6/remove-arbitrary-nominal-venue-from-cross-venue-smart-order-parents

## Baselines and destinations

- prediction_market worktree: `/Users/slingshot/.codex/worktrees/pre-6-cross-venue-parent/prediction_market`, based on main `cda4369a804a96f54288966ab96cf2438ef2de10`; eventual PR base `main`.
- Public SDK worktree: `/private/tmp/pre-6-synpath-public`, branch `codex/pre-6-cross-venue-parent`, based on experimental branch `85013870850c5741788a99a0c746fe224de2e366`; eventual PR base `codex/order-lifecycle-storage`.
- The original checkouts contain unrelated work and remain untouched.
- The current terminal is a read-only market-data application with execution previews. It has no order submission endpoint. Execution UI on another unmerged branch is not assumed to be part of main.
- The latest experimental SDK branch is the terminal compatibility policy. Record tested commits in evidence; do not introduce a workspace SHA pin or update production package versions.

## Shareable explanation: old uses and replacements

1. **Account selection.** The parent took the first enabled member's venue and its account, and child requests initially inherited that account; children on other venues then replaced it. The parent will instead belong to the configured user, with a saved map of eligible venues to execution accounts. Each child selects its account directly from that map using its own market ID, independent of member order.
2. **Risk checks.** The parent currently runs a single-venue risk check against the nominal venue/account, although its children can execute elsewhere. The replacement separates checks on the whole instruction from checks on all eligible execution scopes, followed by the existing checks on each actual child. Flipped members use their own side and complemented YES price for member-specific checks.
3. **Permissions.** Bucket submission already checks member-venue permissions, but parent reads, amendments and cancellations use its one nominal account. The replacement checks the saved account scope consistently: viewing the whole parent requires visibility of every account; actions affecting the whole parent require trade permission on every account. Later bucket edits cannot change those permissions for an existing parent.
4. **Storage and lookup.** The nominal venue currently supplies the parent projection's database key and participates in account filters, pagination and cached lookup. The replacement uses the stable parent ID and a private managed-order storage namespace, with an explicit saved execution scope for account and venue membership queries. Existing parent rows and snapshots require a reviewed migration; venue children keep their existing keys and attribution.
5. **Reporting and lifecycle.** Public parent responses currently show the nominal venue/account even when all fills occur elsewhere. The replacement reports no single execution venue/account, lists eligible venues/accounts explicitly, and continues to derive actual execution and fills from children. Cancellation targets each child's real venue, while a halt of any eligible venue reaches the whole parent, as the routed parent's existing `venues()` method already intends.

## Proposed contract and ownership

Use the existing user identity; `owner_id` on ManagedOrder already means a nested managed-order relationship and must not be repurposed as user ownership. Keep book and trader attribution unchanged.

A routed parent, including a bucket with only one enabled member, has `venue=null`, `account=null`, and an explicit execution scope containing a deterministically ordered venue-to-Account map. This scope is captured with the enabled bucket definition before routing begins and saved in the durable snapshot. Existing single-venue parents and venue orders retain their concrete venue/account. Validate that a venue-held order always has a concrete venue; nullable parent fields must never reach adapters, fills, or ledger keys.

Reject a caller-supplied singular account for a new bucket instruction with an actionable error: it cannot specify the accounts of several venues. Initially use the engine's configured per-venue accounts, retaining the established default-account convention where applicable. A new API for selecting several accounts is outside this ticket. Validate each selected account's venue against its member venue and the configured execution adapter; do not claim arbitrary subaccount routing when an adapter only supports one configured identity.

Persist the exact account selection; a restart with conflicting execution configuration must block new dispatch for that parent with an actionable diagnostic rather than silently select a different account. Preserve access to historical records and the ability to reconcile or clean up through the original accounts when those adapters are available.

D4 accepted on 25 October 2026: prepare the enabled bucket and its account map once in the trusted submission flow. HTTP permission checks, parent risk, durable persistence and routing consume that same prepared snapshot; the engine must not reread the mutable bucket after authorization. Preparation is internal, never trusted from a caller-supplied account-scope payload. Direct SDK calls reuse the preparation path while retaining the existing execution-owner checks. Bind any supported same-client-ID retry to the saved original instruction/scope, not a newly edited bucket. Test a bucket edit interleaved between authorization and dispatch.

```text
user + bucket instruction + client order ID
                  |
       enabled bucket snapshot
       saved venue -> account map
                  |
     instruction / scope validation
     authorization + parent risk
                  |
        durable routed parent
                  |
      existing fee-aware allocator
           /                 \
  member market A       flipped member B
  venue/account A       venue/account B
  child risk/send       converted child risk/send
           \                 /
          child status / fills
                  |
      aggregate parent and report
```

## Risk policy agreed in architecture review

D2 accepted on 25 October 2026: conservative admission requires capacity for the full requested quantity at every eligible destination. Keep the existing risk rechecks on actual children; admission does not reserve capacity or guarantee execution.

Run instruction-wide rules once: user/book validity, price and quantity bounds, order notional, applicable duplicate/rate controls and daily loss. Check the bucket restriction itself and every enabled member restriction. Check venue halts across all enabled venues before accepting the instruction, even when today's best price would allocate only to one venue.

For member/account risk, project the full remaining quantity to each eligible member as a conservative admission check: any eligible member may receive that quantity after a price change. Convert flipped side, outcome and price into member coordinates, and use that member's account, market-close time, event, positions and open orders. Alternative destinations are alternatives, not simultaneous allocations: do not add a full parent quantity to every venue's actual exposure or count parent and children as the same resting venue order. Aggregate venue-level bounds once per venue/account rather than once per member.

This policy can reject a parent whose current split would fit limits but whose future full allocation to one member would not. That is an explicit tradeoff for review. An allocation-only admission policy would be less restrictive but would allow accepted parents to stall on later child risk checks. Actual child dispatch always rechecks current risk; admission is neither a funds reservation nor a guarantee of later acceptance. Existing ineffective or incomplete risk rules, such as a venue-notional rule without supplied exposure, must be identified in verification rather than described as enforced.

## Persistence, query and compatibility policy

Retain managed_orders as the authoritative routed-parent snapshot and retain the shared order projection for now. PRE-5's removal of parent copies is a separate issue, not a dependency to implement here.

The shared projection's current `(venue,id)` primary key requires non-null values. Use an explicitly private reserved managed storage namespace for routed parent rows, while keeping their serialized public execution venue null. Centralize conversion between storage identity and execution identity so no caller accidentally treats the storage namespace as a venue. Never configure an adapter/account for that namespace. Parent lookup, including finished parents after restart, must use managed ID lookup rather than scanning only configured adapters.

Persist a normalized parent execution-account membership relation, keyed by parent ID and account key with its concrete venue, alongside each new parent snapshot. Use it for indexed venue/account filtering and scope lookup without scanning and decoding every historical snapshot or fetching current mutable bucket definitions. A parent matches any eligible venue filter and appears once in an unfiltered list. Preserve stable pagination using the private storage key and parent ID; reject incompatible pre-upgrade cursors clearly and require clients to restart pagination.

D3 accepted on 25 October 2026: save the parent snapshot, execution-account membership, shared readable order projection, submission-result bookkeeping and related journal events in one transaction. Refactor journal writes into transaction-owned operations and helpers that do not start or commit transactions themselves. Update in-memory caches only after a successful commit; propagate cancellation/failure without publishing uncommitted state. Reuse this transaction boundary for parent transitions coupled to child intents. Verify rollback and restart with failures injected between writes on both databases.

Add an immutable Alembic revision for the relation and routed-parent projection rekey/backfill. Migrate routed managed snapshots, shared projections and account indexes consistently; preserve parent IDs, client IDs, user identity, statuses, fills, child IDs and actual child venue/account attribution. Preserve old nominal values as legacy provenance, not operative ownership. Do not edit old audit events to pretend they originally contained the new contract.

Infer historical accounts only from unambiguous persisted child/intent evidence and the historical parent account for its matching venue. Never infer a previously unknown execution account from the current configuration. If a legacy live parent lacks sufficient evidence for an enabled venue, require an explicit reviewed mapping before enabling dispatch; do not silently broaden access or move execution. Test and document this case as an upgrade preflight requirement. The migration and startup validation must fail transactionally on conflicting evidence.

Update HTTP response schemas and any experimental Python callers that assume a string parent venue. Scope predicates must be shared by list/detail/amend/cancel and parent-bearing event delivery/replay. The current event socket sends subscribed events without account-level filtering; parent scope filtering must cover both replay and live delivery, and this ticket must not claim that unrelated event types have thereby received a complete security audit.

Require all-account visibility for a complete parent/report and all-account trade permission for a parent action. A caller may still see an authorized individual child through existing child routes without seeing the whole aggregate. Do not expose another venue's fills through a partial-account parent report.

Upgrade both SQLite and PostgreSQL through the existing application-controlled migration path and one execution worker. Back up and verify restore first. Old binaries must not open the upgraded journal; downgrade requires a compatible backup and resolved outstanding venue activity. SDK merge precedes workspace merge, and workspace CI must verify the actual resulting experimental branch tip.

## Verification plan

Verification uses existing PaperVenue and HTTP test fixtures without exchange credentials. Actual results are recorded separately from this design in the linked workspace verification report.

```text
Engine.submit(bucket request) / POST /orders on the standalone trading app
    -> saved scope -> risk -> durable parent
    -> existing router -> two scripted venue adapters
    -> child responses/fills -> parent/report
    -> list/detail/filter/cancel/halt
    -> process restart + migrated database -> same identities/scope

Terminal fixture book -> select bucket -> enter size/worst price
    -> routed preview -> correct per-venue legs
    -> contract checks + frontend build (no order submission UI)
```

1. **Ordering invariance:** run equivalent buckets with member order reversed and unambiguous best prices; assert identical owner, saved account map, risk/permission decisions and economic allocations, ignoring generated IDs and timestamps. Include all allocation on the non-first venue, multi-venue fills, disabled members and flipped YES/NO members. Assert actual requests delivered to each adapter carry that venue's exact named account.
2. **Risk and authorization:** deny a non-first venue/account, halt it, restrict its market, exceed member/account position limits, and exercise mixed-view/trade grants. Verify denial before dispatch where admission requires it, exact child-side checks after flipping, authorized child access, and no parent/report/live-event/replay leakage to a partial-account key. Check the proposed conservative admission tradeoff explicitly.
3. **Lifecycle and storage:** use resting children, partial fills, colliding venue-native order IDs, delayed cancellation confirmations and cancellation/fill races. Cancel or halt through the public entry points and assert every child is addressed on its own venue; parent stays pending until tracked children resolve. Restart working, cancelling and finished parents; verify ID lookup, reports, filters, pagination, saved account scope and unchanged aggregate fills. A changed bucket definition or account configuration must not silently alter the instruction.
4. **Upgrade:** prepare pre-change databases with live and finished routed parents, child evidence, incomplete/conflicting account evidence and ordinary single-venue orders. Apply the new migration on SQLite and PostgreSQL; verify identity/count/state preservation, no duplicate projection after rekey, explicit failure for unsafe mappings, and successful restart/cancel of migrated parents. Repeat normal startup to verify idempotence. Preserve known recovery limitations rather than claim a new all-children reconciliation barrier.
5. **Terminal compatibility:** extend the installed-consumer smoke check with a minimal offline bucket submission and scope/child-account assertions. Run terminal backend tests, generated-contract consistency and frontend build against the exact candidate SDK commit. In a browser with deterministic market fixtures, select a two-venue bucket, enter size and worst price, inspect routed preview legs, reverse member order and compare. Confirm Submit remains disabled as specified on main.
6. **Release evidence:** require SDK native and pure-Python tests, Rust checks and PostgreSQL storage CI, plus workspace terminal CI against the merged experimental branch tip. Record both tested commit hashes and sanitized results. Create linked PRs, run `/review`, resolve findings, merge SDK to `codex/order-lifecycle-storage` first, then reverify and merge prediction_market to `main`.

## Implementation scope proposed for engineering review

Expected SDK areas: trading models; engine bucket admission/child dispatch/risk integration; managed base/controller/routed scope; journal and projection lookup/filtering; membership schema and Alembic migration; server authorization/events; lifecycle documentation; routing/API/recovery/migration/storage tests. Workspace areas: installed-consumer smoke test, terminal integration decision/rollout documentation and any necessary CI compatibility wiring. This necessarily exceeds eight files but introduces no new service or execution worker.

Suggested sequence: define the scope/model and migration contract; implement shared scope/identity helpers and storage; update admission/dispatch and risk; update server authorization/query/event paths; add regressions and consumer verification; run review and land in dependency order. Both worktrees remain isolated throughout. Cross-repository tasks are sequential where the consumer depends on the SDK contract.

## Required verification report

User requirement confirmed on 25 October 2026: deliver a verification report with screenshots and code snippets showing inputs and actual captured outputs. Do not make actual trades. Use offline scripted/PaperVenue adapters with no real exchange execution credentials; browser verification uses deterministic market fixtures.

The final report must include:

- Exact workspace and SDK commits tested, database backend and migration revision, commands or runnable snippets, test counts/results, and CI links where available.
- Minimal SDK and HTTP request snippets, paired with sanitized captured responses demonstrating a venue-neutral parent, its saved execution accounts, and the actual venue/account on each child. Clearly label all examples as simulated execution.
- Before/after or paired runs for reversed bucket members, all allocation to the non-first venue, and a flipped member, with expected versus observed outcomes.
- Evidence for permission/risk denials, venue-scoped halt and cancellation, restart/finished-parent lookup, and legacy migration. Show relevant persisted state or query results without exposing credentials or private account data.
- Screenshots of the terminal's deterministic two-venue bucket preview, including the entered quantity/worst price and resulting venue legs, plus readable captures of SDK/API verification results where useful. Label screenshots by scenario and distinguish preview evidence from simulated execution evidence; the current terminal cannot submit orders.
- A requirement-to-evidence table linking ticket acceptance criteria to tests, screenshots and captured outputs, with explicit pass/fail/not-run status and remaining limitations. Never substitute illustrative output or a mockup for observed results.

Keep the report and its screenshot assets available as reviewable artifacts and link them from both PRs. Finish the report before requesting the final merge decision or merging under the user's conditional authorization. No live execution is part of this report.

## Existing mechanisms to reuse and exclusions

Reuse user identity, captured routable bucket, market-ID venue resolution, fee-aware allocation, per-child risk, parent IDs, managed snapshots, per-child venue cancellation, RoutedLimit.venues(), scripted venue fixtures and existing CI. Do not build a new router, order state machine, account service or terminal trading screen.

Outside scope: public SDK main/release/PyPI, production dependency changes, live trading, PRE-5 projection removal, multi-user/multi-worker execution, a multi-account selection UI, routing tie-break redesign, and the broader uncertain-send/resync redesign documented in routed.md. No unrelated TODO cleanup.

## Approved decisions

D1 retains two linked repository PRs. D2 chooses full-capacity admission at every
eligible destination. D3 requires atomic parent/account/projection persistence.
D4 captures one scope for authorization, risk and execution. D5 requested an
independent review; D6 includes HTTP audit-route permissions and D7 includes
account checks before recovery/cleanup I/O. D8 explicitly approved implementation
with those additions. The user requested all remaining material questions in one
batch and prohibited actual trades. The verification report must include actual
screenshots, runnable input snippets, captured outputs and exact tested commits.

## Engineering review: architecture

Scope accepted in D1. Three findings were resolved with explicit user decisions: (1) D2 chooses conservative full-quantity admission at every eligible destination; (2) D3 makes parent state, scope, projection and related bookkeeping atomic; (3) D4 authorizes and executes the same captured snapshot. No new service, worker, dependency or published artifact is introduced. Existing SDK and terminal build/CI pipelines remain the distribution and compatibility checks.

Migration is deliberately fail-closed for ambiguous historical account evidence. Preflight must identify such cases before upgrade while the old environment is still available; resolve mappings explicitly rather than starting new execution under guessed accounts. Account mismatches after restart block new dispatch rather than rewrite the saved scope. Scoped event delivery checks both replay and live delivery. These policies are already included above and require no additional architecture decision.

## Engineering review: code quality

No additional issues found against the revised plan. Reuse one scope preparation/validation implementation, one authorization predicate for complete parents, one private storage-identity conversion, and transaction-free low-level write helpers called by transaction-owning journal methods. The storage protocol must expose the atomic parent-save operation consistently for both backends; PostgreSQL already inherits the shared journal operations.

Do not spread `venue is None` fallbacks across adapters or mint a synthetic Account. Model validation distinguishes a cross-venue engine parent from a venue-held order. Keep existing single-venue behavior on its existing path. Storage and lifecycle diagrams must describe the actual transaction boundary and distinguish attempted cancellation/resync from confirmed venue outcomes. Prior cancellation/restart fixes in commits `e2df3ba` and merge `27b4ea4` identify an area for regression testing; their state mappings and delayed-confirmation behavior are retained.

## Engineering review: tests and failure modes

Frameworks: SDK uses pytest/anyio with PaperVenue and FastAPI TestClient; SQLite/PostgreSQL contract tests already share scenarios. Terminal uses unittest backend tests plus generated-contract checks and TypeScript/Vite build; browser checks cover observable preview behavior. Existing routed tests cover fee-driven non-first allocation, flips, cross-venue cancellation, scoped halts and delayed-cancellation recovery. Extend these fixtures rather than invent another simulation engine.

The original verification plan plus the accepted D3/D4 regression requirements covers the planned paths. No additional test-policy decision is needed. The following map specifies branches and expected observable outcomes; implementation must add assertions to the named suites and record actual results in the report.

```text
submit: trusted prepare scope [test_routed_limit / test_serve_buckets]
  |-- missing/inactive/empty bucket, missing adapter -> explicit refusal, no send
  |-- singular account or mismatched account/venue -> explicit refusal, no send
  |-- valid -> capture enabled members/accounts exactly once
       |-- concurrent bucket edit -> captured scope remains operative [D4]
       |-- unauthorized account -> 403, no parent dispatch
       |-- global/member halt or member risk denial -> rejection, no dispatch
       |-- all checks pass -> atomic save [test_order_storage]
            |-- failure/cancellation between any writes -> full rollback [D3]
            |-- commit -> update memory and start existing router
                 |-- normal/flipped/split/non-first allocation -> exact child accounts
                 |-- child risk changes -> existing child rejection behavior
                 |-- fills/status repeats -> unchanged cumulative attribution

read/action: parent ID [test_server_trading / test_serve_buckets]
  |-- partial grants -> hide/refuse complete parent and report
  |-- all grants -> list/detail/report/cancel/amend as supported
  |-- venue/account filter -> scope membership, one parent row, stable paging
  |-- live/replayed parent event -> same visibility rule
  |-- unknown/malformed identity or cursor -> explicit error, no fallback account

restart/upgrade [test_routed_recovery / test_owner_migrations / test_postgres_storage]
  |-- fresh DB -> new schema; repeated start -> unchanged state
  |-- legacy, unambiguous -> preserve IDs/children/fills, rekey parent once
  |-- missing/conflicting historical account -> explicit preflight/migration failure
  |-- failure/cancellation during migration -> unchanged old data, reopen safely
  |-- unfinished parent -> restore scope, reconcile/cancel original children
  |-- finished parent -> ID lookup/report still works without live controller entry
  |-- changed account config -> block new dispatch, retain original attribution
  |-- same client-ID retry where supported -> original parent/scope, no duplicate send

terminal [backend unittest / consumer smoke / browser]
  fixture books -> select bucket -> enter size/worst price -> preview venue legs
       |-- reversed members / flipped outcome -> expected equivalent economics
       |-- missing liquidity -> visible remainder
       |-- Submit -> remains disabled (preview only)
```

| Failure mode | Planned error handling and user-visible result | Required evidence |
|---|---|---|
| Bucket changes during permission check | Continue using the authorized captured snapshot | Interleaved edit test, assert adapters receive only captured members |
| Parent save fails after membership/projection write | Roll back the whole transaction; no accepted parent/event/cache update | Inject failure at each boundary on SQLite and PostgreSQL, reopen and compare all rows |
| Historical account evidence is missing or conflicts | Refuse unsafe migration/dispatch with affected parent diagnostic | Legacy fixtures and rollback assertions on both databases |
| Account configuration changes after restart | Refuse new sends through a different account | Restart fixture, zero new sends, unchanged saved identity |
| Partial-account caller requests parent or replays events | Hide aggregate or return permission error; no unauthorized account data | HTTP/report/live-event/replay grant matrix |
| Two venues return the same native order ID | Match children by concrete venue plus native ID | Two-venue cancellation/fill fixture |
| Cancel acknowledgement remains open or races with fill | Retain pending cleanup and cumulative fills; never claim unconfirmed cancellation | Existing cancellation scenarios extended to null-venue parents and restart |
| Parent is finished before restart | Retrieve by managed ID without scanning adapters | Finished-parent detail, cancel/idempotent read and report tests |
| Many parents or repeated scope lookups | Indexed membership filters and batched scope retrieval | Query-count and query-plan assertions; no full-history snapshot scan |

No unaddressed silent critical failure mode was identified in this plan review. The broader uncertain-send/resync limitations remain explicitly outside this change; this is not a claim of production trading readiness or a test result.

## Engineering review: performance

No additional issues found against the planned normalized membership relation. Index concrete venue/account membership for filtered queries, use `EXISTS` or deduplicated joins so a parent is returned once, and load page scopes in a batch rather than one lookup per row. The snapshot remains authoritative; membership is its transactional query projection, never an independent mutable account-selection source.

Parent admission scales with enabled members, not historical parent count. Reuse one view of the engine's current open orders per admission and group by venue/account before per-member checks; actual children retain their existing checks. Keep order/fee planning unchanged. Test query counts with several page sizes and inspect SQLite and PostgreSQL query plans for the membership filters. Do not add speculative caching or a benchmark service.

## Implementation Tasks

- [ ] **T1 (P2)** — SDK scope/admission — implement the venue-neutral routed-parent contract and the D2 full-capacity risk policy; reuse concrete-account child dispatch. Verify reversed members, all allocation to non-first venue, flips, named accounts and conservative rejection of an otherwise valid split.
- [ ] **T2 (P1)** — SDK journal/migrations — implement D3 atomic parent persistence and scope membership, private projection identity and legacy backfill. Verify injected failures, cancellation, restart and migration on both backends.
- [ ] **T3 (P1)** — SDK submission/server — implement D4 single captured scope and consistent scope-based read/action/event authorization. Verify the interleaved bucket edit and the all-account/partial-account permission matrix.

These tasks derive from the three findings above; ordinary implementation steps and the required verification report remain described in their own sections. Human/agent effort estimates are not supplied because this repository has no applicable compression table; measure actual work rather than invent precision. No new tasks arose from code quality, test-policy or performance review.

## Dependencies and worktrees

| Step | Modules | Depends on |
|---|---|---|
| Contract and captured scope | SDK trading and engine | — |
| Atomic storage and migration | SDK engine storage and migrations | Contract |
| Submission, risk, authorization and recovery | SDK engine and server | Contract, storage |
| SDK regression verification | SDK tests | Submission/storage paths |
| Consumer check and terminal evidence | Workspace research, terminal and CI | Candidate SDK |
| Review and landing | Both PRs | All checks and report |

Sequential implementation in two isolated repository worktrees: the consumer depends on the SDK contract and no parallel agent work is required. SDK regression and terminal checks may run independently once the candidate SDK is stable. Merge SDK into the experimental branch, then rerun workspace checks against that resulting branch commit before merging workspace main.

## NOT in scope

- Public SDK main, release/PyPI and production pins: the intended dependency remains the experimental branch.
- Actual trades: the user explicitly prohibited them; all execution evidence is simulated.
- Terminal trading UI: main only previews orders, and enabling execution is a separate product change.
- PRE-5 projection removal: retaining the projection avoids coupling two storage redesigns.
- Multi-user, multi-worker and account-selection UI: reuse current ownership and configured adapters.
- Routing ties and broader uncertain-send/resync redesign: preserve the existing behavior and documented limitations.

## What already exists

Reuse the user owner registry, bucket routable snapshot, fee-aware router, venue-specific child dispatch, child-based cancellation, `RoutedLimit.venues()`, managed snapshot recovery, Alembic upgrades and shared SQLite/PostgreSQL test scenarios. Reuse terminal CI's SDK installation, backend tests, consumer smoke check, schema check and frontend build. No existing TODO file was found; no new deferred TODO is proposed. Existing lifecycle follow-ups remain documented in routed.md and routed-lifecycle-plan.md.

## Independent review resolutions

The fresh-context independent review found two gaps, both accepted by the user:
HTTP `/journal` and `/order-log` now fall under the saved parent-scope access
contract, and account identity checks must precede early recovery reads and
cleanup writes as well as new dispatch. Legacy events resolve durable scope;
unknown parent attribution is withheld from partial-account viewers. A changed
account must remain unresolved rather than be marked lost/final by reading a
replacement account. These additions are part of the approved scope.

## GSTACK REVIEW REPORT

| Review | Trigger | Why | Runs | Status | Findings |
|---|---|---|---|---|---|
| CEO Review | — | Scope strategy | 0 | Not needed | Contract correction |
| Independent plan review | Outside voice | Fresh-context challenge | 1 | Resolved | 2 P1 findings accepted in D6/D7 |
| Eng Review | `/plan-eng-review` | Architecture, code quality, tests, performance | 1 | Clear | 5 findings resolved through explicit decisions |
| Design Review | — | Visual experience | 0 | Not needed | No UI redesign |
| DX Review | — | Developer experience | 0 | Not needed | Existing setup/CI |

- **UNRESOLVED:** 0 plan decisions.
- **VERDICT:** Engineering plan cleared and implementation approved. Code review, offline verification report and satisfactory CI still gate landing. No actual trades are authorized.
