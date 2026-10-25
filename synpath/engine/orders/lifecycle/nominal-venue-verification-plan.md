# PRE-6 verification plan

Prepared during `/plan-eng-review`, 25 October 2026. Status: planned, not executed. Full design: [nominal-venue-removal-design.md](nominal-venue-removal-design.md). Implementation awaits final human approval. All execution is offline through scripted venues; do not use real trading credentials or place actual trades.

## Affected pages and routes

- Standalone SDK trading app: `POST /orders`, `GET /orders`, `GET /orders/{id}`, `PATCH /orders/{id}` where supported, and `DELETE /orders/{id}`. Confirm mounted prefixes for any other server configuration rather than assuming `/trading`.
- Bucket report/preview routes: use the existing router's paths and test fixtures; assert complete-parent visibility across all captured accounts.
- SDK event socket and replay: verify parent and report events under complete and partial grants.
- Terminal at `http://127.0.0.1:5173`: select bucket, execution ticket and routed preview. Use deterministic market-data fixtures. Order submission remains disabled.

## Key interactions to verify

1. Submit a bucket with two named accounts through `Engine.submit()` and through HTTP. Observe null parent execution venue/account, stable user identity and a saved map of eligible execution accounts. Verify exact concrete venue/account on requests received by each scripted adapter.
2. Reverse otherwise identical member order with distinct prices. Compare owners, account maps, permissions, risk decisions and economic allocation, excluding generated IDs/timestamps.
3. Make the non-first venue strictly cheaper and deep enough for the whole order. Observe all fills there while parent execution venue/account remain null.
4. Flip a member. Verify its actual outcome, side and YES price are converted correctly; parent quantities/prices remain in bucket terms.
5. Revoke one member-account grant, halt a non-first venue, and exceed its position limit in separate scenarios. Observe denial according to the agreed full-capacity admission policy before dispatch.
6. Interleave a bucket edit after permission checks. Observe execution only on the originally authorized captured members/accounts.
7. Submit resting children, cancel the parent, delay one cancellation acknowledgement, then deliver a final status or fill. Observe pending cleanup until children resolve and accurate aggregate fills.
8. Restart working, cancelling and finished parents. Observe original scope/IDs, lookup and reports; account configuration changes never redirect new sends.
9. On the terminal, enter a quantity and worst price for deterministic two-venue books. Observe correct routed preview legs and remainder; reverse members and repeat. Capture screenshots with the entered fields and resulting legs visible.

## Edge cases

- Missing/inactive bucket, no enabled members, missing adapter, mismatched account venue and caller-supplied singular account.
- Partial view/trade grants: refuse or hide complete aggregate/report and scoped events, while preserving authorized individual-child access.
- Same venue-native child ID on two distinct venues, duplicate child status messages, partial/full fills during cancellation.
- Venue/account filters and pagination across live and finished parents: return each matching parent once, use saved membership, and handle incompatible old cursors explicitly.
- Conservative capacity example: order 100 with capacity 60 on each venue is rejected even though a 50/50 split would fit. Label this intentional policy in the report.
- Missing/conflicting legacy account evidence: explicit preflight/migration failure, no guessed current account.
- Same-client-ID retry where supported: original identity/scope and no duplicate send. Document any unsupported existing bucket retry behavior rather than claim an untested guarantee.

## Critical paths

| Path | Test suite / evidence | Required result |
|---|---|---|
| SDK submit to actual child requests | `tests/test_routed_limit.py` and captured request/response snippets | Exact venue accounts; member-order invariance; flipped correctness |
| HTTP permissions to dispatch | `tests/test_server_trading.py`, `tests/test_serve_buckets.py` | No unauthorized sends or parent aggregate leakage |
| Captured snapshot under concurrent edit | HTTP/engine interleaving fixture | Authorized snapshot equals executed snapshot |
| Atomic parent save | `tests/test_order_storage.py` plus portable PostgreSQL scenarios | Inject failure/cancellation between each write; all rows/events roll back |
| Legacy migration | `tests/test_owner_migrations.py` on SQLite/PostgreSQL | Preserve identities/fills, one rekey, unsafe mapping fails transactionally |
| Cancellation and restart | `tests/test_routed_recovery.py` and existing pending-cancellation regressions | No premature completion or new allocation while cancelling |
| Query scaling | Membership-filter query plans and page query counts | Indexed membership and bounded queries per page |
| Terminal integration | Backend unittest, consumer smoke, contract check, frontend build | Compatible installed candidate SDK and unchanged preview-only terminal |
| Browser evidence | Deterministic bucket screenshots | Visible inputs, venue legs and remainder; no simulated order presented as a live trade |

## Evidence and commands

Run SDK targeted tests first, then the existing offline SDK suite with its live/demo exclusions, pure-Python variant and required native/Rust checks. PostgreSQL scenarios require a disposable database through `SYNPATH_TEST_POSTGRES_DSN`; a skipped database suite is not a pass. Require CI's PostgreSQL job or equivalent actual local results.

Workspace checks use its existing commands:

```sh
uv run --project terminal --locked python -m unittest discover -s terminal/backend/tests -v
uv run --project terminal --locked python research/synpath-public-testing/check_install.py
npm --prefix terminal/frontend run check:contracts
npm --prefix terminal/frontend run build
```

Candidate SDK worktrees are deliberately on a feature branch. The setup script currently requires a clean clone on the experimental branch and `.git` to be a directory. Use a documented isolated candidate-install arrangement for pre-merge tests; do not reset the original checkout, claim that setup verified a feature worktree, or add a general setup bypass. After merging the SDK PR, run the normal workspace setup and CI against the resulting experimental branch tip and record its actual hash.

For every scenario, save the exact command or short runnable input snippet, expected result, captured sanitized output, pass/fail/not-run status and limitation. Use fixture account names only. Screenshots must be actual captures, linked by scenario; no mockup may stand in for observed behavior. Record SDK/workspace hashes, schema revision, test totals and CI links. Report actual child fills as simulated, never real. Link the final requirement-to-evidence report and screenshot assets from both PRs before final landing.
