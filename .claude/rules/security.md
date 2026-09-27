# Security rules (remarkable-server)

## Secrets and local data — never commit
`jwt_secret`, `*.pem`/certs, `/etc/remarkable-server/env` values (`ADMIN_TOKEN`), provider
tokens, `test-storage/`, `remarkable-storage/`, `crash-dumps/`, backups, and the tablet's serial
number. If one lands in a branch commit, squash-merge so it never reaches `master` history.
Test fixtures that look like credentials must be built at runtime (GitGuardian scans PR commits).

## Auth
- Every new route: decide device/user token (`auth_user`/`caller`), admin (`require_admin`/
  `check_admin`), or deliberately public (document why, e.g. the crash sink). Default is auth.
- Existing tokens must keep verifying (HS256/HS512 golden tests in `src/device.rs`).
- Secrets never in logs, API responses, or error strings (URLs may carry userinfo).

## Untrusted input
Feeds/HTML, email, CalDAV/ICS, provider APIs, uploads, crash reports: bound bytes, element counts,
nesting, CPU time, and concurrency; stream large bodies to disk (`src/upload.rs`); guard SSRF for
user-supplied URLs (see `calendar_providers/dns.rs`); path-safety helpers for anything touching the
filesystem (`integrations/sync.rs` `safe_components`, `create_dirs_within`).

## Listeners open to the internet
The MQTT broker (:8883) and the SMTP server take connections before any authentication. Each
needs, before the peer is authenticated: a deadline (handshake + first command), a cap on what
is buffered, a cap on concurrent connections, and an accept loop that survives `accept()` errors
(EMFILE). Outbound HTTP to user-given hosts: no automatic redirects, or `dns::GuardedResolver`.
Subprocesses on untrusted input (`pdftotext`, recognisers): a deadline and an output cap
(`search::run_bounded`), off the async workers. Secrets: compare with `api::token_matches`;
never put a long-lived secret in a cookie (derive a purpose-bound MAC instead).

## Supply chain
`Cargo.lock` is committed; keep lock diffs minimal; run `cargo audit`. Prefer crates already in the
tree; justify new ones (maintenance, license, transitive deps). Git deps pinned by `rev`.

## Destructive operations
Nothing is hard-deleted by default: quarantine (`.rms-remote-deleted/`), dry-run defaults
(`/admin/storage/gc`), refuse to rewrite unparseable roots. No `git push --force`, no `git stash`,
no changes on the Linode without an explicit request.

Guard the sink, not the paths to it: before a write/delete/move of user data, check the invariant
right there (e.g. `integrations/sync.rs` `guard_overwrite`: local content never recorded as synced
is quarantined before a download replaces it). Enumerate sinks with `codegraph callers <fn>`
and test each; decision logic upstream (plans, conflict strategies, case matching) may be wrong
without losing data.
