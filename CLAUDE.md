# remarkable-server Claude Instructions

## Mission
A self-hosted replacement for the reMarkable cloud, written in Rust (axum/tokio). It is the
**live source of truth for a real tablet** (paired as `local-user`) at
`https://remarkable.unwrap.rs` (Linode, nginx → `127.0.0.1:3100`; screenshare MQTT broker on
`:8883`). Whatever the tablet syncs lives only here and in backups. Preserve, above all:
- the tablet's sync (`/sync/v3/*`, `sync15`, `gentree`) and its stored bytes, byte for byte;
- the tablet's auth (device/user JWTs signed with `<storage>/jwt_secret`; existing tokens must keep verifying);
- no silent data loss anywhere (documents, cloud-sync files, read-later, calendars).

## Architecture First
Layers, from the core outward. New features compose on these; don't bypass them.
- **Storage** (`src/storage.rs`): content-addressed blobs + `sync.db` (root CAS on generation, blob
  catalogue, parsed index projection, GC). Blob writes go through `put_with_hash` /
  `put_file_with_hash` (both take the `blob_writes` GC lock).
- **Sync protocols** (`src/api.rs`, `src/sync15.rs`, `src/protocol.rs`, `src/gentree.rs`,
  `src/upload.rs` streaming, `src/json_scan.rs`): wire compatibility with the tablet.
- **Server-side documents** (`src/documents.rs`): the only way server code adds to the tree;
  strict root parsing (`parse_root`), batched commits, never rewrite a root it can't parse.
- **Auth & devices** (`src/device.rs`, `src/oauth.rs`, `src/passcode.rs`): pairing, JWTs,
  revocation epochs, session revocation broadcast.
- **Realtime** (`src/notifications.rs`, `src/mqtt_ws.rs` (opt-in `/mqtt`), `src/screenshare*.rs`).
- **Features** (each owns its own SQLite DB under storage): read-later (`src/readlater*.rs`),
  calendars (`src/calendar*.rs`, `src/calendar_providers/`), feeds (`src/feeds.rs`), email
  (`src/email*.rs`), cloud integrations (`src/integrations/`), search (`src/search*.rs`,
  `src/hw_search.rs`), handwriting (`src/handwriting.rs`), firmware, MDM, crash sink, versions.
- **Wiring** (`src/lib.rs` routers, `src/main.rs` startup/env). Production router =
  `create_router` + `feature_routes` (+ opt-in `/mqtt`, screenshare viewer).

## Current State
See `ROADMAP.md` (shipped + follow-ups) and `GAP_ANALYSIS.md` (endpoint coverage, MQTT evidence).
Open: PR #40 (cloud-sync per-folder state) is blocked on case-clash data-loss paths for
case-insensitive providers (Dropbox/OneDrive); do not merge until no adversarial scenario loses
local content. `webrtc` dev-dep waits for an upstream 0.17.3 release.

## Working Rules
- Work in a git worktree on a branch (`../rms-wt-<topic>`), open a PR, never push to `master`,
  never force-push, never `git stash`. Merge only after CI is green and the branch was tested
  merged with current `master`. Squash-merge when an intermediate commit contains a secret,
  the tablet serial, or a credential-looking fixture.
- Match the surrounding style; code is `rustfmt`-formatted with the repo `rustfmt.toml`
  (nightly options). Zero compiler warnings in this crate.
- Tablet-facing paths: no behaviour change unless that is the task; prove compatibility with tests.
- Never destructive by default: quarantine instead of delete, dry-run defaults for admin ops,
  refuse to rewrite what can't be parsed.
- Tests are deterministic: no sleep-as-sync, no global env mutation (inject config), no real
  network (local axum mock servers), no wall-clock limits deciding which assertion trips.
- See `.claude/rules/` for code style, security and testing detail.

## Validation
```sh
cargo fmt --check
cargo build --all-targets --locked --message-format=short 2>&1 | grep -E '^(src|tests)/.*warning'   # must print nothing
cargo test --workspace --all-targets --locked            # rustc-master (default toolchain)
cargo +stable test --workspace --all-targets --locked    # CI's second job
cargo audit                                              # should be clean
```
CI (`.github/workflows/ci.yml`): `rustc-master` job (latest rust-lang main, unpinned) + `stable` job.

## Code navigation (codegraph)
The repo is indexed by codegraph (`.codegraph/`, ignored). Prefer it over grep+read loops:
```sh
codegraph sync                          # after pulling or editing
codegraph explore "CloudSync reconcile"  # source + call paths for an area
codegraph query match_case               # find a symbol
codegraph callers set_root_if            # who calls it
codegraph impact put_with_hash           # blast radius before changing it
```
The codegraph MCP server exposes the same (`codegraph_explore` with `projectPath`).

## Deploy
Only on request. Follow `.claude/skills/linode-deploy/SKILL.md` (backup first, zigbuild for
glibc 2.39, verify health/devices/logs after). Full ops detail: `DEPLOYMENT.md`.

## Project Claude Layout
- `CLAUDE.md` (this file) is primary; `AGENTS.md` mirrors the safety-critical parts.
- `.claude/agents/` researcher (read-only) and verifier; `.claude/commands/` audit and repro;
  `.claude/rules/` code style, security, testing; `.claude/skills/linode-deploy/`.
- `MEMORY.md` is local-only scratch (gitignored).
