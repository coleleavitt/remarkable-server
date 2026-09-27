# Testing rules (remarkable-server)

## Required before every push
```sh
cargo fmt --check
cargo build --all-targets --locked --message-format=short 2>&1 | grep -E '^(src|tests)/.*warning'   # empty
cargo test --workspace --all-targets --locked
cargo +stable test --workspace --all-targets --locked
```
Before merging: test the branch **merged with current `origin/master`** — textually clean merges
have broken compilation and semantics here (changed constructors, shared helpers).

## Strategy
- Every behaviour change gets a test that fails without it (check by reverting in a scratch copy).
- Deterministic only: no sleeps as synchronisation (use `tokio::time::pause`/`start_paused`,
  channels, barriers), no global env mutation (inject config/tokens), no real network (local axum
  mock servers bound to 127.0.0.1:0), no wall-clock limits deciding which assertion fires
  (see `COUNTED` in `src/feeds.rs`). CI runners are slower than dev machines.
- Router-level tests through `create_router`/`feature_routes` with `tower::ServiceExt::oneshot`
  for auth/limits/wiring.
- Tablet compatibility: golden JWTs, byte-identical stored blobs, unchanged status codes.
- Parsers/codecs: property tests (`proptest`) against a reference (serde_json, whole-string decode).
- Migrations: open an old-schema DB with data and assert the upgrade is lossless and idempotent.
- Data-safety features (cloud sync, GC, quarantine): multi-run scenario tests covering crashes
  between transfer and state write, and both sides changing.

## Known caveats
- `tests/screenshare_viewer.rs` needs the `webrtc` dev-dep (upstream `v0.17.x`, pinned rev).
- Some tests need `pdftotext`/`tesseract` on PATH (CI installs `poppler-utils tesseract-ocr`).
