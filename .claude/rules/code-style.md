# Code style (remarkable-server)

- Rust 2021, async on tokio, HTTP on axum 0.8. Format with the repo `rustfmt.toml`
  (nightly-only options; the default local toolchain is `rustc-master`, CI formats with it too).
- Zero warnings in this crate (CI gates `^(src|tests)/.*warning`). Warnings from git
  dependencies (`remarkable-rs`) are not ours.
- Errors: return `crate::error::ServerError` / `Result`; map to HTTP status in one place
  (`src/error.rs`). No `unwrap()` on request input; no panics in handlers.
- Blocking work (SQLite, parsing big inputs, subprocesses) off the async workers:
  `spawn_blocking` or a dedicated thread, with bounded concurrency where input is untrusted.
- Each feature owns its SQLite DB; migrations are additive and idempotent (`PRAGMA table_info`
  guards, table rebuilds in one transaction), and must run on existing production databases.
- Config via env/`AppState` builders, injectable in tests (no reading env inside library logic
  that tests need to vary).
- Keep modules focused; match the density and naming of surrounding code. Doc comments explain
  *why* (tablet behaviour, protocol quirks, safety reasons).
- Stays buildable on stable Rust (CI `stable` job); no nightly-only language features.
