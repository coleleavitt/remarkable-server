---
description: Architecture and correctness audit of an area of remarkable-server
argument-hint: <area, file, or PR number>
---

Audit $ARGUMENTS.

1. Scope it with codegraph (`codegraph explore`, `codegraph impact`) and list the files/symbols in play.
2. Check layer boundaries (see CLAUDE.md "Architecture First"): server code adds documents only via
   `src/documents.rs`; blob writes only via `Storage::put_with_hash`/`put_file_with_hash`; auth only via
   `DeviceManager`; features keep their own DB.
3. Correctness: error paths, partial failure, crash between write and state save, concurrency,
   migrations on existing production databases.
4. Safety: tablet wire compatibility, token validity, silent data loss, destructive ops without
   quarantine/dry-run, unbounded CPU/memory on untrusted input, secrets in logs/responses.
5. Tests: do they exercise failure paths deterministically?

Output a ranked findings list (critical/major/minor/nit) with file:line, a concrete failure scenario,
evidence (ideally a reproducing test), and a suggested fix. Do not edit code unless asked.
