---
description: Reproduce a reported remarkable-server bug with expected vs actual evidence
argument-hint: <bug description>
---

Reproduce: $ARGUMENTS

1. Restate the claim as: preconditions → steps → expected → actual.
2. Find the code path (`codegraph explore`/`callers`). Name file:line.
3. Write the smallest deterministic test that shows it (unit test, or router-level with
   `tower::ServiceExt::oneshot`, or a local axum mock server for provider APIs). No real network,
   no global env mutation, no sleeps.
4. Run it; paste the failing output (expected vs actual).
5. If it doesn't reproduce, say so and show what you tried.
6. Never use live data: for storage-shaped bugs, copy a backup (`~/remarkable-backups/*.tgz`)
   into a scratch dir and run the server locally on `127.0.0.1` against the copy.

Stop after reproducing unless asked to fix.
