# remarkable-server / screenshare — Roadmap

A self-hosted reMarkable cloud + screen-share viewer. This roadmap is
**demand-weighted**: priorities come from what the reMarkable and MOSS
communities actually ask for (a read-only pull of both Discords + a survey of
rmfakecloud, MOSS, and the wider ecosystem, Sept 2026), not from what merely
looks good on a feature grid.

**Positioning.** Self-hosted, own-your-data, no subscription paywall,
**open-source**. The community is privacy- and OSS-leaning and markedly
allergic to closed/paywalled/"AI-slop" framing — features here are pitched as
plumbing you control, not as a product upsell.

Legend: ✅ shipped · 🔵 in progress · ⬜ planned · 🧪 spike/research.

---

## Tier 0 — Already shipped (the baseline)

These are done and, in several cases, ahead of rmfakecloud:

- ✅ **Self-hosted sync** — protocol v1.0 / v1.5 / v2 / v3 + gentree (v4 is
  partial: root/file routes only); no 50-day expiry, no Connect gating
  (self-hosting *is* the escape from the paywall the community complains about).
- ✅ **Device pairing + auth** — one-time pairing code, per-install random
  `jwt_secret`, device→user JWT bundle (software 3.28 OAuth device flow).
- ✅ **Generation-guarded root writes** — optimistic concurrency
  (`set_root_if`, GCS `if-generation-match` semantics). Like GCS, a sync15
  root write *without* the header is unconditional; a present but malformed
  header (or `x-goog-hash`) is rejected with 400 (#19).
- ✅ **WebSocket sync push** — `SyncComplete` on `/notifications/ws`, so a
  tablet learns about changes without polling. Sent on gentree commits,
  server-side uploads and `sync-complete`; a sync v3 root `PUT` only broadcasts
  when the client sets `broadcast: true`.
- ✅ **Handwriting** — convert + **full-text handwriting search** (rmfakecloud
  has convert only, no search). Recognition runs **locally** (the `tesseract`
  binary, or any recogniser via `HWR_COMMAND`) — no MyScript keys, no paywall.
- ✅ **Screen-share browser viewer** — `/screenshare/view`, WebRTC from the
  tablet, MQTT **and** REST-room brokers, with a **pen-tip presenter cursor**
  (the "show the pen pointer" ask) and correct cursor/frame pairing.
- ✅ **MDM** — device instruction queue (`/mdm/*`, `/admin/mdm/*`); nobody else
  in the ecosystem has this.
- ✅ **Sharing** — share links + share-by-email.
- ✅ **Telemetry / usage** — tablet reports kept (rotated), screen-share usage
  history.
- ✅ **Cloud-storage OAuth** — Google Drive / Dropbox / OneDrive integration
  scaffolding; sync confined to `<storage>/integrations`, path-traversal and
  symlink-safe, provider grants revoked on disconnect (#16).
- ✅ **Security hardening (Sept 2026 review)** — OAuth device-code sign-ins need
  owner approval (#18); deleting or re-pairing a device revokes its tokens, and
  users only see and delete their own devices (#17); pairing codes are
  admin-only (#20); a device can't approve its own passcode reset (#20);
  firmware downloads are pinned to the device model (#20); `/debug/clear` is
  admin-only (#14). Server-side uploads refuse to rewrite a root index they
  can't fully parse, instead of dropping entries (#19).

---

## Tier 1 — Reliability & parity (near-term, demand-driven)

The community's top *concrete* pains. Small, bounded, high-value.

- ✅ **Atomic durable writes** *(#12)* — `root.json` and blobs used to persist
  with in-place `fs::write`; a crash mid-write could truncate `root.json` (the
  server then fails to start until it's repaired) or leave a torn blob that the
  tablet syncs as corrupt. The community's "corrupt root empties the cloud"
  failure is this class of bug. Now: write-temp → fsync → atomic rename.
- ✅ **SQLite sync index (`sync.db`)** *(#13)* — root
  hash/generation as a compare-and-swap in one SQLite transaction (safe across
  processes; `root.json` kept as a mirror and adopted if a rolled-back build
  moved it ahead), a `blobs` table replacing `meta/*.meta`, and a derived-only
  projection of the index files (the tablet is still served the stored bytes).
  Adds a read-only unreachable-blob report.
- ⬜ **Large-file upload robustness** — stream uploads to disk instead of
  buffering the full body in memory: every upload route takes `Bytes` (whole
  file in RAM) — sync v3 `put_file`, sync15 `blob_put`, gentree `PutFile`, the
  v2/v4 blob PUTs in `protocol.rs`, and `documents::upload_v2`; verify checksum
  on the fly. The official cloud's
  `302 → Google-upload` redirect that resets progress to 0% does **not** apply
  here (we accept the PUT directly) — document that as a self-hosting win, and
  add resumable/chunked upload only if a client needs it. *Effort: M.*
- ⬜ **Skip re-pair across migrations** — persist device serial↔identity so
  storage moves / binary swaps don't force re-pairing (today `jwt_secret` +
  `devices.db` must move together or every token invalidates). Harden + document.
  *Effort: S–M.*
- ⬜ **Better local handwriting recognition** — local OCR already ships
  (Tesseract by default, pluggable via `HWR_COMMAND`, e.g.
  `contrib/hwr/trocr_hwr.py`), but Tesseract is a *print* OCR. Next: make a
  handwriting model the documented default, and optionally a
  MyScript/iink-compatible backend for people who have keys. Directly answers a
  recurring, paywalled pain (and rmfakecloud's #1 request). *Effort: M.*
- ⬜ **Integration hub (phase 1)** — first-class **WebDAV / Nextcloud** export
  of synced docs (most-requested integration), then CalDAV and an Obsidian-
  friendly export. Replaces the N brittle per-user scripts people run today.
  *Effort: M per integration.*
- ⬜ **Offline-cache guidance / API** — support clients doing offline-first
  with per-folder keep/download semantics (server-side hints/flags). *Effort: M.*

---

## Tier 2 — Differentiators (deliberate bets, not community-requested)

Greenfield across the whole ecosystem. Build because we choose to lead here —
**note:** the hobbyist community did *not* ask for these (0 Discord mentions of
OIDC/SSO/SAML/LDAP), so they target teams/enterprise, not the current base.

- ⬜ **Multi-tenant users + RBAC** — real multiple-user model with roles, beyond
  today's device-centric single tenant. Prerequisite for teams/collab.
  *Effort: L.*
- 🧪 **Collaboration** — staged, because true real-time co-edit is unsolved
  ecosystem-wide (no CRDT/OT anywhere; "not real-time enough for real collab"
  is the current state of the art):
  1. ⬜ **Shared folders / cross-user document sharing** (async, permissioned).
  2. ⬜ **Async merge / conflict resolution** ("which version wins" is an open
     ask even in MOSS).
  3. 🧪 **Real-time co-editing (CRDT)** on the v6 lines model — research spike
     first; large. *Effort: XL.*
- ⬜ **Enterprise identity — OIDC / SSO / SAML / LDAP** — genuinely first in
  this niche (rmfakecloud attempted OIDC 3× and closed every PR unmerged).
  Front-door SSO + provisioning for the multi-tenant model above. *Effort: L.*
- ⬜ **Fleet MDM** — build on the existing MDM primitives: policy push, remote
  config, bulk provisioning. Pairs with enterprise. *Effort: M–L.*
- ⬜ **Server-hosted template / content store** — MOSS is doing this client-side;
  the server-hosted library (templates, splash/lock screens) is open. *Effort: M.*

---

## Tier 3 — Hard shared infrastructure

- 🧪 **Faithful v6 `.rm` rendering + native `.rmdoc` export** — the perennial
  ecosystem blocker; every client renders PDF/ePub but chokes on `.rm`, and
  `.rmdoc` export is repeatedly deferred. **Adopt/track** an existing renderer
  (`rmscene`/`rmc`, or RedTTG's `librm_lines`) rather than reinvent; expose
  server-side render + `.rmdoc` export. Continuous work that shifts with each
  firmware bump. *Effort: XL / ongoing.*

---

## Explicit non-goals (for now)

- No closed-source or paywalled tiers — it would alienate the target community.
- No native mobile/desktop client of our own — MOSS and the official apps cover
  the client; we point them at this server (MOSS already targets rmfakecloud /
  custom backends, so interop is the play).
- No on-device OS layer — that's XOVI / AppLoad / Toltec / Oxide territory.

---

## Evidence

- Community demand: read-only pull of the reMarkable + MOSS Discords
  (feature/dev/roadmap channels) + GitHub issues/PRs/milestones for rmfakecloud,
  MOSS/RedTTG, and ecosystem projects (rmscene, rmc, librm_lines, XOVI, Toltec,
  Scrybble, rmapi, remarkable-mcp), Sept 2026.
- Key finding that shaped the tiers: the loud demand is **reliable no-paywall
  sync, v6/.rmdoc rendering, low-latency screen-share/presenter, local OCR, and
  integrations** — collaboration and enterprise identity are unclaimed
  *differentiators*, not current requests.
