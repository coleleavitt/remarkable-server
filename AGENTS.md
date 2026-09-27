# remarkable-server Agent Instructions

`CLAUDE.md` is the primary guide; read it first. The rules below are the safety-critical subset
for agents that don't read it.

- This server is the **live source of truth for a real reMarkable tablet**. Never change the
  tablet's sync wire behaviour or stored bytes, and never invalidate existing JWTs
  (`<storage>/jwt_secret`), unless that is the explicit task — and then prove compatibility with tests.
- Never deploy, ssh to the Linode, or touch the tablet unless the user asks. Deploys follow
  `.claude/skills/linode-deploy/SKILL.md` (backup first).
- Work on a branch in a worktree, open a PR; never push to `master`, force-push, or `git stash`.
- No silent data loss: quarantine instead of delete; refuse to rewrite what can't be parsed.
- Never commit secrets, `*.pem`, `jwt_secret`, real notebooks (`test-storage/`,
  `remarkable-storage/`), or the tablet's serial number.
- Validation before any push:
  `cargo fmt --check`; no `^(src|tests)/.*warning` in `cargo build --all-targets --locked --message-format=short`;
  `cargo test --workspace --all-targets --locked`; `cargo +stable test --workspace --all-targets --locked`.
- Navigate with codegraph (`codegraph explore|query|callers|impact`, `codegraph sync` after edits).
