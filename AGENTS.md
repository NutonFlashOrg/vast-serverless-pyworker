# AGENTS.md — vast-serverless-pyworker

Repo-local orientation for the Vast.ai container worker runtime. Operational rules live in the `infrastructure-changes` skill at the workspace root.

## Role in product

Worker code that runs inside Vast.ai containers built by `comfy-vast-serverless`. Handles request transformation for ComfyUI JSON, ACE, and WAN workers, plus PyWorker bootstrap and runtime calibration.

## Entrypoints

- `start_server.sh` — container entrypoint; synced to `comfy-vast-serverless/` via `make sync-start-server` on that side
- `workers/` — per-worker runtimes (ComfyUI JSON, ACE, WAN)
- `lib/` — request transformation utilities

## Local commands

```bash
make lint
make check
```

## Branch policy

`main` by default unless a feature branch is in flight.

## What to read before changing

- [`.claude/skills/infrastructure-changes/SKILL.md`](../.claude/skills/infrastructure-changes/SKILL.md)
- [`comfy-vast-serverless/AGENTS.md`](../comfy-vast-serverless/AGENTS.md) — the image-build side of the pair

## Non-negotiable invariants

- `start_server.sh` is the contract with the container environment — changes here must be mirrored to `comfy-vast-serverless` via `make sync-start-server`.
- Keep worker runtime changes minimal; avoid unnecessary dependency additions (image size + cold-start cost).
