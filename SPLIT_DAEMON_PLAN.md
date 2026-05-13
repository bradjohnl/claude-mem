# claude-mem split-daemon plan (fork of thedotmack/claude-mem)

## Context

Upstream claude-mem runs a single daemon (`worker-service.cjs`) that combines:
- HTTP API serving hook calls (intake — cheap, SQLite I/O only)
- LLM-backed processing of pending observations (drain — GPU-bound)

This conflation breaks idle-only GPU policy: gating daemon startup at the source breaks the hooks (they exit code 2 → blocks `UserPromptSubmit`). See `~/_cowu/Error-Registry.md#ERR-2026-05-13-CLAUDE-MEM-SOURCE-GATE-BROKE-HOOKS-01`.

## Goal

Split into two cooperating daemons:
- **Intake** — always-on, HTTP API on `37700+(uid%100)` (= 37777). SQLite I/O only. No LLM.
- **Drain** — lock-gated lifecycle. No HTTP. Polls SQLite queue, calls a small enrichment llama-server on `:8086` for summarization/observation extraction.

## Design decisions (locked, 2026-05-13)

| Decision | Choice |
|---|---|
| Enrichment model | Qwen3-0.6B-Instruct Q4_K_M GGUF (~400MB disk) |
| Enrichment endpoint | `http://127.0.0.1:8086/v1/chat/completions` (separate `claude-mem-enrich-llama-server.service`) |
| Enrichment context size | 32K (~1.1GB VRAM with Q4 KV cache) |
| Always-on Qwen3.6-35B on `:8085` | Untouched — stays loaded for foreground use |
| Drain lifecycle | Lock-gated (managed by `~/.local/bin/memory-drain-on-lock.sh`) |
| Drain HTTP | None — polls SQLite directly + signal-controlled |
| Env flag | `CLAUDE_MEM_SPLIT_DAEMON=1` to enable; default off |
| Hook freshness trade-off | Stale context during long active sessions; freshens on each lock cycle (user accepted) |
| Install method | Pin `CLAUDE_PLUGIN_ROOT=/home/keepupdragon/IT/Forks/claude-mem/plugin` so upstream auto-updates to marketplace copy are ignored |
| Queue engine | Default SQLite (confirmed via settings.json — no BullMQ) |
| Chroma | Stays in INTAKE (default sentence-transformers CPU embeddings, not llama-server) |

## Phased commits

1. **Phase 1 — no-op refactor** ✅ shipped (commit `faadc684`): `initializeBackground` split into `initializeIntakePhase()` + `initializeProviderPhase()`.
2. **Phase 2 — drain skeleton** ✅ shipped (commit `47a31166`): `src/services/drain-service.ts` + build target.
3. **Phase 3a — flag flip** ✅ shipped (commit `cbf808c1`): `CLAUDE_MEM_SPLIT_DAEMON=1` toggles intake/drain split.
4. **Phase 3b — provider dep graph standalone** ✅ shipped (commit `2ab98705`): drain instantiates `DatabaseManager` + `SessionManager` + all 3 providers without worker-service.
5. **Phase 3c — `provider.startSession()` wired** ✅ shipped (commit `eb41ddb7`): drain calls real LLM path, gated by `CLAUDE_MEM_DRAIN_ACTIVE=1`.
6. **Phase 4 — lifecycle wiring**: ⛔ **DEFERRED — see "Hardware constraint" below**.
7. **Phase 5 — intake bundle slim-down**: ⛔ deferred (depends on Phase 4).

## Hardware constraint (2026-05-13 evening)

Phase 4 originally proposed a dedicated `claude-mem-enrich-llama-server.service` running Qwen3-0.6B Q8_0 on port `:8086`, idle-gated so the foreground Qwen3.6-35B (port `:8085`) stayed VRAM-isolated from drain work.

**Measured cost** (RTX 3080 Laptop, 16 GB VRAM, Qwen3.6-35B always-resident at ~12 GB):
| Enrich ctx | Footprint | Free VRAM |
|-----------:|----------:|----------:|
| 32K        | 1910 MiB  | -27 MiB (rejected by `-fit on`) |
| 16K        | 1904 MiB  | 624 MiB (dangerously tight) |
| 8K         | 1652 MiB  | 875 MiB (workable, but long sessions truncate) |

llama-server's static overhead is ~900 MB regardless of context size (model + compute buffer + slot reservation). Static is the bottleneck, not KV cache.

**User decision**: don't run a second model. Drain points at the existing `:8085` Qwen3.6-35B endpoint — same model handles both foreground and enrichment. This is exactly what the upstream claude-mem already does; we don't need Phase 4 to achieve the GPU gate.

## What the in-place mechanism achieves (no Phase 4 needed for v1)

`~/.local/bin/memory-drain-on-lock.sh` already gates GPU work without the split-daemon split:

- **Foreground** (screen unlocked): 10s level-triggered loop kills any `worker-service.cjs --daemon` that hooks/scripts spawn. Qwen3.6-35B sits resident but idle. Hooks still enqueue to SQLite normally.
- **Locked**: D-Bus signal triggers `on_lock` → `wake_orphan_generators.py` spawns worker, which drains the queue against Qwen3.6-35B. Standard upstream behavior, just gated.

The split-daemon code (Phases 1-3c) is preserved on the fork as **architectural prep** in case future hardware (more VRAM, or a dedicated CPU-only drain target) makes a true split useful. It is NOT required to operate the gate today.

## Risks tracked (historical, kept for reference)

## Risks tracked

1. **Hook timeouts** (60-120s in hooks.json): intake must stay sub-second. ✓ Just SQLite inserts.
2. **`pending_messages` orphan sweep**: should move from intake to drain to avoid races. Defer to Phase 3.
3. **MCP self-check** (worker-service.ts:413): can stay in intake as a connectivity test. The MCP search server itself responds to keyword queries via DB.
4. **Long sessions with truncated summary input**: 32K ctx covers virtually all sessions but watch for quality regression on >20-turn sessions.

## Files of interest

- `src/services/worker-service.ts` — primary refactor target (~1250 lines)
- `src/services/worker/OpenRouterProvider.ts:450` — `max_tokens: 4096` output cap
- `src/services/worker/http/shared.ts` — `setIngestContext` / `attachIngestGeneratorStarter`
- `src/services/worker/http/routes/SessionRoutes.ts` — `ensureGeneratorRunning`
- `scripts/build-hooks.js` — bundle build pipeline
- `plugin/scripts/worker-wrapper.cjs` — daemon supervisor
