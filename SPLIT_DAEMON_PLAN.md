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

1. **Phase 1 — no-op refactor (this session, branch `refactor/initialize-phases`)**: split `initializeBackground` in `src/services/worker-service.ts` into `initializeIntakePhase()` + `initializeProviderPhase()`. Both run sequentially. Build, smoke-test all 5 hooks, push.
2. **Phase 2** — new `src/services/drain-service.ts` containing only the provider-phase logic + a polling loop. New `scripts/build-drain.js` emitting `plugin/scripts/drain-service.cjs`. Inert (not wired up).
3. **Phase 3** — flip behind `CLAUDE_MEM_SPLIT_DAEMON=1`. Intake skips `initializeProviderPhase` + `attachIngestGeneratorStarter`. Drain owns it. Provision `claude-mem-enrich-llama-server.service` + download Qwen3-0.6B.
4. **Phase 4** — `drain start/stop/status` CLI; integrate with `memory-drain-on-lock.sh` so lock spawns drain + enrich-llama-server, unlock kills both.
5. **Phase 5** — strip provider deps from intake bundle (smaller, faster cold start).

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
