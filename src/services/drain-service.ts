// drain-service.ts
// Standalone daemon that drains pending observations against an LLM endpoint.
//
// In the split-daemon design (see SPLIT_DAEMON_PLAN.md), this process owns all
// LLM-bound work: TranscriptWatcher, pending_messages drain, ChromaSync backfill,
// MCP self-check. Its lifecycle is gated by screen-lock state so the GPU stays
// free for foreground use.
//
// Phase 2: inert skeleton. Process starts, idles in a poll loop, exits cleanly
// on SIGTERM. Phase 3 wires the actual provider logic from initializeProviderPhase
// in worker-service.ts behind CLAUDE_MEM_SPLIT_DAEMON=1.

import { logger } from '../utils/logger.js';
import { homedir } from 'os';
import { join } from 'path';
import { existsSync } from 'fs';
import { DatabaseManager } from './worker/DatabaseManager.js';
import { SessionManager } from './worker/SessionManager.js';
import { ClaudeProvider } from './worker/ClaudeProvider.js';
import { GeminiProvider, isGeminiSelected, isGeminiAvailable } from './worker/GeminiProvider.js';
import { OpenRouterProvider, isOpenRouterSelected, isOpenRouterAvailable } from './worker/OpenRouterProvider.js';

const POLL_INTERVAL_MS = parseInt(process.env.CLAUDE_MEM_DRAIN_POLL_MS || '2000', 10);
const DATA_DIR = process.env.CLAUDE_MEM_DATA_DIR || join(homedir(), '.claude-mem');
const DB_PATH = join(DATA_DIR, 'claude-mem.db');

let shuttingDown = false;

interface QueueStats {
  pending: number;
  processing: number;
  failed: number;
}

// Opportunistic read-only queue stats. Uses bun:sqlite when available (the
// bundled runtime), falls back to a no-op if the DB is missing. Read-only by
// design for Phase 3a — Phase 3b will add the write path (mark processing →
// call provider → mark done).
async function readQueueStats(): Promise<QueueStats | null> {
  if (!existsSync(DB_PATH)) return null;
  try {
    // bun:sqlite is the bundled SQLite for the worker — same one used elsewhere.
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const { Database } = require('bun:sqlite');
    const db = new Database(DB_PATH, { readonly: true });
    try {
      const row = db.prepare(`
        SELECT
          SUM(status='pending')    AS pending,
          SUM(status='processing') AS processing,
          SUM(status='failed')     AS failed
        FROM pending_messages
      `).get() as { pending: number | null; processing: number | null; failed: number | null };
      return {
        pending: row.pending ?? 0,
        processing: row.processing ?? 0,
        failed: row.failed ?? 0
      };
    } finally {
      db.close();
    }
  } catch (err) {
    logger.debug('DRAIN', 'queue stats read failed', { error: err instanceof Error ? err.message : String(err) });
    return null;
  }
}

interface DrainContext {
  dbManager: DatabaseManager;
  sessionManager: SessionManager;
  claudeProvider: ClaudeProvider;
  geminiProvider: GeminiProvider;
  openRouterProvider: OpenRouterProvider;
}

// Whether drain should actually call provider.startSession on pending rows.
// Default: false (safe — drain reports queue stats only). Lock-driven
// orchestrator (memory-drain-on-lock.sh) sets this to '1' on lock to begin
// actual drain. This prevents foreground GPU use even if drain is started
// manually for testing.
const DRAIN_ACTIVE = process.env.CLAUDE_MEM_DRAIN_ACTIVE === '1';

// Per-tick session cap. Sequential one-at-a-time keeps llama-server simple.
const MAX_SESSIONS_PER_TICK = parseInt(process.env.CLAUDE_MEM_DRAIN_MAX_PER_TICK || '1', 10);

async function initializeDrainContext(): Promise<DrainContext> {
  logger.info('DRAIN', 'initializing drain context (DB + providers)');

  const dbManager = new DatabaseManager();
  await dbManager.initialize();

  const sessionManager = new SessionManager(dbManager);
  const claudeProvider = new ClaudeProvider(dbManager, sessionManager);
  const geminiProvider = new GeminiProvider(dbManager, sessionManager);
  const openRouterProvider = new OpenRouterProvider(dbManager, sessionManager);

  logger.info('DRAIN', 'drain context ready', {
    claudeProviderReady: !!claudeProvider,
    geminiProviderReady: !!geminiProvider,
    openRouterProviderReady: !!openRouterProvider
  });

  return { dbManager, sessionManager, claudeProvider, geminiProvider, openRouterProvider };
}

function getSelectedProvider(): 'claude' | 'gemini' | 'openrouter' {
  if (isOpenRouterSelected() && isOpenRouterAvailable()) return 'openrouter';
  if (isGeminiSelected() && isGeminiAvailable()) return 'gemini';
  return 'claude';
}

// Drain a single session: initialize, pick provider, run startSession.
// Errors are caught and logged so one bad session doesn't kill the drain.
async function drainSession(ctx: DrainContext, sessionDbId: number): Promise<void> {
  const session = ctx.sessionManager.initializeSession(sessionDbId);
  if (!session) {
    logger.warn('DRAIN', 'session not found', { sessionDbId });
    return;
  }
  if (session.generatorPromise) {
    logger.debug('DRAIN', 'session already has active generator, skipping', { sessionDbId });
    return;
  }

  const provider = getSelectedProvider();
  const agent = provider === 'openrouter' ? ctx.openRouterProvider
              : provider === 'gemini' ? ctx.geminiProvider
              : ctx.claudeProvider;

  if (session.abortController.signal.aborted) {
    session.abortController = new AbortController();
  }
  session.currentProvider = provider;
  session.lastGeneratorActivity = Date.now();

  logger.info('DRAIN', 'starting session', { sessionDbId, provider, historyLength: session.conversationHistory.length });

  try {
    // WorkerRef is optional — drain has no SSE/HTTP, so pass undefined.
    await agent.startSession(session, undefined);
    logger.info('DRAIN', 'session drained', { sessionDbId, provider });
  } catch (err) {
    logger.error('DRAIN', 'session drain failed', { sessionDbId, provider }, err instanceof Error ? err : new Error(String(err)));
    try {
      const reset = await ctx.sessionManager.resetProcessingToPending(sessionDbId);
      if (reset > 0) logger.info('DRAIN', 'reset processing rows back to pending after failure', { sessionDbId, reset });
    } catch { /* best-effort */ }
  } finally {
    session.generatorPromise = undefined;
  }
}

// Returns up to MAX_SESSIONS_PER_TICK distinct session_dbids that have pending rows.
function nextPendingSessions(ctx: DrainContext): number[] {
  try {
    const rows = ctx.dbManager.getSessionStore().db.prepare(`
      SELECT DISTINCT session_dbid
      FROM pending_messages
      WHERE status = 'pending'
      ORDER BY session_dbid ASC
      LIMIT ?
    `).all(MAX_SESSIONS_PER_TICK) as { session_dbid: number }[];
    return rows.map(r => r.session_dbid);
  } catch (err) {
    logger.debug('DRAIN', 'next pending lookup failed', { error: err instanceof Error ? err.message : String(err) });
    return [];
  }
}

async function runDrainLoop(ctx: DrainContext | null): Promise<void> {
  let tickCount = 0;
  while (!shuttingDown) {
    tickCount++;
    if (tickCount % 30 === 1) {
      const stats = await readQueueStats();
      if (stats) {
        logger.info('DRAIN', 'queue depth', {
          ...stats,
          selectedProvider: ctx ? getSelectedProvider() : 'none',
          drainActive: DRAIN_ACTIVE
        });
      }
    }

    if (ctx && DRAIN_ACTIVE) {
      // Active drain: process one (or N) pending session per tick. Sequential
      // to keep llama-server :8086 from being overloaded.
      const pending = nextPendingSessions(ctx);
      for (const sessionDbId of pending) {
        if (shuttingDown) break;
        await drainSession(ctx, sessionDbId);
      }
    }

    await new Promise(resolve => setTimeout(resolve, POLL_INTERVAL_MS));
  }
}

function installSignalHandlers(): void {
  const onSignal = (signal: NodeJS.Signals) => {
    if (shuttingDown) return;
    logger.info('DRAIN', `${signal} received, shutting down`);
    shuttingDown = true;
  };
  process.on('SIGTERM', () => onSignal('SIGTERM'));
  process.on('SIGINT', () => onSignal('SIGINT'));
}

async function main(): Promise<void> {
  const argv = process.argv.slice(2);
  const command = argv[0] || 'start';

  if (command === 'version' || command === '--version') {
    console.log('claude-mem drain-service (Phase 2 skeleton)');
    process.exit(0);
  }

  if (command === 'status') {
    // Phase 4 will inspect a pidfile + queue depth here
    console.log('drain-service: not yet implemented in Phase 2');
    process.exit(0);
  }

  if (command !== 'start') {
    console.error(`Unknown command: ${command}. Supported: start, status, version`);
    process.exit(1);
  }

  logger.info('DRAIN', 'drain-service starting (Phase 3c: provider.startSession wired, gated by CLAUDE_MEM_DRAIN_ACTIVE)', {
    pid: process.pid,
    pollIntervalMs: POLL_INTERVAL_MS,
    splitDaemonFlag: process.env.CLAUDE_MEM_SPLIT_DAEMON || 'unset',
    drainActive: DRAIN_ACTIVE,
    maxSessionsPerTick: MAX_SESSIONS_PER_TICK
  });

  installSignalHandlers();

  // Best-effort dep init. If anything throws we fall back to read-only queue
  // stats — drain stays alive and useful for diagnostics even if providers
  // can't be instantiated (e.g. missing API key, DB lock).
  let ctx: DrainContext | null = null;
  try {
    ctx = await initializeDrainContext();
  } catch (err) {
    logger.error('DRAIN', 'drain context init failed (continuing in read-only mode)', {}, err instanceof Error ? err : new Error(String(err)));
  }

  await runDrainLoop(ctx);

  logger.info('DRAIN', 'drain-service exited cleanly');
  process.exit(0);
}

main().catch((err) => {
  logger.error('DRAIN', 'drain-service crashed', {}, err instanceof Error ? err : new Error(String(err)));
  process.exit(1);
});
