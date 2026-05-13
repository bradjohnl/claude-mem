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

async function runDrainLoop(): Promise<void> {
  let tickCount = 0;
  while (!shuttingDown) {
    tickCount++;
    // Phase 3b will add the actual write path here:
    //   1. SELECT pending_messages WHERE status='pending' LIMIT N
    //   2. UPDATE ... SET status='processing'
    //   3. Call provider.startSession against the enrichment llama-server (:8086)
    //   4. Write results back, mark 'done' or 'failed'
    //   5. Tick ChromaSync.backfillAllProjects on a slower cadence
    //
    // For now: report queue depth every 30 ticks (~1 min at default 2s poll)
    if (tickCount % 30 === 1) {
      const stats = await readQueueStats();
      if (stats) {
        logger.info('DRAIN', 'queue depth', stats);
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

  logger.info('DRAIN', 'drain-service starting (Phase 2 inert skeleton)', {
    pid: process.pid,
    pollIntervalMs: POLL_INTERVAL_MS,
    splitDaemonFlag: process.env.CLAUDE_MEM_SPLIT_DAEMON || 'unset'
  });

  installSignalHandlers();
  await runDrainLoop();

  logger.info('DRAIN', 'drain-service exited cleanly');
  process.exit(0);
}

main().catch((err) => {
  logger.error('DRAIN', 'drain-service crashed', {}, err instanceof Error ? err : new Error(String(err)));
  process.exit(1);
});
