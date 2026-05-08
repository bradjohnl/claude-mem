
import { buildContinuationPrompt, buildInitPrompt, buildObservationPrompt, buildSummaryPrompt } from '../../sdk/prompts.js';
import { SettingsDefaultsManager } from '../../shared/SettingsDefaultsManager.js';
import { USER_SETTINGS_PATH } from '../../shared/paths.js';
import { logger } from '../../utils/logger.js';
import { ModeManager } from '../domain/ModeManager.js';
import type { ModeConfig } from '../domain/types.js';
import type { ActiveSession, ConversationMessage } from '../worker-types.js';
import { DatabaseManager } from './DatabaseManager.js';
import { SessionManager } from './SessionManager.js';
import {
  isAbortError,
  processAgentResponse,
  type WorkerRef
} from './agents/index.js';
import {
  OpenAIChatCompletionsClient,
  conversationToOpenAIMessages,
  type ErrorClassifierInput,
  type OpenAIMessage,
} from './OpenAIChatCompletionsClient.js';
import { ClassifiedProviderError } from './provider-errors.js';

/**
 * Provider for OpenAI-compatible /v1/chat/completions endpoints. Use this for
 * self-hosted llama.cpp / Ollama / vLLM / LiteLLM, or for any service that
 * speaks the OpenAI chat-completions protocol but is NOT OpenRouter.
 *
 * The OpenRouter-specific provider (OpenRouterProvider) sends additional
 * analytics headers and assumes OpenRouter's pricing model; this provider
 * sends none of that and treats the endpoint as fully generic. It also makes
 * BASE_URL a required setting (no cloud default) — the user opted into a
 * custom endpoint.
 */

/**
 * Parse Retry-After header (seconds or HTTP-date). Returns ms or undefined.
 */
function parseRetryAfterMs(value: string | null): number | undefined {
  if (!value) return undefined;
  const seconds = Number(value);
  if (!Number.isNaN(seconds) && seconds >= 0) {
    return Math.floor(seconds * 1000);
  }
  const dateMs = Date.parse(value);
  if (!Number.isNaN(dateMs)) {
    const delta = dateMs - Date.now();
    return delta > 0 ? delta : 0;
  }
  return undefined;
}

/**
 * Classify a fetch failure from a generic OpenAI-compatible endpoint. Mirrors
 * the OpenRouter classifier shape but with neutral language and no provider-
 * specific quota markers — most self-hosted endpoints don't surface
 * "quota_exhausted" semantics.
 */
export function classifyOpenAICompatibleError(input: ErrorClassifierInput): ClassifiedProviderError {
  const status = input.status;
  const body = input.bodyText ?? '';
  const headers = input.headers;
  const retryAfterMs = headers ? parseRetryAfterMs(headers.get('retry-after')) : undefined;

  if (status === 429) {
    return new ClassifiedProviderError(
      'OpenAI-compatible endpoint rate limit (429)',
      { kind: 'rate_limit', cause: input.cause, ...(retryAfterMs !== undefined ? { retryAfterMs } : {}) },
    );
  }

  if (status === 401 || status === 403) {
    return new ClassifiedProviderError(
      `OpenAI-compatible endpoint auth error (status ${status})`,
      { kind: 'auth_invalid', cause: input.cause },
    );
  }

  if (status === 400 || status === 404) {
    return new ClassifiedProviderError(
      `OpenAI-compatible endpoint bad request (status ${status})`,
      { kind: 'unrecoverable', cause: input.cause },
    );
  }

  if (status !== undefined && status >= 500 && status < 600) {
    return new ClassifiedProviderError(
      `OpenAI-compatible endpoint upstream error (status ${status})`,
      { kind: 'transient', cause: input.cause },
    );
  }

  // Network errors (no status) — treat as transient.
  if (status === undefined) {
    return new ClassifiedProviderError(
      `OpenAI-compatible endpoint network error: ${input.cause instanceof Error ? input.cause.message : String(input.cause)}`,
      { kind: 'transient', cause: input.cause },
    );
  }

  return new ClassifiedProviderError(
    `OpenAI-compatible endpoint API error: ${status}${body ? ` - ${body.substring(0, 200)}` : ''}`,
    { kind: 'unrecoverable', cause: input.cause },
  );
}

const DEFAULT_MAX_CONTEXT_MESSAGES = 20;
const DEFAULT_MAX_ESTIMATED_TOKENS = 100000;
const CHARS_PER_TOKEN_ESTIMATE = 4;

interface OpenAICompatibleConfig {
  baseUrl: string;
  apiKey: string;
  model: string;
}

export class OpenAICompatibleProvider {
  private dbManager: DatabaseManager;
  private sessionManager: SessionManager;

  constructor(dbManager: DatabaseManager, sessionManager: SessionManager) {
    this.dbManager = dbManager;
    this.sessionManager = sessionManager;
  }

  async startSession(session: ActiveSession, worker?: WorkerRef): Promise<void> {
    const cfg = this.getConfig();

    if (!cfg.baseUrl) {
      throw new Error('OpenAI-compatible base URL not configured. Set CLAUDE_MEM_OPENAI_COMPATIBLE_BASE_URL in settings (e.g. http://127.0.0.1:8085/v1).');
    }
    if (!cfg.model) {
      throw new Error('OpenAI-compatible model not configured. Set CLAUDE_MEM_OPENAI_COMPATIBLE_MODEL in settings.');
    }

    const client = this.buildClient(cfg);

    if (!session.memorySessionId) {
      const syntheticMemorySessionId = `openai-compatible-${session.contentSessionId}-${Date.now()}`;
      session.memorySessionId = syntheticMemorySessionId;
      this.dbManager.getSessionStore().updateMemorySessionId(session.sessionDbId, syntheticMemorySessionId);
      logger.info('SESSION', `MEMORY_ID_GENERATED | sessionDbId=${session.sessionDbId} | provider=OpenAICompatible`);
    }

    const mode = ModeManager.getInstance().getActiveMode();

    const initPrompt = session.lastPromptNumber === 1
      ? buildInitPrompt(session.project, session.contentSessionId, session.userPrompt, mode)
      : buildContinuationPrompt(session.userPrompt, session.lastPromptNumber, session.contentSessionId, mode);

    session.conversationHistory.push({ role: 'user', content: initPrompt });

    try {
      const initResponse = await this.queryMultiTurn(client, session.conversationHistory, cfg.model);
      await this.handleInitResponse(initResponse, session, worker, cfg.model);
    } catch (error: unknown) {
      logger.error(
        'SDK',
        'OpenAI-compatible init failed',
        { sessionId: session.sessionDbId, model: cfg.model },
        error instanceof Error ? error : new Error(String(error)),
      );
      await this.handleSessionError(error, session);
      return;
    }

    let lastCwd: string | undefined;
    try {
      for await (const message of this.sessionManager.getMessageIterator(session.sessionDbId)) {
        lastCwd = await this.processOneMessage(session, message, lastCwd, client, cfg.model, worker, mode);
      }
    } catch (error: unknown) {
      logger.error(
        'SDK',
        'OpenAI-compatible message processing failed',
        { sessionId: session.sessionDbId, model: cfg.model },
        error instanceof Error ? error : new Error(String(error)),
      );
      await this.handleSessionError(error, session);
      return;
    }

    const sessionDuration = Date.now() - session.startTime;
    logger.success('SDK', 'OpenAI-compatible agent completed', {
      sessionId: session.sessionDbId,
      duration: `${(sessionDuration / 1000).toFixed(1)}s`,
      historyLength: session.conversationHistory.length,
      model: cfg.model,
    });
  }

  private buildClient(cfg: OpenAICompatibleConfig): OpenAIChatCompletionsClient {
    return new OpenAIChatCompletionsClient({
      baseUrl: cfg.baseUrl,
      apiKey: cfg.apiKey,
      classifyError: classifyOpenAICompatibleError,
      retryLabel: `OpenAI-compatible ${cfg.model}`,
    });
  }

  private async processOneMessage(
    session: ActiveSession,
    message: { _persistentId: number; agentId?: string | null; agentType?: string | null; type: 'observation' | 'summarize'; cwd?: string; prompt_number?: number; tool_name?: string; tool_input?: unknown; tool_response?: unknown; last_assistant_message?: string },
    lastCwd: string | undefined,
    client: OpenAIChatCompletionsClient,
    model: string,
    worker: WorkerRef | undefined,
    mode: ModeConfig,
  ): Promise<string | undefined> {
    session.pendingAgentId = message.agentId ?? null;
    session.pendingAgentType = message.agentType ?? null;

    if (message.cwd) {
      lastCwd = message.cwd;
    }
    const originalTimestamp = session.earliestPendingTimestamp;

    if (message.type === 'observation') {
      await this.processObservationMessage(session, message, originalTimestamp, lastCwd, client, model, worker);
    } else if (message.type === 'summarize') {
      await this.processSummaryMessage(session, message, originalTimestamp, lastCwd, client, model, worker, mode);
    }

    return lastCwd;
  }

  private async handleInitResponse(
    initResponse: { content: string; tokensUsed?: number },
    session: ActiveSession,
    worker: WorkerRef | undefined,
    model: string,
  ): Promise<void> {
    if (initResponse.content) {
      session.conversationHistory.push({ role: 'assistant', content: initResponse.content });
      const tokensUsed = initResponse.tokensUsed || 0;
      session.cumulativeInputTokens += Math.floor(tokensUsed * 0.7);
      session.cumulativeOutputTokens += Math.floor(tokensUsed * 0.3);

      await processAgentResponse(
        initResponse.content, session, this.dbManager, this.sessionManager,
        worker, tokensUsed, null, 'OpenAICompatible', undefined, model,
      );
    } else {
      logger.error('SDK', 'Empty OpenAI-compatible init response - session may lack context', {
        sessionId: session.sessionDbId, model,
      });
    }
  }

  private async processObservationMessage(
    session: ActiveSession,
    message: { prompt_number?: number; tool_name?: string; tool_input?: unknown; tool_response?: unknown; cwd?: string },
    originalTimestamp: number | null,
    lastCwd: string | undefined,
    client: OpenAIChatCompletionsClient,
    model: string,
    worker: WorkerRef | undefined,
  ): Promise<void> {
    if (message.prompt_number !== undefined) {
      session.lastPromptNumber = message.prompt_number;
    }

    if (!session.memorySessionId) {
      throw new Error('Cannot process observations: memorySessionId not yet captured. This session may need to be reinitialized.');
    }

    const obsPrompt = buildObservationPrompt({
      id: 0,
      tool_name: message.tool_name!,
      tool_input: JSON.stringify(message.tool_input),
      tool_output: JSON.stringify(message.tool_response),
      created_at_epoch: originalTimestamp ?? Date.now(),
      cwd: message.cwd,
    });

    session.conversationHistory.push({ role: 'user', content: obsPrompt });
    const obsResponse = await this.queryMultiTurn(client, session.conversationHistory, model);

    let tokensUsed = 0;
    if (obsResponse.content) {
      session.conversationHistory.push({ role: 'assistant', content: obsResponse.content });
      tokensUsed = obsResponse.tokensUsed || 0;
      session.cumulativeInputTokens += Math.floor(tokensUsed * 0.7);
      session.cumulativeOutputTokens += Math.floor(tokensUsed * 0.3);
    }

    await processAgentResponse(
      obsResponse.content || '', session, this.dbManager, this.sessionManager,
      worker, tokensUsed, originalTimestamp, 'OpenAICompatible', lastCwd, model,
    );
  }

  private async processSummaryMessage(
    session: ActiveSession,
    message: { last_assistant_message?: string },
    originalTimestamp: number | null,
    lastCwd: string | undefined,
    client: OpenAIChatCompletionsClient,
    model: string,
    worker: WorkerRef | undefined,
    mode: ModeConfig,
  ): Promise<void> {
    if (!session.memorySessionId) {
      throw new Error('Cannot process summary: memorySessionId not yet captured. This session may need to be reinitialized.');
    }

    const summaryPrompt = buildSummaryPrompt({
      id: session.sessionDbId,
      memory_session_id: session.memorySessionId,
      project: session.project,
      user_prompt: session.userPrompt,
      last_assistant_message: message.last_assistant_message || '',
    }, mode);

    session.conversationHistory.push({ role: 'user', content: summaryPrompt });
    const summaryResponse = await this.queryMultiTurn(client, session.conversationHistory, model);

    let tokensUsed = 0;
    if (summaryResponse.content) {
      session.conversationHistory.push({ role: 'assistant', content: summaryResponse.content });
      tokensUsed = summaryResponse.tokensUsed || 0;
      session.cumulativeInputTokens += Math.floor(tokensUsed * 0.7);
      session.cumulativeOutputTokens += Math.floor(tokensUsed * 0.3);
    }

    await processAgentResponse(
      summaryResponse.content || '', session, this.dbManager, this.sessionManager,
      worker, tokensUsed, originalTimestamp, 'OpenAICompatible', lastCwd, model,
    );
  }

  private async handleSessionError(error: unknown, session: ActiveSession): Promise<never> {
    if (isAbortError(error)) {
      logger.warn('SDK', 'OpenAI-compatible agent aborted', { sessionId: session.sessionDbId });
      throw error;
    }
    logger.failure('SDK', 'OpenAI-compatible agent error', { sessionDbId: session.sessionDbId }, error instanceof Error ? error : new Error(String(error)));
    throw error;
  }

  private estimateTokens(text: string): number {
    return Math.ceil(text.length / CHARS_PER_TOKEN_ESTIMATE);
  }

  private truncateHistory(history: ConversationMessage[]): ConversationMessage[] {
    const settings = SettingsDefaultsManager.loadFromFile(USER_SETTINGS_PATH);
    const maxMessages = parseInt(settings.CLAUDE_MEM_OPENAI_COMPATIBLE_MAX_CONTEXT_MESSAGES) || DEFAULT_MAX_CONTEXT_MESSAGES;
    const maxTokens = parseInt(settings.CLAUDE_MEM_OPENAI_COMPATIBLE_MAX_TOKENS) || DEFAULT_MAX_ESTIMATED_TOKENS;

    if (history.length <= maxMessages) {
      const totalTokens = history.reduce((sum, m) => sum + this.estimateTokens(m.content), 0);
      if (totalTokens <= maxTokens) {
        return history;
      }
    }

    const truncated: ConversationMessage[] = [];
    let tokenCount = 0;

    for (let i = history.length - 1; i >= 0; i--) {
      const msg = history[i];
      const msgTokens = this.estimateTokens(msg.content);

      if (truncated.length >= maxMessages || tokenCount + msgTokens > maxTokens) {
        logger.warn('SDK', 'Context window truncated to prevent runaway costs', {
          originalMessages: history.length,
          keptMessages: truncated.length,
          droppedMessages: i + 1,
          estimatedTokens: tokenCount,
          tokenLimit: maxTokens,
        });
        break;
      }

      truncated.unshift(msg);
      tokenCount += msgTokens;
    }

    return truncated;
  }

  private async queryMultiTurn(
    client: OpenAIChatCompletionsClient,
    history: ConversationMessage[],
    model: string,
  ): Promise<{ content: string; tokensUsed?: number }> {
    const truncated = this.truncateHistory(history);
    const messages: OpenAIMessage[] = conversationToOpenAIMessages(truncated);
    const totalChars = truncated.reduce((sum, m) => sum + m.content.length, 0);

    logger.debug('SDK', `Querying OpenAI-compatible multi-turn (${model})`, {
      turns: truncated.length,
      totalChars,
      estimatedTokens: this.estimateTokens(truncated.map(m => m.content).join('')),
    });

    const result = await client.query(messages, { model });

    if (result.totalTokens) {
      logger.info('SDK', 'OpenAI-compatible API usage', {
        model,
        inputTokens: result.promptTokens ?? 0,
        outputTokens: result.completionTokens ?? 0,
        totalTokens: result.totalTokens,
        messagesInContext: truncated.length,
      });

      if (result.totalTokens > 50000) {
        logger.warn('SDK', 'High token usage detected - consider reducing context', {
          totalTokens: result.totalTokens,
        });
      }
    }

    return { content: result.content, tokensUsed: result.totalTokens };
  }

  private getConfig(): OpenAICompatibleConfig {
    const settings = SettingsDefaultsManager.loadFromFile(USER_SETTINGS_PATH);
    const baseUrl = settings.CLAUDE_MEM_OPENAI_COMPATIBLE_BASE_URL || '';
    // API key is optional for self-hosted endpoints; many ignore it. Default to "dummy"
    // so that providers requiring *some* token (but not validating it) accept the request.
    const apiKey = settings.CLAUDE_MEM_OPENAI_COMPATIBLE_API_KEY || 'dummy';
    const model = settings.CLAUDE_MEM_OPENAI_COMPATIBLE_MODEL || '';
    return { baseUrl, apiKey, model };
  }
}

export function isOpenAICompatibleAvailable(): boolean {
  const settings = SettingsDefaultsManager.loadFromFile(USER_SETTINGS_PATH);
  return !!(settings.CLAUDE_MEM_OPENAI_COMPATIBLE_BASE_URL && settings.CLAUDE_MEM_OPENAI_COMPATIBLE_MODEL);
}

export function isOpenAICompatibleSelected(): boolean {
  const settings = SettingsDefaultsManager.loadFromFile(USER_SETTINGS_PATH);
  return settings.CLAUDE_MEM_PROVIDER === 'openai-compatible';
}
