
import { logger } from '../../utils/logger.js';
import type { ConversationMessage } from '../worker-types.js';
import { ClassifiedProviderError } from './provider-errors.js';
import { withRetry } from './retry.js';

/**
 * Shared HTTP client for OpenAI-compatible /v1/chat/completions endpoints.
 *
 * Both `OpenRouterProvider` and `OpenAICompatibleProvider` use this — they
 * differ only in default base URL, authentication-header expectations, and
 * vendor-specific extra headers (e.g. OpenRouter's HTTP-Referer / X-Title).
 *
 * The client deliberately does NOT own:
 *   - context-window truncation (each provider has its own *_MAX_CONTEXT_*
 *     settings, so the caller passes already-truncated messages)
 *   - cost estimation (provider-specific pricing constants)
 *   - settings loading (each provider reads its own *_API_KEY / *_MODEL)
 *
 * It only owns: HTTP request shape, retry/abort handling, response parsing,
 * and routing of failures to a caller-supplied error classifier.
 */

export interface OpenAIMessage {
  role: 'user' | 'assistant' | 'system';
  content: string;
}

interface OpenAIChatCompletionsResponse {
  choices?: Array<{
    message?: {
      role?: string;
      content?: string;
    };
    finish_reason?: string;
  }>;
  usage?: {
    prompt_tokens?: number;
    completion_tokens?: number;
    total_tokens?: number;
  };
  error?: {
    message?: string;
    code?: string;
  };
}

export interface ErrorClassifierInput {
  status?: number;
  bodyText?: string;
  headers?: Headers | { get(name: string): string | null };
  cause: unknown;
  requestId?: string;
}

export type ErrorClassifier = (input: ErrorClassifierInput) => ClassifiedProviderError;

export interface OpenAIChatCompletionsClientConfig {
  /** Base URL up to and including the version segment, e.g. `https://openrouter.ai/api/v1`. */
  baseUrl: string;
  /** Bearer token sent in the `Authorization` header. */
  apiKey: string;
  /** Additional headers (e.g. OpenRouter's `HTTP-Referer`, `X-Title`). */
  extraHeaders?: Record<string, string>;
  /** Maps a fetch failure / non-OK response to a typed `ClassifiedProviderError`. */
  classifyError: ErrorClassifier;
  /** Label used by `withRetry` for log correlation, e.g. `"OpenRouter qwen/..."`. */
  retryLabel: string;
}

export interface QueryOptions {
  model: string;
  /** Temperature for structured-extraction prompts. Default 0.3. */
  temperature?: number;
  /** Max tokens to generate. Default 4096. */
  maxTokens?: number;
}

export interface QueryResult {
  content: string;
  promptTokens?: number;
  completionTokens?: number;
  totalTokens?: number;
}

/**
 * Build the chat-completions URL from a configured base URL.
 * Trims trailing slashes so both `https://host/api/v1` and
 * `https://host/api/v1/` resolve to the same endpoint. The base URL must
 * already include the version segment (e.g. `/v1`) — we only append
 * `/chat/completions`.
 */
export function buildChatCompletionsUrl(baseUrl: string): string {
  return `${baseUrl.replace(/\/+$/, '')}/chat/completions`;
}

export function conversationToOpenAIMessages(history: ConversationMessage[]): OpenAIMessage[] {
  return history.map(msg => ({
    role: msg.role === 'assistant' ? 'assistant' : 'user',
    content: msg.content,
  }));
}

export class OpenAIChatCompletionsClient {
  constructor(private readonly cfg: OpenAIChatCompletionsClientConfig) {}

  async query(messages: OpenAIMessage[], options: QueryOptions): Promise<QueryResult> {
    const url = buildChatCompletionsUrl(this.cfg.baseUrl);
    const temperature = options.temperature ?? 0.3;
    const maxTokens = options.maxTokens ?? 4096;

    let priorRequestId: string | null = null;

    const data = await withRetry<OpenAIChatCompletionsResponse>(async (attemptSignal) => {
      let response: Response;
      try {
        response = await fetch(url, {
          method: 'POST',
          headers: {
            'Authorization': `Bearer ${this.cfg.apiKey}`,
            'Content-Type': 'application/json',
            ...(this.cfg.extraHeaders ?? {}),
            ...(priorRequestId ? { 'x-claude-mem-prior-request-id': priorRequestId } : {}),
          },
          body: JSON.stringify({
            model: options.model,
            messages,
            temperature,
            max_tokens: maxTokens,
          }),
          signal: attemptSignal,
        });
      } catch (networkError: unknown) {
        throw this.cfg.classifyError({ cause: networkError });
      }

      const requestId = response.headers.get('x-request-id') ?? response.headers.get('x-openrouter-request-id');
      if (requestId) {
        priorRequestId = requestId;
      } else {
        logger.debug('SDK', `${this.cfg.retryLabel} response missing request-id header; retry dedup is best-effort`);
      }

      if (!response.ok) {
        const errorText = await response.text();
        throw this.cfg.classifyError({
          status: response.status,
          bodyText: errorText,
          headers: response.headers,
          cause: new Error(`${this.cfg.retryLabel} API error: ${response.status} - ${errorText}`),
          ...(requestId ? { requestId } : {}),
        });
      }

      const responseData = await response.json() as OpenAIChatCompletionsResponse;

      if (responseData.error) {
        // Some providers (notably OpenRouter) embed errors inside 200 responses.
        throw this.cfg.classifyError({
          status: response.status,
          bodyText: `${responseData.error.code} ${responseData.error.message ?? ''}`,
          headers: response.headers,
          cause: new Error(`${this.cfg.retryLabel} API error: ${responseData.error.code} - ${responseData.error.message}`),
        });
      }

      return responseData;
    }, { label: this.cfg.retryLabel });

    if (!data.choices?.[0]?.message?.content) {
      logger.error('SDK', `Empty response from ${this.cfg.retryLabel}`);
      return { content: '' };
    }

    return {
      content: data.choices[0].message.content,
      promptTokens: data.usage?.prompt_tokens,
      completionTokens: data.usage?.completion_tokens,
      totalTokens: data.usage?.total_tokens,
    };
  }
}
