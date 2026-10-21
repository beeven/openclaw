import { randomUUID } from "node:crypto";
import { fetchWithSsrFGuard, type SsrFPolicy } from "openclaw/plugin-sdk/ssrf-runtime";
import {
  A2A_CONTENT_TYPE,
  A2A_NOTIFICATION_TOKEN_HEADER,
  type A2aPushNotificationConfig,
  type A2aStreamResponse,
  type A2aTask,
} from "./a2a-model.js";
import type { A2aPushNotificationSettings } from "./config-schema.js";

const A2A_PUSH_TIMEOUT_MS = 10_000;
const A2A_PUSH_MAX_ATTEMPTS = 3;
const A2A_PUSH_RETRY_BASE_MS = 250;

/**
 * Resolve the SSRF policy for a peer-supplied webhook URL.
 *
 * Push notification targets come from the remote peer, so unlike configured peer
 * URLs they must never promote their own origin into the allowlist — that would
 * let any authenticated peer reach internal services. The policy therefore
 * starts from the operator's allowlist and blocks private networks by default.
 */
export function resolveA2aPushSsrFPolicy(settings?: A2aPushNotificationSettings): SsrFPolicy {
  const policy: SsrFPolicy = {};
  const hostnames = settings?.allowedHostnames?.filter((entry) => entry.trim().length > 0);
  if (hostnames && hostnames.length > 0) {
    policy.hostnameAllowlist = hostnames;
  }
  if (settings?.allowPrivateNetwork === true) {
    // Explicit operator opt-in only: internal webhook receivers.
    policy.allowPrivateNetwork = true;
  }
  return policy;
}

function buildPushHeaders(config: A2aPushNotificationConfig): Record<string, string> {
  const headers: Record<string, string> = {
    "content-type": A2A_CONTENT_TYPE,
    // Lets the receiver deduplicate retries of the same notification.
    "x-a2a-notification-id": randomUUID(),
  };
  if (config.authentication?.scheme) {
    const scheme = config.authentication.scheme.trim();
    const credentials = config.authentication.credentials ?? "";
    headers.authorization = credentials ? `${scheme} ${credentials}` : scheme;
  } else if (config.token) {
    headers[A2A_NOTIFICATION_TOKEN_HEADER] = config.token;
  }
  return headers;
}

export type A2aPushSenderLogger = (message: string) => void;

/**
 * Deliver task updates to registered webhooks (spec §4.3.3).
 *
 * The payload is a `StreamResponse`, the same shape the streaming binding uses,
 * so one renderer serves both. Deliveries are fire-and-forget: a broken webhook
 * must never fail the task that triggered it.
 */
export class A2aPushSender {
  readonly #log: A2aPushSenderLogger;
  readonly #policy: SsrFPolicy;
  /** One in-flight chain per task keeps notifications for a task ordered. */
  readonly #chains = new Map<string, Promise<void>>();

  constructor(params: { settings?: A2aPushNotificationSettings; log?: A2aPushSenderLogger }) {
    // Settings are folded into the SSRF policy here rather than kept around:
    // every delivery decision below reads the policy, never the raw config.
    this.#log = params.log ?? (() => undefined);
    this.#policy = resolveA2aPushSsrFPolicy(params.settings);
  }

  notify(params: {
    task: A2aTask;
    configs: A2aPushNotificationConfig[];
    event: A2aStreamResponse;
  }): void {
    if (params.configs.length === 0) {
      return;
    }
    const previous = this.#chains.get(params.task.id) ?? Promise.resolve();
    const next = previous
      .catch(() => undefined)
      .then(async () => {
        for (const config of params.configs) {
          await this.#deliver(config, params.event);
        }
      })
      .catch((error: unknown) => {
        this.#log(`a2a push notification failed for task ${params.task.id}: ${String(error)}`);
      });
    this.#chains.set(
      params.task.id,
      next.finally(() => {
        if (this.#chains.get(params.task.id) === next) {
          this.#chains.delete(params.task.id);
        }
      }),
    );
  }

  async #deliver(config: A2aPushNotificationConfig, event: A2aStreamResponse): Promise<void> {
    const body = JSON.stringify(event);
    const headers = buildPushHeaders(config);
    for (let attempt = 1; attempt <= A2A_PUSH_MAX_ATTEMPTS; attempt += 1) {
      try {
        const { response, release } = await fetchWithSsrFGuard({
          url: config.url,
          timeoutMs: A2A_PUSH_TIMEOUT_MS,
          signal: AbortSignal.timeout(A2A_PUSH_TIMEOUT_MS),
          policy: this.#policy,
          auditContext: "a2a.push_notification",
          // A redirect could deliver a task update to an unintended receiver.
          maxRedirects: 0,
          init: { method: "POST", headers, body },
        });
        try {
          if (response.ok) {
            return;
          }
          // 4xx (except 408/429) means the receiver rejected it; retrying cannot help.
          if (response.status >= 400 && response.status < 500 && response.status !== 408) {
            this.#log(
              `a2a push notification to ${config.url} rejected (HTTP ${response.status}); dropping`,
            );
            return;
          }
        } finally {
          await release();
        }
      } catch (error) {
        this.#log(`a2a push notification to ${config.url} failed: ${String(error)}`);
      }
      if (attempt < A2A_PUSH_MAX_ATTEMPTS) {
        // Exponential backoff keeps a wedged receiver from being hammered.
        await new Promise((resolve) => {
          setTimeout(resolve, A2A_PUSH_RETRY_BASE_MS * 2 ** (attempt - 1)).unref?.();
        });
      }
    }
    this.#log(`a2a push notification to ${config.url} exhausted ${A2A_PUSH_MAX_ATTEMPTS} attempts`);
  }

  /** Drop in-flight bookkeeping; used when the channel stops. */
  reset(): void {
    this.#chains.clear();
  }
}
