import { createHash, timingSafeEqual } from "node:crypto";
import type { IncomingMessage, ServerResponse } from "node:http";
import type { ChannelStatusIssue } from "openclaw/plugin-sdk/channel-contract";
import {
  buildChannelOutboundSessionRoute,
  createChatChannelPlugin,
  type ChannelPlugin,
} from "openclaw/plugin-sdk/channel-core";
import {
  createMessageReceiptFromOutboundResults,
  defineChannelMessageAdapter,
  waitUntilAbort,
} from "openclaw/plugin-sdk/channel-outbound";
import { channelReadyPatch, channelStoppedPatch } from "openclaw/plugin-sdk/gateway-runtime";
import {
  isHostNoticePayload,
  isReplyPayloadTerminalContent,
} from "openclaw/plugin-sdk/reply-payload";
import type { PluginRuntime } from "openclaw/plugin-sdk/runtime-store";
import {
  createComputedAccountStatusAdapter,
  createDefaultChannelRuntimeState,
  readAccountStatusSnapshot,
  resolveEnabledConfiguredAccountId,
} from "openclaw/plugin-sdk/status-helpers";
import { filterStringEntries, isRecord } from "openclaw/plugin-sdk/string-coerce-runtime";
import { boundedJsonUtf8Bytes } from "openclaw/plugin-sdk/text-utility-runtime";
import {
  isRequestBodyLimitError,
  readRequestBodyWithLimit,
  registerPluginHttpRoute,
} from "openclaw/plugin-sdk/webhook-ingress";
import { sendHttpRequestRejection } from "openclaw/plugin-sdk/webhook-request-guards";
import {
  A2A_JSON_RPC_ERROR_CODES,
  A2aProtocolError,
  contentTypeNotSupported,
  extensionSupportRequired,
  invalidParams,
  methodNotFound,
  versionNotSupported,
} from "./a2a-errors.js";
import {
  A2A_EXTENSIONS_HEADER,
  A2A_VERSION_HEADER,
  isSupportedA2aVersion,
  normalizeA2aVersion,
  parseServiceParameterHeader,
  type A2aStreamResponse,
} from "./a2a-model.js";
import {
  A2A_AGENT_CARD_PATHS,
  A2A_EXTENDED_CARD_PATH,
  A2A_JSONRPC_PATH,
  A2A_REST_PATH,
  buildA2aAgentCard,
  type A2aResolvedCapabilities,
} from "./agent-card.js";
import { DEFAULT_ACCOUNT_ID } from "./accounts.js";
import { A2A_CHANNEL_ID, a2aChannelSetupPlugin } from "./channel-base.js";
import { sendA2aChannelText } from "./outbound.js";
import {
  A2aRpcRequestSchema,
  isA2aStreamingMethod,
  resolveA2aRpcMethod,
} from "./protocol.js";
import { A2aPushConfigStore } from "./push-config-store.js";
import { A2aPushSender } from "./push-sender.js";
import { getA2aChannelRuntime } from "./runtime.js";
import { isResponseWritable, onResponseClosed, writeSseError, writeSseHead, writeSseResult } from "./sse.js";
import { A2aTaskStore } from "./task-store.js";
import {
  executeA2aMethod,
  streamA2aMethod,
  type A2aMethodContext,
} from "./methods.js";
import type { A2aChannelConfig, ResolvedA2aChannelAccount } from "./types.js";

// TEMP(local-dev shim): inline of openclaw/plugin-sdk/http-response-runtime's
// sendJsonNoStore, which the 2026.10.5-release runtime does not export yet.
// Revert to the SDK import when the host version matches this checkout.
function writeJsonResponse(res: ServerResponse, status: number, body: unknown): void {
  res.setHeader("Cache-Control", "no-store");
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

const MAX_REQUEST_BODY_BYTES = 1024 * 1024;
const MAX_RESPONSE_BODY_BYTES = 1024 * 1024;
// This cap stays enforced when an operator disables the per-peer rate limit.
const MAX_BATCH_REQUESTS = 30;
const DEFAULT_REPLY_TIMEOUT_MS = 120_000;
const DEFAULT_STREAM_TIMEOUT_MS = 120_000;
const DEFAULT_RATE_LIMIT_PER_MINUTE = 30;
const RATE_LIMIT_WINDOW_MS = 60_000;

type A2aRpcIdentifier = string | number | null;

type A2aRpcErrorObject = { code: number; message: string; data?: unknown };

type A2aRpcResponse =
  | { jsonrpc: "2.0"; id: A2aRpcIdentifier; result: unknown }
  | { jsonrpc: "2.0"; id: A2aRpcIdentifier; error: A2aRpcErrorObject };

function toRpcErrorObject(error: unknown): A2aRpcErrorObject {
  if (error instanceof A2aProtocolError) {
    const data = error.toErrorData();
    return {
      code: error.code,
      message: error.message,
      ...(data.length > 0 ? { data } : {}),
    };
  }
  return { code: A2A_JSON_RPC_ERROR_CODES.INTERNAL_ERROR, message: "Internal error" };
}

function createRpcError(id: A2aRpcIdentifier, code: number, message: string): A2aRpcResponse {
  return { jsonrpc: "2.0", id, error: { code, message } };
}

function writeRpcResponse(
  response: ServerResponse,
  value: A2aRpcResponse | A2aRpcResponse[],
): void {
  const measured = boundedJsonUtf8Bytes(value, MAX_RESPONSE_BODY_BYTES);
  if (measured.complete) {
    writeJsonResponse(response, 200, value);
    return;
  }
  const errorMessage = "A2A response exceeds the 1 MiB limit";
  const bounded = Array.isArray(value)
    ? value.map((entry) => createRpcError(entry.id, -32000, errorMessage))
    : createRpcError(value.id, -32000, errorMessage);
  // Request IDs can consume nearly the entire inbound budget. Drop correlation
  // only when an already-oversized response's replacement still cannot fit.
  const fallback = boundedJsonUtf8Bytes(bounded, MAX_RESPONSE_BODY_BYTES).complete
    ? bounded
    : Array.isArray(bounded)
      ? bounded.map(() => createRpcError(null, -32000, errorMessage))
      : createRpcError(null, -32000, errorMessage);
  writeJsonResponse(response, 200, fallback);
}

function resolvePeerName(request: IncomingMessage, config: A2aChannelConfig): string | undefined {
  const authorization = request.headers.authorization;
  const token = authorization?.match(/^Bearer\s+([^\s]+)$/i)?.[1];
  if (!token) {
    return undefined;
  }
  const presentedDigest = createHash("sha256").update(token).digest();
  for (const [peerName, peer] of Object.entries(config.peers ?? {})) {
    const configuredDigest = createHash("sha256").update(peer.token).digest();
    if (timingSafeEqual(presentedDigest, configuredDigest)) {
      return peerName;
    }
  }
  return undefined;
}

const A2A_ACCOUNT_STATUS_FIELDS = ["unresolvedPeers", "unresolvedOutboundPeers"] as const;

function normalizeA2aChannelTarget(raw: string): string | undefined {
  const target = raw.trim().replace(/^a2a:/i, "");
  return /^[a-z0-9][a-z0-9._-]{0,63}$/.test(target) ? target : undefined;
}

/**
 * Resolve the capabilities advertised in the Agent Card and enforced on the
 * gated methods (spec §3.3.4).
 *
 * Streaming is core and on by default. Push notifications and the extended card
 * are opt-in: push makes the Gateway emit POSTs to a URL chosen by the remote
 * peer, and the extended card publishes operator-authored text.
 */
function resolveCapabilities(config: A2aChannelConfig): A2aResolvedCapabilities {
  const configured = config.capabilities ?? {};
  return {
    streaming: configured.streaming !== false,
    pushNotifications: configured.pushNotifications === true,
    extendedAgentCard: configured.extendedAgentCard === true,
  };
}

/** Reject a request whose content type is present and clearly not JSON. */
function assertJsonContentType(request: IncomingMessage): void {
  const contentType = request.headers["content-type"];
  if (contentType === undefined) {
    return;
  }
  const essence = String(contentType).split(";", 1)[0]?.trim().toLowerCase() ?? "";
  if (essence === "application/json" || essence.endsWith("+json")) {
    return;
  }
  throw contentTypeNotSupported(
    `Unsupported content type "${essence}"; A2A JSON-RPC requires application/json`,
  );
}

function assertSupportedVersion(request: IncomingMessage): void {
  const version = normalizeA2aVersion(
    Array.isArray(request.headers[A2A_VERSION_HEADER])
      ? request.headers[A2A_VERSION_HEADER]?.[0]
      : request.headers[A2A_VERSION_HEADER],
  );
  if (isSupportedA2aVersion(version)) {
    return;
  }
  throw versionNotSupported(`A2A version ${version} is not supported; this agent serves 1.0`);
}

const a2aChannelMessageAdapter = defineChannelMessageAdapter({
  id: A2A_CHANNEL_ID,
  durableFinal: { capabilities: { text: true } },
  send: {
    text: async (ctx) => {
      const result = await sendA2aChannelText({
        cfg: ctx.cfg,
        accountId: ctx.accountId,
        to: ctx.to,
        text: ctx.text,
        assertDirectAdapterHandoff: ctx.assertDirectAdapterHandoff,
      });
      return {
        messageId: result.messageId,
        receipt: createMessageReceiptFromOutboundResults({
          results: [{ channel: A2A_CHANNEL_ID, messageId: result.messageId }],
          kind: "text",
        }),
      };
    },
  },
});

export const a2aChannelPlugin: ChannelPlugin<ResolvedA2aChannelAccount> = createChatChannelPlugin({
  base: {
    ...a2aChannelSetupPlugin,
    messaging: {
      transformReplyPayload: ({ payload }) =>
        isHostNoticePayload(payload) || !isReplyPayloadTerminalContent(payload) ? null : payload,
      normalizeTarget: normalizeA2aChannelTarget,
      inferTargetChatType: () => "direct",
      targetResolver: {
        looksLikeId: (raw) => normalizeA2aChannelTarget(raw) !== undefined,
        hint: "<a2a-peer-name>",
      },
      resolveOutboundSessionRoute: ({ cfg, agentId, accountId, target }) => {
        const peerName = normalizeA2aChannelTarget(target);
        if (!peerName) {
          return null;
        }
        return buildChannelOutboundSessionRoute({
          cfg,
          agentId,
          channel: A2A_CHANNEL_ID,
          accountId,
          recipientSessionExact: true,
          peer: { kind: "direct", id: peerName },
          chatType: "direct",
          from: `${A2A_CHANNEL_ID}:${accountId ?? DEFAULT_ACCOUNT_ID}`,
          to: peerName,
        });
      },
    },
    status: createComputedAccountStatusAdapter<ResolvedA2aChannelAccount>({
      defaultRuntime: createDefaultChannelRuntimeState(DEFAULT_ACCOUNT_ID),
      /**
       * Surfaces peers that config loading left unusable because a `${VAR}` credential
       * reference did not resolve. Without this the only signal is one startup log line,
       * while the remote peer just sees a bare 401 or a refused send.
       */
      collectStatusIssues: (accounts) => {
        const issues: ChannelStatusIssue[] = [];
        for (const entry of accounts) {
          const account = readAccountStatusSnapshot(entry, A2A_ACCOUNT_STATUS_FIELDS);
          if (!account) {
            continue;
          }
          // A fully withheld channel reports configured=false, so only enabled accounts are skipped here.
          const accountId = resolveEnabledConfiguredAccountId(account) ?? account.accountId;
          if (account.enabled === false || typeof accountId !== "string" || !accountId) {
            continue;
          }
          const inbound = filterStringEntries(account.unresolvedPeers).filter(Boolean);
          if (inbound.length > 0) {
            issues.push({
              channel: "a2a",
              accountId,
              kind: "config",
              message: `A2A peer${inbound.length === 1 ? "" : "s"} ${inbound.join(", ")} cannot authenticate: the inbound token reference did not resolve, so requests from ${inbound.length === 1 ? "it are" : "them are"} rejected with 401.`,
              fix: `Set the environment variable referenced by channels.a2a.peers.<name>.token for ${inbound.join(", ")}, then reload or restart the gateway.`,
            });
          }
          const outbound = filterStringEntries(account.unresolvedOutboundPeers).filter(Boolean);
          if (outbound.length > 0) {
            issues.push({
              channel: "a2a",
              accountId,
              kind: "config",
              message: `Sends to A2A peer${outbound.length === 1 ? "" : "s"} ${outbound.join(", ")} are refused: the outbound token reference did not resolve.`,
              fix: `Set the environment variable referenced by channels.a2a.peers.<name>.outboundToken for ${outbound.join(", ")}, then reload or restart the gateway.`,
            });
          }
        }
        return issues;
      },
      buildChannelSummary: ({ snapshot }) => ({
        configured: snapshot.configured ?? false,
        running: snapshot.running ?? false,
      }),
      resolveAccountSnapshot: ({ account }) => ({
        accountId: account.accountId,
        enabled: account.enabled,
        configured: account.configured,
        extra: {
          peerCount: Object.keys(account.config.peers ?? {}).length,
          // Names only: these peers stay unavailable until their token variable is set.
          ...(account.unresolvedPeers.length > 0
            ? { unresolvedPeers: account.unresolvedPeers }
            : {}),
          ...(account.unresolvedOutboundPeers.length > 0
            ? { unresolvedOutboundPeers: account.unresolvedOutboundPeers }
            : {}),
        },
      }),
    }),
    gateway: {
      startAccount: async (ctx) => {
        const { account } = ctx;
        const config = ctx.cfg;
        if (!account.configured) {
          throw new Error(`A2A channel is not configured for account "${account.accountId}"`);
        }

        ctx.setStatus({
          accountId: account.accountId,
          running: true,
          lifecycle: "starting",
          configured: true,
          enabled: account.enabled,
        });

        const runtime = getA2aChannelRuntime();
        const a2aConfig = account.config;
        const version = runtime.version;
        const capabilities = resolveCapabilities(a2aConfig);
        // Card content is derived from the config loaded at account start, so
        // the start instant is the honest "last modified" for card validators
        // (§8.6.1): a reload restarts the account and with it the timestamp.
        const cardModifiedAt = new Date();
        // SAFETY: Gateway injects its full runtime despite the narrowed public contract.
        const channelRuntime = (ctx.channelRuntime ?? runtime.channel) as PluginRuntime["channel"];
        const store = new A2aTaskStore();
        const pushConfigs = new A2aPushConfigStore();
        const pushSender = capabilities.pushNotifications
          ? new A2aPushSender({ settings: a2aConfig.pushNotifications })
          : null;
        // Fan every task event out to the webhooks registered by that task's peer.
        const stopPushObserver = pushSender
          ? store.onEvent(({ task, event, ownerPeer }) => {
              if (!ownerPeer) {
                return;
              }
              pushSender.notify({
                task,
                configs: pushConfigs.forTask(ownerPeer, task.id),
                event,
              });
            })
          : () => undefined;
        const unregisterRoutes: Array<() => void> = [];

        function createAgentCard(request: IncomingMessage, extended: boolean) {
          return buildA2aAgentCard({
            config,
            a2aConfig,
            version,
            request,
            capabilities,
            extended,
          });
        }

        /**
         * Serve a card with the caching validators of §8.6.1.
         *
         * The ETag is hashed from the rendered body rather than the config,
         * because the advertised origin derives from the request `Host`: two
         * peers can legitimately receive different bytes for the same config.
         * Conditional requests are answered with 304 so a peer that already
         * holds the card pays nothing but the header round-trip.
         */
        function writeCardResponse(
          request: IncomingMessage,
          response: ServerResponse,
          card: unknown,
          visibility: "public" | "private",
        ): void {
          const body = JSON.stringify(card);
          const etag = `"${createHash("sha256").update(body).digest("base64url")}"`;
          // The extended card is authenticated content; a shared cache must
          // never store it, which is what `private` (vs `public`) forbids.
          const cacheControl =
            visibility === "public" ? "public, max-age=300" : "private, max-age=300";
          response.setHeader("Cache-Control", cacheControl);
          response.setHeader("ETag", etag);
          const conditional = String(request.headers["if-none-match"] ?? "");
          const matches =
            conditional.includes("*") ||
            conditional
              .split(",")
              .map((entry) => entry.trim())
              .includes(etag);
          if (matches) {
            response.statusCode = 304;
            response.end();
            return;
          }
          response.setHeader("Content-Type", "application/json; charset=utf-8");
          response.setHeader("Last-Modified", cardModifiedAt.toUTCString());
          response.statusCode = 200;
          response.end(body);
        }

        function createMethodContext(params: {
          request: IncomingMessage;
          peerName: string;
        }): A2aMethodContext {
          return {
            account,
            config,
            a2aConfig,
            channelRuntime,
            buildContext: channelRuntime.inbound.buildContext,
            store,
            pushConfigs,
            capabilities,
            peerName: params.peerName,
            version,
            request: params.request,
            replyTimeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
            streamTimeoutMs: DEFAULT_STREAM_TIMEOUT_MS,
            registerInlinePushConfig: (taskId, pushConfig) => {
              if (!capabilities.pushNotifications) {
                return;
              }
              pushConfigs.create(params.peerName, {
                taskId,
                url: pushConfig.url,
                ...(pushConfig.id ? { id: pushConfig.id } : {}),
                ...(pushConfig.tenant ? { tenant: pushConfig.tenant } : {}),
                ...(pushConfig.token ? { token: pushConfig.token } : {}),
                ...(pushConfig.authentication
                  ? { authentication: pushConfig.authentication }
                  : {}),
              });
            },
          };
        }

        try {
          const peerRequestTimes = new Map<string, number[]>();

          function isRateLimited(peerName: string): boolean {
            const maximum = a2aConfig.rateLimitPerMinute ?? DEFAULT_RATE_LIMIT_PER_MINUTE;
            if (maximum === 0) {
              return false;
            }
            const now = Date.now();
            const requests = (peerRequestTimes.get(peerName) ?? []).filter(
              (timestamp) => now - timestamp < RATE_LIMIT_WINDOW_MS,
            );
            if (requests.length >= maximum) {
              peerRequestTimes.set(peerName, requests);
              return true;
            }
            requests.push(now);
            peerRequestTimes.set(peerName, requests);
            return false;
          }

          /** Run one unary JSON-RPC request; returns undefined for notifications. */
          async function processRpcRequest(
            input: unknown,
            peerName: string,
            request: IncomingMessage,
          ): Promise<A2aRpcResponse | undefined> {
            const parsed = A2aRpcRequestSchema.safeParse(input);
            const candidateId = isRecord(input) ? input.id : undefined;
            const id =
              typeof candidateId === "string" || typeof candidateId === "number"
                ? candidateId
                : null;
            const notification = parsed.success && !Object.hasOwn(parsed.data, "id");

            if (isRateLimited(peerName)) {
              return notification ? undefined : createRpcError(id, -32000, "Peer is rate limited");
            }

            if (!parsed.success) {
              return createRpcError(id, A2A_JSON_RPC_ERROR_CODES.INVALID_REQUEST, "Invalid JSON-RPC request");
            }

            const rpcRequest = parsed.data;
            const methodContext = createMethodContext({ request, peerName });

            try {
              assertSupportedVersion(request);
              assertRequiredExtensions(request, a2aConfig);

              const resolved = resolveA2aRpcMethod(rpcRequest.method);
              if (!resolved) {
                throw methodNotFound(rpcRequest.method);
              }
              // A stream cannot be interleaved into a batch response, so a
              // streaming method inside a batch reports as a normal error entry.
              if (resolved.streaming) {
                throw invalidParams(
                  `${rpcRequest.method} is a streaming method and cannot be used in a batch request`,
                );
              }
              const result = await executeA2aMethod(
                methodContext,
                resolved,
                rpcRequest.params,
              );
              return notification ? undefined : { jsonrpc: "2.0", id, result };
            } catch (error) {
              if (notification) {
                return undefined;
              }
              return { jsonrpc: "2.0", id, error: toRpcErrorObject(error) };
            }
          }

          /**
           * Serve one streaming JSON-RPC request as SSE.
           *
           * The first event is fetched before the headers are flushed so a
           * rejected request still comes back as an ordinary JSON-RPC error
           * instead of a 200 stream carrying only an error frame.
           */
          async function processStreamingRpcRequest(
            input: unknown,
            peerName: string,
            request: IncomingMessage,
            response: ServerResponse,
          ): Promise<void> {
            const parsed = A2aRpcRequestSchema.safeParse(input);
            const candidateId = isRecord(input) ? input.id : undefined;
            const id =
              typeof candidateId === "string" || typeof candidateId === "number"
                ? candidateId
                : null;

            const fail = (error: unknown) => {
              writeJsonResponse(response, 200, {
                jsonrpc: "2.0",
                id,
                error: toRpcErrorObject(error),
              });
            };

            if (!parsed.success) {
              fail(
                new A2aProtocolError(
                  A2A_JSON_RPC_ERROR_CODES.INVALID_REQUEST,
                  "Invalid JSON-RPC request",
                ),
              );
              return;
            }
            const methodContext = createMethodContext({ request, peerName });
            const resolved = resolveA2aRpcMethod(parsed.data.method);

            let iterator: AsyncIterator<A2aStreamResponse> | undefined;
            let first: IteratorResult<A2aStreamResponse> | undefined;
            try {
              assertSupportedVersion(request);
              // Checked before the stream opens: once headers are flushed the
              // only way to report a refusal is an SSE error frame.
              assertRequiredExtensions(request, a2aConfig);
              if (!resolved?.streaming) {
                throw methodNotFound(parsed.data.method);
              }
              const generator = streamA2aMethod(methodContext, resolved, parsed.data.params);
              iterator = generator[Symbol.asyncIterator]();
              first = await iterator.next();
            } catch (error) {
              await iterator?.return?.(undefined);
              fail(error);
              return;
            }

            if (first.done || !first.value) {
              await iterator.return?.(undefined);
              writeJsonResponse(response, 200, { jsonrpc: "2.0", id, result: null });
              return;
            }

            const activated = activatedExtensions(request, a2aConfig);
            writeSseHead(
              response,
              activated.length > 0
                ? { [A2A_EXTENSIONS_HEADER]: activated.join(",") }
                : undefined,
            );
            writeSseResult(response, { jsonrpc: "2.0", id, result: first.value });

            let closed = false;
            const stopListening = onResponseClosed(request, response, () => {
              closed = true;
            });
            try {
              while (!closed) {
                const next = await iterator.next();
                if (next.done || !isResponseWritable(response)) {
                  break;
                }
                writeSseResult(response, { jsonrpc: "2.0", id, result: next.value });
              }
            } catch (error) {
              if (isResponseWritable(response)) {
                writeSseError(response, { jsonrpc: "2.0", id, error: toRpcErrorObject(error) });
              }
            } finally {
              stopListening();
              await iterator.return?.(undefined).catch(() => undefined);
              if (isResponseWritable(response)) {
                response.end();
              }
            }
          }

          const handler = async (
            request: IncomingMessage,
            response: ServerResponse,
          ): Promise<boolean> => {
            // httpx and several other standard clients normalize `base_url` to a
            // trailing slash, so `/a2a/v1/` must mean `/a2a/v1`. Our route
            // grammar has no path segment that is meaningful only with a slash,
            // so one trailing slash is stripped everywhere except root.
            const rawPathname = new URL(request.url ?? "/", "http://localhost").pathname;
            const pathname =
              rawPathname.length > 1 && rawPathname.endsWith("/")
                ? rawPathname.slice(0, -1)
                : rawPathname;

            if (
              request.method === "GET" &&
              (A2A_AGENT_CARD_PATHS as readonly string[]).includes(pathname)
            ) {
              try {
                assertSupportedVersion(request);
              } catch {
                writeJsonResponse(response, 400, { error: "Unsupported A2A version" });
                return true;
              }
              writeCardResponse(request, response, createAgentCard(request, false), "public");
              return true;
            }

            // The authenticated extended card is served at its own path because it
            // publishes operator-authored text and must never be world-readable.
            if (
              (request.method === "GET" || request.method === "POST") &&
              (pathname === A2A_EXTENDED_CARD_PATH ||
                pathname === `${A2A_REST_PATH}/extendedAgentCard`)
            ) {
              const peerName = resolvePeerName(request, a2aConfig);
              if (!peerName) {
                writeJsonResponse(response, 401, {
                  error: "Unauthorized; the extended agent card requires a configured Bearer token",
                });
                return true;
              }
              try {
                assertSupportedVersion(request);
                const methodContext = createMethodContext({ request, peerName });
                const card = await executeA2aMethod(
                  methodContext,
                  { method: "GetExtendedAgentCard" },
                  {},
                );
                writeCardResponse(request, response, card, "private");
              } catch (error) {
                writeJsonResponse(response, 200, {
                  error: toRpcErrorObject(error),
                });
              }
              return true;
            }

            if (a2aConfig.rest?.enabled === true && pathname.startsWith(`${A2A_REST_PATH}/`)) {
              return await handleRestRequest(request, response, pathname);
            }

            if (request.method !== "POST" || pathname !== A2A_JSONRPC_PATH) {
              writeJsonResponse(response, 404, { error: "Not found" });
              return true;
            }

            const peerName = resolvePeerName(request, a2aConfig);
            if (!peerName) {
              writeJsonResponse(response, 401, {
                error: "Unauthorized; configure channels.a2a.peers with a matching Bearer token",
              });
              return true;
            }

            let body: string;
            try {
              assertJsonContentType(request);
              body = await readRequestBodyWithLimit(request, {
                maxBytes: MAX_REQUEST_BODY_BYTES,
                // Defer destruction so the rejection below reaches the peer before the close.
                destroyOnLimit: false,
              });
            } catch (error) {
              const bodyRejection = isRequestBodyLimitError(error, "PAYLOAD_TOO_LARGE")
                ? { statusCode: 413, body: { error: "Request body exceeds the 1 MiB limit" } }
                : isRequestBodyLimitError(error, "REQUEST_BODY_TIMEOUT")
                  ? {
                      statusCode: 200,
                      body: createRpcError(null, -32000, "Request body could not be read"),
                    }
                  : undefined;
              if (bodyRejection) {
                response.setHeader("cache-control", "no-store");
                await sendHttpRequestRejection(
                  request,
                  response,
                  bodyRejection.statusCode,
                  JSON.stringify(bodyRejection.body),
                  "application/json; charset=utf-8",
                );
                return true;
              }
              writeJsonResponse(response, 200, {
                error: toRpcErrorObject(error),
              });
              return true;
            }

            let payload: unknown;
            try {
              payload = JSON.parse(body);
            } catch {
              writeJsonResponse(response, 200, createRpcError(null, A2A_JSON_RPC_ERROR_CODES.PARSE_ERROR, "Parse error"));
              return true;
            }

            if (Array.isArray(payload)) {
              if (payload.length === 0) {
                writeJsonResponse(
                  response,
                  200,
                  createRpcError(null, A2A_JSON_RPC_ERROR_CODES.INVALID_REQUEST, "Invalid JSON-RPC request"),
                );
                return true;
              }
              if (payload.length > MAX_BATCH_REQUESTS) {
                const error = isRateLimited(peerName)
                  ? createRpcError(null, -32000, "Peer is rate limited")
                  : createRpcError(
                      null,
                      -32000,
                      `A2A batch exceeds the ${MAX_BATCH_REQUESTS} request limit`,
                    );
                writeRpcResponse(response, error);
                return true;
              }
              const responses = (
                await Promise.all(
                  payload.map((entry) => processRpcRequest(entry, peerName, request)),
                )
              ).filter((entry): entry is A2aRpcResponse => entry !== undefined);
              const result = responses.length > 0 ? responses : undefined;
              if (result) {
                writeRpcResponse(response, result);
              } else {
                response.statusCode = 200;
                response.end();
              }
              return true;
            }

            if (isRecord(payload) && isA2aStreamingMethod(String(payload.method ?? ""))) {
              await processStreamingRpcRequest(payload, peerName, request, response);
              return true;
            }

            const result = await processRpcRequest(payload, peerName, request);
            if (result) {
              writeRpcResponse(response, result);
            } else {
              response.statusCode = 200;
              response.end();
            }
            return true;
          };

          /**
           * HTTP+JSON/REST binding (spec §11).
           *
           * Paths mirror the normative `google.api.http` annotations. Errors use
           * the ProtoJSON form of `google.rpc.Status` so peers can read the
           * `ErrorInfo` reason instead of guessing from the status code.
           */
          async function handleRestRequest(
            request: IncomingMessage,
            response: ServerResponse,
            pathname: string,
          ): Promise<boolean> {
            const peerName = resolvePeerName(request, a2aConfig);
            if (!peerName) {
              writeJsonResponse(response, 401, { error: "Unauthorized" });
              return true;
            }
            const method = request.method ?? "GET";
            const suffix = pathname.slice(A2A_REST_PATH.length) || "/";
            const query = new URL(request.url ?? "/", "http://localhost").searchParams;
            const methodContext = createMethodContext({ request, peerName });

            const sendResult = (value: unknown, status = 200) => {
              writeJsonResponse(response, status, value);
            };
            const sendError = (error: unknown) => {
              const rpcError = toRpcErrorObject(error);
              const status = restStatusForCode(rpcError.code);
              sendResult(
                {
                  error: {
                    code: status,
                    status: restStatusNameForCode(rpcError.code),
                    message: rpcError.message,
                    // Only A2A-specific errors carry an ErrorInfo reason
                    // (§11.6). A plain 404 for an unmatched route is not an A2A
                    // error type, so its details stay empty.
                    details: rpcError.data ?? [],
                  },
                },
                status,
              );
            };

            try {
              assertSupportedVersion(request);
              assertRequiredExtensions(request, a2aConfig);
              const result = await runRestRoute({
                method,
                suffix,
                query,
                request,
                response,
                methodContext,
              });
              if (result === undefined) {
                // Streams own the response; nothing further to write.
                return true;
              }
              if (result === null) {
                response.statusCode = 204;
                response.end();
                return true;
              }
              sendResult(result);
              return true;
            } catch (error) {
              sendError(error);
              return true;
            }
          }

          /** Resolve one REST route to an A2A method; streams return undefined. */
          async function runRestRoute(params: {
            method: string;
            suffix: string;
            query: URLSearchParams;
            request: IncomingMessage;
            response: ServerResponse;
            methodContext: A2aMethodContext;
          }): Promise<unknown> {
            const { method, suffix, query, request, response, methodContext } = params;
            const readBody = async (): Promise<unknown> => {
              const raw = await readRequestBodyWithLimit(request, {
                maxBytes: MAX_REQUEST_BODY_BYTES,
                destroyOnLimit: false,
              });
              return raw.trim() ? JSON.parse(raw) : {};
            };
            // No `id` parameter: the REST binding emits bare `StreamResponse`
            // frames with no JSON-RPC envelope to correlate (§11.5).
            const stream = async (
              resolved: NonNullable<ReturnType<typeof resolveA2aRpcMethod>>,
              requestParams: unknown,
            ) => {
              const iterator = streamA2aMethod(
                methodContext,
                resolved,
                requestParams,
              )[Symbol.asyncIterator]();
              const first = await iterator.next();
              if (first.done || !first.value) {
                await iterator.return?.(undefined);
                return;
              }
              writeSseHead(response);
              // The REST binding streams bare `StreamResponse` objects, with no
              // JSON-RPC envelope around them.
              writeSseResult(response, first.value);
              let closed = false;
              const stop = onResponseClosed(request, response, () => {
                closed = true;
              });
              try {
                while (!closed) {
                  const next = await iterator.next();
                  if (next.done || !isResponseWritable(response)) {
                    break;
                  }
                  writeSseResult(response, next.value);
                }
              } finally {
                stop();
                await iterator.return?.(undefined).catch(() => undefined);
                if (isResponseWritable(response)) {
                  response.end();
                }
              }
              return undefined;
            };

            if (suffix === "/message:send" && method === "POST") {
              return await executeA2aMethod(
                methodContext,
                { method: "SendMessage" },
                await readBody(),
              );
            }
            if (suffix === "/message:stream" && method === "POST") {
              await stream(
                { method: "SendStreamingMessage", capability: "streaming", streaming: true },
                await readBody(),
              );
              return undefined;
            }
            if (suffix === "/tasks" && method === "GET") {
              return await executeA2aMethod(methodContext, { method: "ListTasks" }, {
                ...(query.get("contextId") ? { contextId: query.get("contextId") } : {}),
                ...(query.get("status") ? { status: query.get("status") } : {}),
                ...(query.get("pageSize") ? { pageSize: Number(query.get("pageSize")) } : {}),
                ...(query.get("pageToken") ? { pageToken: query.get("pageToken") } : {}),
                ...(query.get("historyLength")
                  ? { historyLength: Number(query.get("historyLength")) }
                  : {}),
                ...(query.get("includeArtifacts")
                  ? { includeArtifacts: query.get("includeArtifacts") === "true" }
                  : {}),
              });
            }

            const taskMatch = /^\/tasks\/([^/]+?)(?::(cancel|subscribe))?$/u.exec(suffix);
            if (taskMatch) {
              const taskId = decodeURIComponent(taskMatch[1] ?? "");
              const action = taskMatch[2];
              if (action === "cancel" && method === "POST") {
                return await executeA2aMethod(
                  methodContext,
                  { method: "CancelTask" },
                  { id: taskId, ...((await readBody()) as object) },
                );
              }
              if (action === "subscribe" && (method === "GET" || method === "POST")) {
                await stream(
                  { method: "SubscribeToTask", capability: "streaming", streaming: true },
                  { id: taskId },
                );
                return undefined;
              }
              if (!action && method === "GET") {
                return await executeA2aMethod(methodContext, { method: "GetTask" }, {
                  id: taskId,
                  ...(query.get("historyLength")
                    ? { historyLength: Number(query.get("historyLength")) }
                    : {}),
                });
              }
            }

            const pushMatch = /^\/tasks\/([^/]+)\/pushNotificationConfigs(?:\/([^/]+))?$/u.exec(
              suffix,
            );
            if (pushMatch) {
              const taskId = decodeURIComponent(pushMatch[1] ?? "");
              const configId = pushMatch[2] ? decodeURIComponent(pushMatch[2]) : undefined;
              if (!configId && method === "POST") {
                return await executeA2aMethod(
                  methodContext,
                  { method: "CreateTaskPushNotificationConfig", capability: "pushNotifications" },
                  await readBody(),
                );
              }
              if (!configId && method === "GET") {
                return await executeA2aMethod(
                  methodContext,
                  { method: "ListTaskPushNotificationConfigs", capability: "pushNotifications" },
                  {
                    taskId,
                    ...(query.get("pageSize") ? { pageSize: Number(query.get("pageSize")) } : {}),
                    ...(query.get("pageToken") ? { pageToken: query.get("pageToken") } : {}),
                  },
                );
              }
              if (configId && method === "GET") {
                return await executeA2aMethod(
                  methodContext,
                  { method: "GetTaskPushNotificationConfig", capability: "pushNotifications" },
                  { taskId, id: configId },
                );
              }
              if (configId && method === "DELETE") {
                await executeA2aMethod(
                  methodContext,
                  { method: "DeleteTaskPushNotificationConfig", capability: "pushNotifications" },
                  { taskId, id: configId },
                );
                return null;
              }
            }

            throw methodNotFound(`${method} ${A2A_REST_PATH}${suffix}`);
          }

          for (const routePath of [...A2A_AGENT_CARD_PATHS, A2A_JSONRPC_PATH, A2A_EXTENDED_CARD_PATH]) {
            unregisterRoutes.push(
              // A2A owns fixed global paths on a single account, so a duplicate
              // registration means a stale or conflicting owner. Fail loudly instead
              // of replacing a live handler (GHSA-RQP8-Q22P-5J9Q).
              registerPluginHttpRoute({
                path: routePath,
                auth: "plugin",
                match: "exact",
                pluginId: "a2a",
                source: "a2a-gateway",
                accountId: account.accountId,
                throwOnFailure: true,
                handler,
              }),
            );
          }
          if (a2aConfig.rest?.enabled === true) {
            unregisterRoutes.push(
              registerPluginHttpRoute({
                path: A2A_REST_PATH,
                auth: "plugin",
                match: "prefix",
                pluginId: "a2a",
                source: "a2a-gateway-rest",
                accountId: account.accountId,
                throwOnFailure: true,
                handler,
              }),
            );
          }

          ctx.setStatus(channelReadyPatch({ accountId: account.accountId }));
          await waitUntilAbort(ctx.abortSignal);
        } finally {
          // Stop admission before releasing blocked responses; otherwise shutdown can
          // admit a task after its lifecycle-owned waiters and timers were cleared.
          for (const unregister of unregisterRoutes.toReversed()) {
            unregister();
          }
          stopPushObserver();
          pushSender?.reset();
          pushConfigs.clear();
          store.stop();
          ctx.setStatus(channelStoppedPatch({ accountId: account.accountId }));
        }
      },
    },
    message: a2aChannelMessageAdapter,
  },
  outbound: {
    base: { deliveryMode: "direct" },
    attachedResults: {
      channel: A2A_CHANNEL_ID,
      sendText: sendA2aChannelText,
    },
  },
});

/**
 * Map an A2A JSON-RPC error code to the HTTP status used by the REST binding.
 *
 * The v1.0.0 specification table (§5.4) assigns several A2A errors their own
 * HTTP status instead of a blanket 400: a cancelable-state violation is 409
 * (the task state conflicts with the request), an unsupported media type is
 * 415, and an invalid agent response is 502 (the failure originated behind
 * the gateway, in the agent).
 */
function restStatusForCode(code: number): number {
  switch (code) {
    case A2A_JSON_RPC_ERROR_CODES.TASK_NOT_FOUND:
    case A2A_JSON_RPC_ERROR_CODES.METHOD_NOT_FOUND:
      return 404;
    case A2A_JSON_RPC_ERROR_CODES.TASK_NOT_CANCELABLE:
      return 409;
    case A2A_JSON_RPC_ERROR_CODES.CONTENT_TYPE_NOT_SUPPORTED:
      return 415;
    case A2A_JSON_RPC_ERROR_CODES.INVALID_AGENT_RESPONSE:
      return 502;
    case A2A_JSON_RPC_ERROR_CODES.INTERNAL_ERROR:
      return 500;
    default:
      return 400;
  }
}

/**
 * Canonical `google.rpc.Code` name for the REST binding.
 *
 * §5.4 assigns a gRPC status per A2A error type, and the §11.6 example carries it
 * in `error.status`. The name is not derivable from the HTTP status alone: 400
 * covers both FAILED_PRECONDITION and UNIMPLEMENTED, and peers branch on the
 * name rather than re-deriving it.
 */
function restStatusNameForCode(code: number): string {
  switch (code) {
    case A2A_JSON_RPC_ERROR_CODES.TASK_NOT_FOUND:
    case A2A_JSON_RPC_ERROR_CODES.METHOD_NOT_FOUND:
      return "NOT_FOUND";
    case A2A_JSON_RPC_ERROR_CODES.CONTENT_TYPE_NOT_SUPPORTED:
    case A2A_JSON_RPC_ERROR_CODES.INVALID_PARAMS:
    case A2A_JSON_RPC_ERROR_CODES.INVALID_REQUEST:
    case A2A_JSON_RPC_ERROR_CODES.PARSE_ERROR:
      return "INVALID_ARGUMENT";
    case A2A_JSON_RPC_ERROR_CODES.INVALID_AGENT_RESPONSE:
    case A2A_JSON_RPC_ERROR_CODES.INTERNAL_ERROR:
      return "INTERNAL";
    case A2A_JSON_RPC_ERROR_CODES.PUSH_NOTIFICATION_NOT_SUPPORTED:
    case A2A_JSON_RPC_ERROR_CODES.UNSUPPORTED_OPERATION:
    case A2A_JSON_RPC_ERROR_CODES.VERSION_NOT_SUPPORTED:
      return "UNIMPLEMENTED";
    default:
      return "FAILED_PRECONDITION";
  }
}

/** Echo the extensions the agent actually activated for this request. */
function activatedExtensions(
  request: IncomingMessage,
  a2aConfig: A2aChannelConfig,
): string[] {
  const requested = parseServiceParameterHeader(request.headers[A2A_EXTENSIONS_HEADER]);
  if (requested.length === 0) {
    return [];
  }
  const declared = new Set((a2aConfig.extensions ?? []).map((entry) => entry.uri));
  return requested.filter((uri) => declared.has(uri));
}

/**
 * Refuse requests that skip an extension the card marks `required` (§4.6).
 *
 * A required extension changes how the agent interprets the request, so serving
 * the call anyway would answer a different question than the peer asked. The
 * opt-in is per request, so the check has to run on every call rather than once
 * at startup.
 */
function assertRequiredExtensions(request: IncomingMessage, a2aConfig: A2aChannelConfig): void {
  const activated = new Set(activatedExtensions(request, a2aConfig));
  const missing = (a2aConfig.extensions ?? [])
    .filter((entry) => entry.required === true && !activated.has(entry.uri))
    .map((entry) => entry.uri);
  if (missing.length > 0) {
    throw extensionSupportRequired(missing);
  }
}

