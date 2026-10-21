import type { ServerResponse } from "node:http";
import { A2A_JSON_RPC_ERROR_CODES, A2aProtocolError } from "./a2a-errors.js";

export type A2aRpcErrorObject = { code: number; message: string; data?: unknown };

// TEMP(local-dev shim): inline of openclaw/plugin-sdk/http-response-runtime's
// sendJsonNoStore, which the 2026.10.5-release runtime does not export yet.
// Revert to the SDK import when the host version matches this checkout.
export function writeJsonResponse(res: ServerResponse, status: number, body: unknown): void {
  res.setHeader("Cache-Control", "no-store");
  res.statusCode = status;
  res.setHeader("Content-Type", "application/json; charset=utf-8");
  res.end(JSON.stringify(body));
}

/** Render any thrown error as a JSON-RPC `error` object. */
export function toRpcErrorObject(error: unknown): A2aRpcErrorObject {
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
