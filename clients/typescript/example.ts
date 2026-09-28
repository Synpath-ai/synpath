/**
 * The step's exit criterion: a TypeScript user places, watches and cancels an
 * order without Python. Types come from the generated `synpath.d.ts`, so a
 * wrong field name is a compile error rather than a runtime surprise.
 */
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import WebSocket from "ws";
import type { components } from "./src/synpath.js";  // generated: npm run types; see synpath.dev/docs/sdks/typescript

type Order = components["schemas"]["Order"];
type OrderRequest = components["schemas"]["OrderRequest"];
type PageResponseOrder = components["schemas"]["PageResponse_Order_"];

// Your own `synpath serve`: market data at /, trading at /trading.
const server = process.env.SYNPATH_SERVER ?? "http://127.0.0.1:8000";
const base = server.replace(/\/$/, "") + "/trading";

// The server's access token: given, or, for a server on this machine, read from where
// `synpath serve` left it (~/.synpath/servers.json, readable by you only).
function localKey(url: string): string | undefined {
  const { hostname, port, protocol } = new URL(url);
  if (!["127.0.0.1", "localhost", "::1"].includes(hostname)) return undefined;
  const name = `127.0.0.1:${port || (protocol === "https:" ? 443 : 80)}`;
  try {
    const home = process.env.SYNPATH_HOME ?? join(homedir(), ".synpath");
    return JSON.parse(readFileSync(join(home, "servers.json"), "utf8"))[name]?.access_token ?? undefined;
  } catch {
    return undefined;
  }
}
const key = process.env.SYNPATH_ACCESS_TOKEN ?? localKey(server);
if (!key) throw new Error(`no access token for ${server}: set SYNPATH_ACCESS_TOKEN, or start synpath serve on this machine`);
const headers = { authorization: `Bearer ${key}`, "content-type": "application/json" };

async function call<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(base + path, { ...init, headers: { ...headers, ...(init.headers ?? {}) } });
  if (!response.ok) {
    const { error } = await response.json();          // { code, message, details } on every route
    throw new Error(`${init.method ?? "GET"} ${path} → ${response.status} ${error.code}: ${error.message}`);
  }
  return (await response.json()) as T;
}

const seen: string[] = [];
const socket = new WebSocket(`${base.replace(/^http/, "ws")}/ws/events?key=${key}&since=0`);
await new Promise<void>((resolve) => socket.on("open", () => resolve()));
socket.on("message", (raw) => seen.push(JSON.parse(raw.toString()).kind));

const request: OrderRequest = {
  market_id: "kalshi:KX-A",
  side: "buy",
  amount: "5",
  type: "limit",
  price: "0.35",
  book: "typescript",
  account: { venue: "kalshi", name: "desk-a", subaccount: null },
};

const placed = await call<Order>("/orders", { method: "POST", body: JSON.stringify(request) });
console.log(`placed  ${placed.id} ${placed.side} ${placed.amount} at ${placed.price} (${placed.status})`);

const open = await call<PageResponseOrder>("/orders");
console.log(`open    ${open.count} order(s): ${open.data.map((o) => o.id).join(", ")}`);

const canceled = await call<Order>(`/orders/${placed.id}`, { method: "DELETE" });
console.log(`cancel  ${canceled.id} → ${canceled.status}`);

await new Promise((resolve) => setTimeout(resolve, 600));
socket.close();
const interesting = seen.filter((k) => k.startsWith("order.") || k.startsWith("intent."));
console.log(`watched ${seen.length} events, including ${[...new Set(interesting)].join(", ")}`);

const after = await call<PageResponseOrder>("/orders");
if (after.count !== 0) throw new Error(`expected no open orders, got ${after.count}`);
console.log("done    the account is flat, no Python was involved");
