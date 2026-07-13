/**
 * Lumenqraph TypeScript SDK — a typed client over the Lumenqraph REST + GraphQL
 * API. Zero runtime dependencies: it uses the platform `fetch` (Node 18+ or the
 * browser).
 *
 * ```ts
 * import { LumenqraphClient } from "@lumenqraph/sdk";
 *
 * const lq = new LumenqraphClient({ baseUrl: "http://localhost:8080" });
 * const contracts = await lq.listContracts();
 * for await (const ev of lq.paginateEvents(contracts[0].contract_id)) {
 *   console.log(ev.event_name, ev.enriched ?? ev.decoded_value);
 * }
 * ```
 */

// ---- Types ----

export type Json = unknown;

export interface Contract {
  contract_id: string;
  event_count: number;
  first_seen_ledger: number | null;
  last_seen_ledger: number | null;
}

export interface EventRecord {
  event_id: string;
  contract_id: string;
  ledger: number;
  ledger_closed_at: string;
  event_type: string;
  topics: string[];
  decoded_topics: Json;
  event_name: string | null;
  value: string;
  decoded_value: Json;
  /** Named, typed record from the contract's on-chain spec; null when none. */
  enriched: Json | null;
  tx_hash: string;
  in_successful_call: boolean;
  paging_token: string;
  created_at: string;
}

export interface Transfer {
  event_id: string;
  contract_id: string;
  from_addr: string | null;
  to_addr: string | null;
  amount: string;
  ledger: number;
  ledger_closed_at: string;
}

export interface StateVersion {
  ledger: number;
  storage: Json;
  captured_at: string;
}

export interface ContractState {
  contract_id: string;
  count: number;
  versions: StateVersion[];
}

export interface DataKey {
  key_hash: string;
  key: Json;
  durability: string;
  ledger: number;
  value: Json;
  label: string | null;
  captured_at: string;
}

export interface ContractData {
  contract_id: string;
  count: number;
  keys: DataKey[];
}

export interface DataKeyHistory {
  contract_id: string;
  key_hash: string;
  key: Json;
  durability: string;
  label: string | null;
  count: number;
  versions: { ledger: number; value: Json; captured_at: string }[];
}

export interface CallResult {
  contract_id: string;
  function: string;
  result: Json;
  simulated_at_ledger: number;
  /** Present for `simulate`: the events the call would emit. */
  events?: Json[];
  /** Present for `simulate`: the minimum resource fee, in stroops. */
  min_resource_fee?: string;
}

export interface CallOptions {
  function: string;
  /** Arguments: an object keyed by parameter name, or a positional array. */
  args?: Json;
  /** Optional `G…` source account for the simulation. */
  sourceAccount?: string;
}

/** A Relay-style page returned by the GraphQL cursor connections. */
export interface Page<T> {
  nodes: T[];
  endCursor: string | null;
  hasNextPage: boolean;
}

export interface ClientOptions {
  /** Base URL of the Lumenqraph API, e.g. `http://localhost:8080`. */
  baseUrl: string;
  /** API key, sent as `x-api-key` when `REQUIRE_API_KEY` is enabled. */
  apiKey?: string;
  /** Override the fetch implementation (defaults to global `fetch`). */
  fetch?: typeof fetch;
}

/** Error thrown for any non-2xx API response. */
export class LumenqraphError extends Error {
  constructor(
    message: string,
    readonly status: number,
    readonly body: unknown,
  ) {
    super(message);
    this.name = "LumenqraphError";
  }
}

// ---- Client ----

export class LumenqraphClient {
  private readonly baseUrl: string;
  private readonly apiKey?: string;
  private readonly doFetch: typeof fetch;

  constructor(opts: ClientOptions) {
    this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
    this.apiKey = opts.apiKey;
    const f = opts.fetch ?? globalThis.fetch;
    if (!f) {
      throw new Error(
        "no fetch implementation available; pass one via ClientOptions.fetch",
      );
    }
    this.doFetch = f.bind(globalThis);
  }

  // ---- REST ----

  /** Liveness + indexing-lag report. */
  health(): Promise<Json> {
    return this.get("/health");
  }

  /** Contracts the indexer has seen, with per-contract event counts. */
  listContracts(): Promise<Contract[]> {
    return this.get("/contracts");
  }

  /** A contract's decoded on-chain interface (functions, events, types). */
  getInterface(contractId: string): Promise<Json> {
    return this.get(`/contracts/${enc(contractId)}/interface`);
  }

  /** Versioned instance-storage snapshots, newest first (`limit=1` = current). */
  getState(contractId: string, opts: { limit?: number } = {}): Promise<ContractState> {
    return this.get(`/contracts/${enc(contractId)}/state`, { limit: opts.limit });
  }

  /** Latest value of every per-key entry (e.g. holder balances). */
  getData(
    contractId: string,
    opts: { label?: string; limit?: number } = {},
  ): Promise<ContractData> {
    return this.get(`/contracts/${enc(contractId)}/data`, {
      label: opts.label,
      limit: opts.limit,
    });
  }

  /** The version history of a single per-key entry (e.g. one balance). */
  getDataKey(
    contractId: string,
    keyHash: string,
    opts: { limit?: number } = {},
  ): Promise<DataKeyHistory> {
    return this.get(`/contracts/${enc(contractId)}/data/${enc(keyHash)}`, {
      limit: opts.limit,
    });
  }

  /** Recent events for a contract, newest first (limit/offset). */
  listEvents(
    contractId: string,
    opts: { limit?: number; offset?: number; eventName?: string } = {},
  ): Promise<EventRecord[]> {
    return this.get(`/contracts/${enc(contractId)}/events`, {
      limit: opts.limit,
      offset: opts.offset,
      event_name: opts.eventName,
    });
  }

  /** Materialized SEP-41 transfers, newest first (limit/offset). */
  listTransfers(
    contractId?: string,
    opts: { limit?: number; offset?: number } = {},
  ): Promise<Transfer[]> {
    const path = contractId
      ? `/contracts/${enc(contractId)}/transfers`
      : `/transfers`;
    return this.get(path, { limit: opts.limit, offset: opts.offset });
  }

  /** A contract's callable view functions and their typed signatures. */
  listFunctions(contractId: string): Promise<Json> {
    return this.get(`/contracts/${enc(contractId)}/functions`);
  }

  /** Invoke a view function read-only and get a typed result. */
  call(contractId: string, opts: CallOptions): Promise<CallResult> {
    return this.post(`/contracts/${enc(contractId)}/call`, {
      function: opts.function,
      args: opts.args ?? null,
      source_account: opts.sourceAccount,
    });
  }

  /** Dry-run any call and preview its result, emitted events, and cost. */
  simulate(contractId: string, opts: CallOptions): Promise<CallResult> {
    return this.post(`/contracts/${enc(contractId)}/simulate`, {
      function: opts.function,
      args: opts.args ?? null,
      source_account: opts.sourceAccount,
    });
  }

  // ---- GraphQL ----

  /** Execute a raw GraphQL query against `/graphql`. */
  async graphql<T = Json>(
    query: string,
    variables: Record<string, unknown> = {},
  ): Promise<T> {
    const body = await this.post<{ data?: T; errors?: { message: string }[] }>(
      "/graphql",
      { query, variables },
    );
    if (body.errors?.length) {
      throw new LumenqraphError(
        `GraphQL error: ${body.errors.map((e) => e.message).join("; ")}`,
        200,
        body.errors,
      );
    }
    return body.data as T;
  }

  /** One cursor page of events via GraphQL. */
  async eventsPage(
    contractId: string,
    opts: { first?: number; after?: string; eventName?: string } = {},
  ): Promise<Page<EventRecord>> {
    const query = `
      query Events($id: String!, $name: String, $first: Int, $after: String) {
        events(contractId: $id, eventName: $name, first: $first, after: $after) {
          edges { cursor node {
            eventId contractId ledger ledgerClosedAt eventType eventName
            decodedTopics decodedValue enriched txHash inSuccessfulCall
          } }
          pageInfo { hasNextPage endCursor }
        }
      }`;
    const data = await this.graphql<{
      events: {
        edges: { cursor: string; node: Record<string, unknown> }[];
        pageInfo: { hasNextPage: boolean; endCursor: string | null };
      };
    }>(query, {
      id: contractId,
      name: opts.eventName ?? null,
      first: opts.first ?? 50,
      after: opts.after ?? null,
    });
    return {
      nodes: data.events.edges.map((e) => e.node as unknown as EventRecord),
      endCursor: data.events.pageInfo.endCursor,
      hasNextPage: data.events.pageInfo.hasNextPage,
    };
  }

  /**
   * Async iterator over *all* of a contract's events via GraphQL cursor
   * pagination — transparently fetching page after page.
   */
  async *paginateEvents(
    contractId: string,
    opts: { pageSize?: number; eventName?: string } = {},
  ): AsyncGenerator<EventRecord> {
    let after: string | undefined;
    for (;;) {
      const page = await this.eventsPage(contractId, {
        first: opts.pageSize ?? 100,
        after,
        eventName: opts.eventName,
      });
      for (const node of page.nodes) yield node;
      if (!page.hasNextPage || !page.endCursor) return;
      after = page.endCursor;
    }
  }

  // ---- Internals ----

  private async get<T = Json>(
    path: string,
    query: Record<string, unknown> = {},
  ): Promise<T> {
    const url = new URL(this.baseUrl + path);
    for (const [k, v] of Object.entries(query)) {
      if (v !== undefined && v !== null) url.searchParams.set(k, String(v));
    }
    return this.request<T>(url.toString(), { method: "GET" });
  }

  private post<T = Json>(path: string, body: unknown): Promise<T> {
    return this.request<T>(this.baseUrl + path, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify(body),
    });
  }

  private async request<T>(url: string, init: RequestInit): Promise<T> {
    const headers = new Headers(init.headers);
    if (this.apiKey) headers.set("x-api-key", this.apiKey);
    const res = await this.doFetch(url, { ...init, headers });
    const text = await res.text();
    const parsed = text ? safeJson(text) : null;
    if (!res.ok) {
      const message =
        (parsed as { error?: string } | null)?.error ??
        `${res.status} ${res.statusText}`;
      throw new LumenqraphError(message, res.status, parsed ?? text);
    }
    return parsed as T;
  }
}

// ---- Helpers ----

function enc(segment: string): string {
  return encodeURIComponent(segment);
}

function safeJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}
