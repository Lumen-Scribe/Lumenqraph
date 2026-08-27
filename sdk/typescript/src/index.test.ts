import { describe, it, expect, vi } from "vitest";
import { LumenqraphClient, LumenqraphError } from "./index.js";

// ---- helpers ----

/** Build a minimal event-shaped node for GraphQL page mocks. */
const eventNode = (id: string) => ({
  eventId: id,
  contractId: "C1",
  ledger: 1,
  ledgerClosedAt: "2024-01-01T00:00:00Z",
  eventType: "contract",
  eventName: "transfer",
  decodedTopics: null,
  decodedValue: null,
  enriched: null,
  txHash: "0xabc",
  inSuccessfulCall: true,
});

/** Build a well-formed GraphQL eventsPage response body. */
function gqlPage(
  ids: string[],
  hasNextPage: boolean,
  endCursor: string | null,
) {
  return {
    data: {
      events: {
        edges: ids.map((id) => ({ cursor: id, node: eventNode(id) })),
        pageInfo: { hasNextPage, endCursor },
      },
    },
  };
}

/** A vi.fn() that returns the given responses in order for each fetch call. */
function mockFetch(responses: Array<{ status: number; body: unknown }>) {
  let idx = 0;
  return vi.fn().mockImplementation(() => {
    const r = responses[idx++] ?? { status: 200, body: null };
    const text = JSON.stringify(r.body);
    return Promise.resolve({
      ok: r.status >= 200 && r.status < 300,
      status: r.status,
      statusText: r.status === 200 ? "OK" : "Error",
      headers: {
        get: () => null,
      },
      text: () => Promise.resolve(text),
    });
  });
}

function client(fetch: typeof globalThis.fetch, apiKey?: string) {
  return new LumenqraphClient({
    baseUrl: "http://api.example.com/",
    apiKey,
    fetch,
  });
}

// ---- URL / query construction ----

describe("URL construction", () => {
  it("listContracts hits /contracts", async () => {
    const f = mockFetch([{ status: 200, body: [] }]);
    await client(f).listContracts();
    expect(f.mock.calls[0]?.[0]).toBe("http://api.example.com/contracts");
  });

  it("trailing slash in baseUrl is stripped (no double slash in path)", async () => {
    const f = mockFetch([{ status: 200, body: [] }]);
    await client(f).listContracts();
    const url = new URL(f.mock.calls[0]?.[0] as string);
    expect(url.pathname).toBe("/contracts");
  });

  it("getState appends limit param", async () => {
    const body = { contract_id: "C1", count: 0, versions: [] };
    const f = mockFetch([{ status: 200, body }]);
    await client(f).getState("C1", { limit: 5 });
    const url = new URL(f.mock.calls[0]?.[0] as string);
    expect(url.pathname).toBe("/contracts/C1/state");
    expect(url.searchParams.get("limit")).toBe("5");
  });

  it("listEvents appends limit, offset, and event_name", async () => {
    const f = mockFetch([{ status: 200, body: [] }]);
    await client(f).listEvents("C1", {
      limit: 10,
      offset: 20,
      eventName: "transfer",
    });
    const url = new URL(f.mock.calls[0]?.[0] as string);
    expect(url.pathname).toBe("/contracts/C1/events");
    expect(url.searchParams.get("limit")).toBe("10");
    expect(url.searchParams.get("offset")).toBe("20");
    expect(url.searchParams.get("event_name")).toBe("transfer");
  });

  it("listTransfers without contractId hits /transfers", async () => {
    const f = mockFetch([{ status: 200, body: [] }]);
    await client(f).listTransfers();
    expect(new URL(f.mock.calls[0]?.[0] as string).pathname).toBe(
      "/transfers",
    );
  });

  it("listTransfers with contractId hits /contracts/:id/transfers", async () => {
    const f = mockFetch([{ status: 200, body: [] }]);
    await client(f).listTransfers("C1");
    expect(new URL(f.mock.calls[0]?.[0] as string).pathname).toBe(
      "/contracts/C1/transfers",
    );
  });

  it("contract ID with special chars is URL-encoded", async () => {
    const f = mockFetch([{ status: 200, body: [] }]);
    await client(f).listEvents("C1/slash&amp");
    const url = new URL(f.mock.calls[0]?.[0] as string);
    expect(url.pathname).toBe("/contracts/C1%2Fslash%26amp/events");
  });

  it("call POSTs to /contracts/:id/call", async () => {
    const body = {
      contract_id: "C1",
      function: "balance",
      result: 0,
      simulated_at_ledger: 1,
    };
    const f = mockFetch([{ status: 200, body }]);
    await client(f).call("C1", { function: "balance" });
    const [url, init] = f.mock.calls[0] as [string, RequestInit];
    expect(new URL(url).pathname).toBe("/contracts/C1/call");
    expect(init.method).toBe("POST");
  });

  it("getData appends label param when provided", async () => {
    const body = { contract_id: "C1", count: 0, keys: [] };
    const f = mockFetch([{ status: 200, body }]);
    await client(f).getData("C1", { label: "balance", limit: 10 });
    const url = new URL(f.mock.calls[0]?.[0] as string);
    expect(url.searchParams.get("label")).toBe("balance");
    expect(url.searchParams.get("limit")).toBe("10");
  });

  it("undefined query params are omitted", async () => {
    const body = { contract_id: "C1", count: 0, versions: [] };
    const f = mockFetch([{ status: 200, body }]);
    await client(f).getState("C1");
    const url = new URL(f.mock.calls[0]?.[0] as string);
    expect(url.searchParams.has("limit")).toBe(false);
  });
});

// ---- Auth header injection ----

describe("auth headers", () => {
  it("sends x-api-key when apiKey is provided", async () => {
    const f = mockFetch([{ status: 200, body: [] }]);
    await client(f, "my-secret-key").listContracts();
    const headers = (f.mock.calls[0]?.[1] as RequestInit).headers as Headers;
    expect(headers.get("x-api-key")).toBe("my-secret-key");
  });

  it("does not send x-api-key when apiKey is absent", async () => {
    const f = mockFetch([{ status: 200, body: [] }]);
    await client(f).listContracts();
    const headers = (f.mock.calls[0]?.[1] as RequestInit).headers as Headers;
    expect(headers.get("x-api-key")).toBeNull();
  });

  it("sends x-api-key on POST requests too", async () => {
    const body = {
      contract_id: "C1",
      function: "f",
      result: null,
      simulated_at_ledger: 1,
    };
    const f = mockFetch([{ status: 200, body }]);
    await client(f, "k").call("C1", { function: "f" });
    const headers = (f.mock.calls[0]?.[1] as RequestInit).headers as Headers;
    expect(headers.get("x-api-key")).toBe("k");
  });
});

// ---- Error handling ----

describe("error handling", () => {
  it("throws LumenqraphError on 401", async () => {
    const f = mockFetch([{ status: 401, body: { error: "invalid API key" } }]);
    await expect(client(f).listContracts()).rejects.toThrow(LumenqraphError);
  });

  it("throws LumenqraphError on 500", async () => {
    const f = mockFetch([{ status: 500, body: { error: "internal error" } }]);
    await expect(client(f).listContracts()).rejects.toThrow(LumenqraphError);
  });

  it("error.status matches the HTTP status code", async () => {
    const f = mockFetch([{ status: 404, body: { error: "not found" } }]);
    await expect(client(f).listContracts()).rejects.toMatchObject({
      status: 404,
      name: "LumenqraphError",
    });
  });

  it("uses the error.error field from the response body", async () => {
    const f = mockFetch([
      { status: 401, body: { error: "invalid API key" } },
    ]);
    await expect(client(f).listContracts()).rejects.toMatchObject({
      message: "invalid API key",
    });
  });

  it("falls back to status text when body has no error field", async () => {
    const f = mockFetch([
      { status: 503, body: "Service Unavailable" },
      { status: 503, body: "Service Unavailable" },
      { status: 503, body: "Service Unavailable" },
      { status: 503, body: "Service Unavailable" },
    ]);
    await expect(client(f).listContracts()).rejects.toThrow(LumenqraphError);
  });

  it("throws LumenqraphError (status 200) for a GraphQL errors array", async () => {
    const f = mockFetch([
      {
        status: 200,
        body: { data: null, errors: [{ message: "field not found" }] },
      },
    ]);
    await expect(client(f).graphql("{ bad }")).rejects.toMatchObject({
      name: "LumenqraphError",
      status: 200,
    });
  });

  it("includes all GraphQL error messages in the thrown error", async () => {
    const f = mockFetch([
      {
        status: 200,
        body: {
          errors: [{ message: "err1" }, { message: "err2" }],
        },
      },
    ]);
    await expect(client(f).graphql("{ bad }")).rejects.toMatchObject({
      message: expect.stringContaining("err1"),
    });
  });
});

// ---- paginateEvents ----

describe("paginateEvents", () => {
  async function collect(gen: AsyncGenerator<unknown>) {
    const items: unknown[] = [];
    for await (const item of gen) items.push(item);
    return items;
  }

  it("yields nothing on an empty first page", async () => {
    const f = mockFetch([{ status: 200, body: gqlPage([], false, null) }]);
    const items = await collect(client(f).paginateEvents("C1"));
    expect(items).toHaveLength(0);
    expect(f).toHaveBeenCalledTimes(1);
  });

  it("yields all events on a single page", async () => {
    const f = mockFetch([
      { status: 200, body: gqlPage(["e1", "e2", "e3"], false, null) },
    ]);
    const items = await collect(client(f).paginateEvents("C1"));
    expect(items).toHaveLength(3);
  });

  it("fetches multiple pages until hasNextPage is false", async () => {
    const f = mockFetch([
      { status: 200, body: gqlPage(["e1", "e2"], true, "cur-1") },
      { status: 200, body: gqlPage(["e3", "e4"], true, "cur-2") },
      { status: 200, body: gqlPage(["e5"], false, null) },
    ]);
    const items = await collect(client(f).paginateEvents("C1"));
    expect(items).toHaveLength(5);
    expect(f).toHaveBeenCalledTimes(3);
  });

  it("terminates when hasNextPage is false even if endCursor is set", async () => {
    const f = mockFetch([
      { status: 200, body: gqlPage(["e1"], false, "ignored-cursor") },
    ]);
    const items = await collect(client(f).paginateEvents("C1"));
    expect(items).toHaveLength(1);
    expect(f).toHaveBeenCalledTimes(1);
  });

  it("passes the previous endCursor as after on subsequent page requests", async () => {
    const f = mockFetch([
      { status: 200, body: gqlPage(["e1"], true, "my-cursor") },
      { status: 200, body: gqlPage(["e2"], false, null) },
    ]);
    await collect(client(f).paginateEvents("C1"));
    const secondBody = JSON.parse(
      (f.mock.calls[1]?.[1] as RequestInit).body as string,
    ) as { variables: { after: string } };
    expect(secondBody.variables.after).toBe("my-cursor");
  });

  it("uses pageSize option as first variable", async () => {
    const f = mockFetch([{ status: 200, body: gqlPage([], false, null) }]);
    await collect(client(f).paginateEvents("C1", { pageSize: 25 }));
    const body = JSON.parse(
      (f.mock.calls[0]?.[1] as RequestInit).body as string,
    ) as { variables: { first: number } };
    expect(body.variables.first).toBe(25);
  });
});
