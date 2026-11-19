import type { Health, Contract, EventRecord, Transfer, ContractState, ContractData, InterfaceInfo, InterfaceHistory, Page } from './types';

export function getBase(): string {
  return (document.getElementById('base') as HTMLInputElement).value.replace(/\/+$/, '');
}

export function getHeaders(): Record<string, string> {
  const key = (document.getElementById('key') as HTMLInputElement).value;
  return key ? { 'x-api-key': key } : {};
}

export async function get<T>(path: string): Promise<T> {
  const r = await fetch(getBase() + path, { headers: getHeaders() });
  if (!r.ok) {
    let msg = 'HTTP ' + r.status;
    try {
      const b = await r.json() as { error?: string };
      if (b.error) msg = b.error;
    } catch {
      // Ignore JSON parse errors, use HTTP status text
    }
    throw new Error(msg);
  }
  return r.json() as Promise<T>;
}

export async function health(): Promise<Health> {
  return get('/health');
}

/** Append query parameters, skipping any that are undefined or empty. */
function withQuery(path: string, params: Record<string, string | number | undefined>): string {
  const parts = Object.entries(params)
    .filter(([, v]) => v !== undefined && v !== '')
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(String(v))}`);
  return parts.length ? `${path}?${parts.join('&')}` : path;
}

// The list endpoints return the Page<T> envelope, not a bare array (#127, #445).
// Callers render `page.data` and follow `page.next_cursor` for more rows.

export async function listContracts(after?: string): Promise<Page<Contract>> {
  return get(withQuery('/contracts', { after }));
}

export async function listEvents(contractId: string, limit = 50, after?: string): Promise<Page<EventRecord>> {
  return get(withQuery(`/contracts/${encodeURIComponent(contractId)}/events`, { limit, after }));
}

export async function listTransfers(contractId: string, limit = 50, after?: string): Promise<Page<Transfer>> {
  return get(withQuery(`/contracts/${encodeURIComponent(contractId)}/transfers`, { limit, after }));
}

export async function getState(contractId: string, limit = 1): Promise<ContractState> {
  return get(`/contracts/${encodeURIComponent(contractId)}/state?limit=${limit}`);
}

export async function getData(contractId: string, limit = 200, label = ''): Promise<ContractData> {
  const query = label ? `?label=${encodeURIComponent(label)}&limit=${limit}` : `?limit=${limit}`;
  return get(`/contracts/${encodeURIComponent(contractId)}/data${query}`);
}

export async function getInterface(contractId: string, version?: number): Promise<InterfaceInfo> {
  const query = version ? `?version=${version}` : '';
  return get(`/contracts/${encodeURIComponent(contractId)}/interface${query}`);
}

export async function getInterfaceHistory(contractId: string): Promise<InterfaceHistory> {
  return get(`/contracts/${encodeURIComponent(contractId)}/interface/history?limit=100`);
}
