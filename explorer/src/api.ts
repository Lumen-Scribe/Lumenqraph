import type { Health, Contract, EventRecord, Transfer, ContractState, ContractData, InterfaceInfo, InterfaceHistory } from './types';

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

export async function listContracts(): Promise<Contract[]> {
  return get('/contracts');
}

export async function listEvents(contractId: string, limit = 50): Promise<EventRecord[]> {
  return get(`/contracts/${encodeURIComponent(contractId)}/events?limit=${limit}`);
}

export async function listTransfers(contractId: string, limit = 50): Promise<Transfer[]> {
  return get(`/contracts/${encodeURIComponent(contractId)}/transfers?limit=${limit}`);
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
