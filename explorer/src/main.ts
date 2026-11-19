import { injectStyles } from './styles';
import * as api from './api';
import * as ui from './ui';
import type { Contract, EventRecord, Transfer } from './types';

let selected: string | null = null;
let tab = 'events';
let healthTimer: NodeJS.Timeout | null = null;
let mounts: Record<string, string> = {};
let ifaceVersion: number | null = null;

// Contract list, paged by `next_cursor`. Polling refreshes the first page
// without discarding pages the user has already loaded.
let contractRows: Contract[] = [];
let contractCursor: string | null = null;
let contractHasMore = false;

// The events or transfers table for the selected contract, paged the same way.
interface Listing {
  kind: 'events' | 'transfers';
  cid: string;
  rows: Array<EventRecord | Transfer>;
  cursor: string | null;
  hasMore: boolean;
}
let listing: Listing | null = null;

function $<T extends HTMLElement>(id: string): T {
  return document.getElementById(id) as T;
}

async function loadHealth(): Promise<void> {
  try {
    const h = await api.health();
    ui.updateHealth(h);
    if (h.mounts) mounts = { ...mounts, ...h.mounts };
    ui.updateNetworkLinks(h.network);
  } catch (e) {
    const dot = $<HTMLElement>('statusDot');
    dot.className = 'dot crit';
    const text = $<HTMLElement>('statusText');
    text.textContent = 'unreachable';
    ui.toast('Health: ' + (e instanceof Error ? e.message : String(e)));
  }
}

function renderContracts(): void {
  ui.updateContractList(contractRows, selected, contractHasMore);
}

async function loadContracts(): Promise<void> {
  try {
    const page = await api.listContracts();
    // Keep rows from pages loaded earlier. Only the first page is refreshed.
    const extra = contractRows.filter(r => !page.data.some(f => f.contract_id === r.contract_id));
    if (extra.length === 0) {
      contractCursor = page.next_cursor;
      contractHasMore = page.has_more;
    }
    contractRows = [...page.data, ...extra];
    renderContracts();
    if (!selected && contractRows[0]) selectContract(contractRows[0].contract_id);
  } catch (e) {
    ui.toast('Contracts: ' + (e instanceof Error ? e.message : String(e)));
  }
}

async function loadMoreContracts(): Promise<void> {
  if (!contractHasMore || !contractCursor) return;
  try {
    const page = await api.listContracts(contractCursor);
    const seen = new Set(contractRows.map(r => r.contract_id));
    contractRows = [...contractRows, ...page.data.filter(r => !seen.has(r.contract_id))];
    contractCursor = page.next_cursor;
    contractHasMore = page.has_more;
    renderContracts();
  } catch (e) {
    ui.toast('Contracts: ' + (e instanceof Error ? e.message : String(e)));
  }
}

function renderListing(): void {
  if (!listing) return;
  const table = listing.kind === 'events'
    ? ui.renderEventsTable(listing.rows as EventRecord[])
    : ui.renderTransfersTable(listing.rows as Transfer[]);
  ui.setContent(table + ui.loadMoreControl(listing.hasMore));
}

async function loadMoreRows(): Promise<void> {
  const current = listing;
  if (!current || !current.hasMore || !current.cursor) return;
  const btn = document.getElementById('loadMoreBtn') as HTMLButtonElement | null;
  if (btn) btn.disabled = true;
  try {
    const page = current.kind === 'events'
      ? await api.listEvents(current.cid, 50, current.cursor)
      : await api.listTransfers(current.cid, 50, current.cursor);
    // The user may have switched contract or tab while this request was in flight.
    if (listing !== current) return;
    current.rows = [...current.rows, ...page.data];
    current.cursor = page.next_cursor;
    current.hasMore = page.has_more;
    renderListing();
  } catch (e) {
    ui.toast('Load more: ' + (e instanceof Error ? e.message : String(e)));
    if (btn) btn.disabled = false;
  }
}

function selectContract(cid: string): void {
  selected = cid;
  ifaceVersion = null;
  document.querySelectorAll('.clist li').forEach(li => li.classList.remove('active'));
  [...document.querySelectorAll('.clist li')].forEach(li => {
    const cidEl = li.querySelector('.cid');
    if (cidEl && cid.startsWith(cidEl.textContent!.split('…')[0])) li.classList.add('active');
  });
  ui.setDetailTitle(cid);
  loadTab();
}

function setTab(t: string): void {
  ifaceVersion = null;
  tab = t;
  document.querySelectorAll('.tabs button').forEach(b => b.classList.toggle('active', (b as HTMLElement & {dataset:{tab:string}}).dataset.tab === t));
  loadTab();
}

function getSeNet(): string {
  return ($<HTMLSelectElement>('senet')).value;
}

function mountBase(path: string): string {
  const b = api.getBase();
  if (!b) return path;
  try { return new URL(b, location.href).origin + path; } catch { return path; }
}

function netChanged(): void {
  const want = getSeNet() === 'public' ? 'mainnet' : 'testnet';
  const saved = localStorage.getItem('lq.base.' + want);
  if (saved !== null && saved !== api.getBase()) {
    ($<HTMLInputElement>('base')).value = saved;
    ui.toast(`Switching to the ${want} API…`);
  } else if (saved === null && mounts[want]) {
    ($<HTMLInputElement>('base')).value = mountBase(mounts[want]);
    ui.toast(`Switching to the ${want} API…`);
  } else if (saved === null) {
    ui.toast(`No ${want} API known yet — enter its URL in the API base field.`);
  }
  connect();
}

function baseChanged(): void {
  connect();
}

function connect(): void {
  localStorage.setItem('lq.base', api.getBase());
  localStorage.setItem('lq.key', ($<HTMLInputElement>('key')).value);
  loadHealth();
  loadContracts();
  if (healthTimer) clearInterval(healthTimer);
  let tick = 0;
  healthTimer = setInterval(() => { loadHealth(); if (++tick % 3 === 0) loadContracts(); }, 10000);
}

async function loadTab(): Promise<void> {
  if (!selected) { ui.setContent('<div class="empty">No contract selected.</div>'); return; }
  const cid = selected;
  ui.setLoading();
  try {
    if (tab === 'events' || tab === 'transfers') {
      const kind: Listing['kind'] = tab === 'transfers' ? 'transfers' : 'events';
      const page = kind === 'events' ? await api.listEvents(cid) : await api.listTransfers(cid);
      listing = { kind, cid, rows: page.data, cursor: page.next_cursor, hasMore: page.has_more };
      renderListing();
    } else if (tab === 'state') {
      const s = await api.getState(cid);
      const v = s.versions && s.versions[0];
      ui.setContent(v
        ? `<div class="muted" style="padding:.4rem .6rem">Instance storage @ ledger ${Number(v.ledger).toLocaleString()} · captured ${ui.esc(v.captured_at)}</div><pre>${ui.esc(JSON.stringify(v.storage, null, 2))}</pre>`
        : '<div class="empty">No state snapshots (enable STATE_INDEXING on the indexer).</div>');
    } else if (tab === 'holders') {
      const d = await api.getData(cid);
      ui.setContent(ui.table(d.keys, [
        { label: 'Holder / key', render: r => `<code>${ui.esc(JSON.stringify(r.key).slice(0, 28))}</code>` },
        { label: 'Value', render: r => `<code>${ui.esc(JSON.stringify(r.value))}</code>` },
        { label: 'Ledger', key: 'ledger' },
      ]));
    } else if (tab === 'interface') {
      const iface = await api.getInterface(cid, ifaceVersion ?? undefined);
      ui.setContent(ui.renderInterface(iface, ifaceVersion ?? undefined, cid));
    } else if (tab === 'upgrades') {
      const h = await api.getInterfaceHistory(cid);
      ui.setContent(ui.renderUpgrades(cid, h.versions || []));
    }
  } catch (e) {
    ui.setContent(`<div class="empty">${ui.esc(e instanceof Error ? e.message : String(e))}</div>`);
  }
}

async function decodeAny(): Promise<void> {
  const cid = ($<HTMLInputElement>('anyCid')).value.trim();
  if (!cid) return;
  selected = cid;
  tab = 'interface';
  ifaceVersion = null;
  document.querySelectorAll('.tabs button').forEach(b => b.classList.toggle('active', (b as HTMLElement & {dataset:{tab:string}}).dataset.tab === 'interface'));
  ui.setDetailTitle(cid);
  ui.setLoading();
  try {
    const iface = await api.getInterface(cid);
    ui.setContent(ui.renderInterface(iface, undefined, cid));
  } catch (e) {
    ui.setContent(`<div class="empty">${ui.esc(e instanceof Error ? e.message : String(e))}</div>`);
  }
}

function viewVersion(cid: string, n: number): void {
  ifaceVersion = n;
  tab = 'interface';
  document.querySelectorAll('.tabs button').forEach(b => b.classList.toggle('active', (b as HTMLElement & {dataset:{tab:string}}).dataset.tab === 'interface'));
  loadTab();
}

// Initialize on page load
window.addEventListener('DOMContentLoaded', () => {
  injectStyles();

  // Restore previous settings
  ($<HTMLInputElement>('base')).value = localStorage.getItem('lq.base') ?? '';
  ($<HTMLInputElement>('key')).value = localStorage.getItem('lq.key') ?? '';

  // Set up event listeners
  ($<HTMLInputElement>('base')).addEventListener('change', baseChanged);
  ($<HTMLInputElement>('key')).addEventListener('change', connect);
  ($<HTMLSelectElement>('senet')).addEventListener('change', netChanged);

  // Set up custom events
  window.addEventListener('selectContract', (e: Event) => {
    const evt = e as CustomEvent<{ cid: string }>;
    selectContract(evt.detail.cid);
  });

  window.addEventListener('setTab', (e: Event) => {
    const evt = e as CustomEvent<{ tab: string }>;
    setTab(evt.detail.tab);
  });

  window.addEventListener('loadMoreContracts', () => { loadMoreContracts(); });

  // One delegate on #detail covers the "Load more" button, which is re-rendered with each table.
  $<HTMLElement>('detail').addEventListener('click', (e: Event) => {
    if ((e.target as HTMLElement).closest('#loadMoreBtn')) loadMoreRows();
  });

  window.addEventListener('viewVersion', (e: Event) => {
    const evt = e as CustomEvent<{ cid: string; version: number }>;
    viewVersion(evt.detail.cid, evt.detail.version);
  });

  // Set up button event listeners
  for (const btn of document.querySelectorAll('.tabs button')) {
    const button = btn as HTMLElement & {dataset:{tab:string}};
    button.addEventListener('click', () => setTab(button.dataset.tab));
  }

  ($<HTMLButtonElement>('decodeBtn')).addEventListener('click', decodeAny);
  ($<HTMLInputElement>('anyCid')).addEventListener('keydown', (e) => {
    if (e.key === 'Enter') decodeAny();
  });

  connect();
});

export {};
