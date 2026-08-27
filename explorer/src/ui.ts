import { getBase, getHeaders } from './api';
import type { EventRecord, Transfer, ContractFunction, ContractEvent, ContractStruct, ContractUnion, ContractEnum, VersionInfo, VersionDiff, DiffSection } from './types';

function $<T extends HTMLElement>(id: string): T {
  return document.getElementById(id) as T;
}

function fmt(n: unknown): string {
  return (n === null || n === undefined) ? '—' : Number(n).toLocaleString();
}

function fmtDate(s: string | undefined): string {
  if (!s) return '—';
  const d = new Date(s);
  return isNaN(d.getTime()) ? esc(s) : d.toLocaleString(undefined, { dateStyle: 'medium', timeStyle: 'short' });
}

export function esc(s: unknown): string {
  return String(s).replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c as '&'|'<'|'>']));
}

function short(s: string, n = 12): string {
  return s.length > n * 2 ? s.slice(0, n) + '…' + s.slice(-n) : s;
}

function cell(v: unknown): string {
  if (v === null || v === undefined) return '<span class="muted">—</span>';
  if (typeof v === 'object') return `<code>${esc(JSON.stringify(v))}</code>`;
  return `<code>${esc(v)}</code>`;
}

const seNet = (): string => ($<HTMLSelectElement>('senet')).value;
const seUrl = (type: string, val: string): string => `https://stellar.expert/explorer/${seNet()}/${type}/${encodeURIComponent(val)}`;

function seLink(type: string, val: string | null, n = 8): string {
  if (!val) return '<span class="muted">—</span>';
  return `<a class="ext" href="${seUrl(type, val)}" target="_blank" rel="noopener" title="${esc(val)}"><code>${esc(short(val, n))}</code> ↗</a>`;
}

export function toast(msg: string): void {
  const t = $<HTMLDivElement>('toast');
  t.textContent = msg;
  t.classList.add('show');
  setTimeout(() => t.classList.remove('show'), 3200);
}

export function updateHealth(data: Record<string, unknown>): void {
  const lag = data.lag_ledgers ?? data.lag ?? null;
  $<HTMLElement>('kLag').textContent = fmt(lag);
  $<HTMLElement>('kProcessed').textContent = fmt(data.last_processed_ledger);
  $<HTMLElement>('kTip').textContent = fmt(data.chain_tip_ledger ?? data.chain_tip);
  $<HTMLElement>('kEvents').textContent = fmt(data.events_ingested_total);
  $<HTMLElement>('kErrors').textContent = fmt(data.errors_total);

  let cls = 'good', label = 'healthy';
  if (lag === null) {
    cls = 'good';
    label = (data.status as string) || 'up';
  } else if ((lag as number) > 500) {
    cls = 'crit';
    label = 'far behind';
  } else if ((lag as number) > 50) {
    cls = 'warn';
    label = 'lagging';
  }
  const dot = $<HTMLElement>('statusDot');
  dot.className = 'dot ' + cls;
  const text = $<HTMLElement>('statusText');
  text.textContent = data.network ? `${label} · ${data.network}` : label;
}

export function updateNetworkLinks(network: string | undefined): void {
  if (network !== 'mainnet' && network !== 'testnet') return;
  localStorage.setItem('lq.base.' + network, getBase());
  const val = network === 'mainnet' ? 'public' : 'testnet';
  if (seNet() !== val) {
    $<HTMLSelectElement>('senet').value = val;
    toast(`This API indexes ${network} — links updated.`);
  }
}

export function updateContractList(contracts: Array<Record<string, unknown>>, selected: string | null): void {
  const ul = $<HTMLUListElement>('clist');
  if (!contracts.length) {
    ul.innerHTML = '<li class="muted" style="cursor:default">No contracts indexed yet.</li>';
    return;
  }
  ul.innerHTML = '';
  for (const c of contracts) {
    const li = document.createElement('li');
    li.className = c.contract_id === selected ? 'active' : '';
    const cid = c.contract_id as string;
    li.innerHTML = `<div class="cid">${esc(short(cid, 10))}
        <a class="ext" href="${seUrl('contract', cid)}" target="_blank" rel="noopener" onclick="event.stopPropagation()" title="View on Stellar.Expert">↗</a></div>
      <div class="meta">${fmt(c.event_count)} events · ledgers ${fmt(c.first_seen_ledger)}–${fmt(c.last_seen_ledger)}</div>`;
    li.onclick = () => window.dispatchEvent(new CustomEvent('selectContract', { detail: { cid } }));
    ul.appendChild(li);
  }
}

export function setDetailTitle(cid: string): void {
  $<HTMLElement>('detailTitle').innerHTML = `Contract <a class="ext" href="${seUrl('contract', cid)}" target="_blank" rel="noopener"><code>${esc(cid)}</code> ↗</a>`;
}

export function table(rows: Array<Record<string, unknown>>, cols: Array<{ label: string; key?: string; render?: (r: Record<string, unknown>) => string }>): string {
  if (!rows || !rows.length) return '<div class="empty">Nothing here yet.</div>';
  let h = '<table><thead><tr>' + cols.map(c => `<th>${c.label}</th>`).join('') + '</tr></thead><tbody>';
  for (const r of rows) {
    h += '<tr>' + cols.map(c => `<td>${c.render ? c.render(r) : cell(r[c.key!])}</td>`).join('') + '</tr>';
  }
  return h + '</tbody></table>';
}

export function renderEventsTable(rows: EventRecord[]): string {
  return table(rows, [
    { label: 'Ledger', key: 'ledger' },
    { label: 'Event', render: r => `<span class="pill">${esc(r.event_name ?? r.event_type)}</span>` },
    { label: 'Typed / decoded', render: r => `<code>${esc(JSON.stringify(r.enriched ?? r.decoded_value))}</code>` },
    { label: 'Tx', render: r => seLink('tx', r.tx_hash) },
  ]);
}

export function renderTransfersTable(rows: Transfer[]): string {
  return table(rows, [
    { label: 'Ledger', key: 'ledger' },
    { label: 'From', render: r => seLink('account', r.from_addr) },
    { label: 'To', render: r => seLink('account', r.to_addr) },
    { label: 'Amount', key: 'amount' },
  ]);
}

export function setContent(html: string): void {
  $<HTMLElement>('detail').innerHTML = html;
}

export function setLoading(): void {
  setContent('<div class="empty">Loading…</div>');
}

function typeRows(i: Record<string, unknown>): Array<[string, string]> {
  const rows: Array<[string, string]> = [];
  const tn = (t: unknown): string => (t && typeof t === 'object') ? JSON.stringify(t) : String(t ?? '?');

  for (const s of (i.structs || []) as ContractStruct[]) {
    rows.push([s.name, `struct ${s.name} { ${(s.fields || []).map(f => `${f.name}: ${tn(f.type)}`).join(', ')} }`]);
  }
  for (const u of (i.unions || []) as ContractUnion[]) {
    rows.push([u.name, `union ${u.name} { ${(u.cases || []).map(c =>
      (c.types && c.types.length) ? `${c.name}(${c.types.map(tn).join(', ')})` : c.name).join(', ')} }`]);
  }
  for (const e of (i.enums || []) as ContractEnum[]) {
    rows.push([e.name, `enum ${e.name} { ${(e.cases || []).map(c => `${c[0]} = ${c[1]}`).join(', ')} }`]);
  }
  return rows;
}

function fnSig(f: ContractFunction): string {
  const tn = (t: unknown): string => (t && typeof t === 'object') ? JSON.stringify(t) : String(t ?? '?');
  const ins = (f.inputs || []).map(a => `${a.name}: ${tn(a.type)}`).join(', ');
  const outs = f.outputs || [];
  const out = outs.length === 0 ? 'void' : outs.length === 1 ? tn(outs[0]) : `(${outs.map(tn).join(', ')})`;
  return `${f.name}(${ins}) -> ${out}`;
}

function evSig(e: ContractEvent): string {
  const tn = (t: unknown): string => (t && typeof t === 'object') ? JSON.stringify(t) : String(t ?? '?');
  const ps = (e.params || []).map(p => `${p.name}: ${tn(p.type)} @${p.location}`).join(', ');
  return `${e.name}(${ps}) [${e.data_format}]`;
}

function sigCell(sig: string, doc?: string): string {
  return `<code>${esc(sig)}</code>${doc ? `<div class="tl-note" style="margin-top:.15rem">${esc(doc)}</div>` : ''}`;
}

export function renderInterface(iface: Record<string, unknown>, version?: number, cid?: string): string {
  const tn = (t: unknown): string => (t && typeof t === 'object') ? JSON.stringify(t) : String(t ?? '?');
  const i = (iface.interface as Record<string, unknown> | undefined) || iface;
  const fns = ((i.functions || []) as ContractFunction[])
    .map(f => `<tr><td><code>${esc(f.name)}</code></td><td>${sigCell(fnSig(f), f.doc)}</td></tr>`).join('');
  const evs = ((i.events || []) as ContractEvent[])
    .map(e => `<tr><td><code>${esc(e.name)}</code></td><td>${sigCell(evSig(e), e.doc)}</td></tr>`).join('');
  const tys = typeRows(i)
    .map(([name, sig]) => `<tr><td><code>${esc(name)}</code></td><td><code>${esc(sig)}</code></td></tr>`).join('');

  const sdkHref = `${getBase()}/contracts/${cid}/sdk?lang=ts${version ? `&version=${version}` : ''}`;
  const sdkLink = `<a href="${sdkHref}" download="${esc(cid!)}${version ? `.v${version}` : ''}.ts" title="Typed, zero-dependency client generated from this interface">TypeScript client ⬇</a>`;
  const head = version
    ? `<div class="summary"><span>Interface at <strong>v${version}</strong></span>
         <span class="muted">·</span><span class="muted">observed ${fmtDate(iface.observed_at as string)}</span>
         <span class="muted">·</span><a href="#" onclick="window.dispatchEvent(new CustomEvent('setTab', {detail:{tab:'interface'}}));return false;">show current</a>
         <span class="muted">·</span>${sdkLink}</div>`
    : `<div class="muted" style="padding:.4rem .6rem">${(i.functions||[]).length} functions · ${(i.events||[]).length} events · ${typeRows(i).length} types${iface.fetched_at ? ' · fetched ' + fmtDate(iface.fetched_at as string) : ''} · ${sdkLink}</div>`;
  const section = (label: string, col: string, rows: string) =>
    rows ? `<div style="height:.6rem"></div><table><thead><tr><th>${label}</th><th>${col}</th></tr></thead><tbody>${rows}</tbody></table>` : '';

  return (head +
     (fns ? `<table><thead><tr><th>Function</th><th>Signature</th></tr></thead><tbody>${fns}</tbody></table>` : '') +
     section('Event', 'Schema', evs) +
     section('Type', 'Definition', tys)) ||
    '<div class="empty">No interface indexed (Stellar Asset Contracts have none).</div>';
}

function diffLines(diff: VersionDiff): string {
  const out: string[] = [];
  for (const [kind, sec] of [['function', diff.functions], ['event', diff.events], ['type', diff.types]]) {
    if (!sec) continue;
    for (const sig of (sec as DiffSection).removed || []) out.push(`<li class="rm">− removed ${kind} ${esc(sig)}</li>`);
    for (const it of (sec as DiffSection).changed || []) {
      out.push(`<li class="ch">~ changed ${kind} ${esc(it.name)}</li>`);
      out.push(`<li class="rm sub">− ${esc(it.from)}</li>`);
      out.push(`<li class="add sub">+ ${esc(it.to)}</li>`);
    }
    for (const sig of (sec as DiffSection).added || []) out.push(`<li class="add">+ added ${kind} ${esc(sig)}</li>`);
  }
  return out.length
    ? `<ul class="changes">${out.join('')}</ul>`
    : '<div class="tl-note">Interface unchanged — code-only upgrade.</div>';
}

export function renderVersionItem(cid: string, v: VersionInfo): string {
  const isBaseline = v.version === 1;
  const dot = v.breaking ? 'crit' : isBaseline ? '' : 'good';
  const badge = v.breaking
    ? '<span class="badge crit">breaking</span>'
    : isBaseline
      ? '<span class="badge mute">baseline</span>'
      : '<span class="badge good">compatible</span>';

  let body;
  if (isBaseline) {
    body = '<div class="tl-note">First interface observed — the baseline this history is measured from, not an upgrade.</div>';
  } else if (!v.diff) {
    body = '<div class="tl-note">Upgraded, but the previous interface could not be re-parsed — no diff available.</div>';
  } else {
    body = diffLines(v.diff);
  }

  const prev = v.previous_wasm_hash ? ` ← ${esc(short(v.previous_wasm_hash, 6))}` : '';
  return `<li class="tl-item">
    <span class="tl-dot ${dot}"></span>
    <div class="tl-head">
      <span class="ver">v${v.version}</span>
      ${badge}
      <span class="tl-when">${fmtDate(v.observed_at)}</span>
      <a href="#" onclick="window.dispatchEvent(new CustomEvent('viewVersion', {detail:{cid:'${esc(cid)}',version:${v.version}}}));return false;" title="Show the full interface at this version">interface</a>
    </div>
    <div class="tl-hash" title="${esc(v.wasm_hash || '')}">wasm ${esc(short(v.wasm_hash || '—', 6))}${prev}</div>
    ${body}
  </li>`;
}

export function renderUpgrades(cid: string, versions: VersionInfo[]): string {
  const vs = versions || [];
  if (!vs.length) return '<div class="empty">No interface history for this contract.</div>';

  const upgrades = vs.filter(v => v.version > 1).length;
  const breaking = vs.filter(v => v.breaking).length;
  const summary = upgrades === 0
    ? '<span>No upgrades observed — this contract\'s interface hasn\'t changed since indexing began.</span>'
    : `<span><strong>${fmt(upgrades)}</strong> upgrade${upgrades === 1 ? '' : 's'} observed</span>
       <span class="muted">·</span>
       <span>${breaking ? `<span class="badge crit">${fmt(breaking)} breaking</span>` : '<span class="badge good">none breaking</span>'}</span>`;

  return `<div class="summary">${summary}</div>
     <ol class="timeline">${vs.map(v => renderVersionItem(cid, v)).join('')}</ol>`;
}
