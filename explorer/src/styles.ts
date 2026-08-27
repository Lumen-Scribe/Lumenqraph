export const styles = `
  :root {
    color-scheme: light dark;
    --bg: #ffffff;
    --surface: #f6f7f9;
    --surface-2: #eef0f3;
    --border: #d8dce3;
    --ink: #1a1d24;
    --ink-2: #4a505c;
    --muted: #7b8291;
    --accent: #5566ff;
    --accent-soft: #5566ff1f;
    --good: #1f9d57;
    --warn: #b5820a;
    --crit: #d23b3b;
    --good-soft: #1f9d571f;
    --warn-soft: #b5820a1f;
    --crit-soft: #d23b3b1f;
    --radius: 10px;
  }
  @media (prefers-color-scheme: dark) {
    :root {
      --bg: #14161b;
      --surface: #1b1e25;
      --surface-2: #22262f;
      --border: #333944;
      --ink: #e8eaef;
      --ink-2: #b3b9c5;
      --muted: #7f8794;
      --accent: #8f9cff;
      --accent-soft: #8f9cff26;
      --good: #46c983;
      --warn: #e0b64b;
      --crit: #f0736f;
      --good-soft: #46c98326;
      --warn-soft: #e0b64b26;
      --crit-soft: #f0736f26;
    }
  }
  * { box-sizing: border-box; }
  body {
    font: 15px/1.55 system-ui, -apple-system, Segoe UI, sans-serif;
    margin: 0; background: var(--bg); color: var(--ink);
  }
  .container { max-width: 1200px; margin-inline: auto; padding: 1.25rem 1.25rem 3rem; }
  header.app { display: flex; flex-wrap: wrap; align-items: center; gap: .75rem 1rem; margin-bottom: 1.25rem; }
  .brand { display: flex; align-items: baseline; gap: .55rem; }
  .brand h1 { margin: 0; font-size: 1.4rem; letter-spacing: -.01em; }
  .brand .tag { color: var(--muted); font-size: .82rem; }
  .conn { display: flex; flex-wrap: wrap; gap: .4rem; margin-left: auto; }
  input, button, select {
    font: inherit; padding: .42rem .6rem; border-radius: 8px;
    border: 1px solid var(--border); background: var(--bg); color: inherit;
  }
  input::placeholder { color: var(--muted); }
  button { cursor: pointer; background: var(--surface); }
  button.primary { background: var(--accent); border-color: var(--accent); color: #fff; }
  button:hover { filter: brightness(1.05); }
  a { color: var(--accent); }

  .kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: .7rem; margin-bottom: 1.4rem; }
  .kpi { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); padding: .7rem .85rem; }
  .kpi .label { font-size: .72rem; text-transform: uppercase; letter-spacing: .05em; color: var(--muted); }
  .kpi .value { font-size: 1.45rem; font-weight: 600; margin-top: .15rem; font-variant-numeric: tabular-nums; }
  .kpi .value.small { font-size: 1.05rem; font-weight: 600; }
  .status { display: inline-flex; align-items: center; gap: .4rem; }
  .dot { width: .62rem; height: .62rem; border-radius: 50%; background: var(--muted); flex: none; }
  .dot.good { background: var(--good); } .dot.warn { background: var(--warn); } .dot.crit { background: var(--crit); }

  .grid { display: grid; grid-template-columns: 300px 1fr; gap: 1.1rem; align-items: start; }
  @media (max-width: 820px) { .grid { grid-template-columns: 1fr; } }
  .panel { background: var(--surface); border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
  .panel > h2 { margin: 0; font-size: .8rem; text-transform: uppercase; letter-spacing: .05em; color: var(--muted); padding: .7rem .85rem; border-bottom: 1px solid var(--border); }

  .clist { list-style: none; margin: 0; padding: .35rem; max-height: 72vh; overflow: auto; }
  .clist li { padding: .5rem .6rem; border-radius: 8px; cursor: pointer; }
  .clist li:hover { background: var(--surface-2); }
  .clist li.active { background: var(--accent-soft); outline: 1px solid var(--accent); }
  .clist .cid { font-family: ui-monospace, monospace; font-size: .74rem; word-break: break-all; }
  .clist .meta { font-size: .74rem; color: var(--muted); margin-top: .1rem; }

  .tabs { display: flex; flex-wrap: wrap; gap: .25rem; padding: .55rem .6rem; border-bottom: 1px solid var(--border); }
  .tabs button { padding: .3rem .7rem; border-radius: 999px; font-size: .82rem; }
  .tabs button.active { background: var(--accent); border-color: var(--accent); color: #fff; }
  .detail-head { padding: .7rem .85rem; border-bottom: 1px solid var(--border); font-size: .8rem; color: var(--ink-2); }
  .detail-head code { font-family: ui-monospace, monospace; word-break: break-all; }
  .decodebar { display: flex; gap: .4rem; align-items: center; flex-wrap: wrap; }
  .decodebar input { flex: 1 1 16rem; min-width: 0; }
  .detail-title { margin-top: .5rem; }
  .detail-title:empty { display: none; }
  .body { padding: .5rem; overflow-x: auto; }

  table { width: 100%; border-collapse: collapse; font-size: 12.5px; }
  th, td { text-align: left; padding: .4rem .55rem; border-bottom: 1px solid var(--border); vertical-align: top; }
  th { color: var(--muted); font-weight: 600; position: sticky; top: 0; background: var(--surface); }
  td code, .mono { font-family: ui-monospace, monospace; font-size: 11.5px; word-break: break-all; }
  tr:hover td { background: var(--surface-2); }
  .pill { display: inline-block; padding: .05rem .45rem; border-radius: 999px; background: var(--accent-soft); color: var(--accent); font-size: 11px; font-weight: 600; }
  a.ext { text-decoration: none; white-space: nowrap; }
  a.ext:hover { text-decoration: underline; }
  a.ext code { color: var(--accent); }
  pre { margin: 0; padding: .6rem .7rem; background: var(--surface-2); border-radius: 8px; font-size: 11.5px; overflow-x: auto; }
  .empty { padding: 1.5rem; color: var(--muted); text-align: center; }
  .rowbar { display: flex; gap: .45rem; flex-wrap: wrap; align-items: center; padding: .5rem .6rem; }
  .rowbar input { padding: .3rem .5rem; }
  .toast { position: fixed; bottom: 1rem; left: 50%; transform: translateX(-50%); background: var(--crit); color: #fff; padding: .5rem .9rem; border-radius: 8px; font-size: .85rem; opacity: 0; transition: opacity .2s; pointer-events: none; }
  .toast.show { opacity: 1; }
  .muted { color: var(--muted); }

  .summary { padding: .5rem .7rem; font-size: 12.5px; color: var(--ink-2); border-bottom: 1px solid var(--border); display: flex; gap: .5rem; flex-wrap: wrap; align-items: center; }
  .timeline { list-style: none; margin: 0; padding: .8rem .7rem .3rem 1.1rem; }
  .tl-item { position: relative; padding: 0 0 1.15rem 1.15rem; border-left: 2px solid var(--border); }
  .tl-item:last-child { border-left-color: transparent; padding-bottom: .3rem; }
  .tl-dot { position: absolute; left: -.47rem; top: .2rem; width: .72rem; height: .72rem; border-radius: 50%;
            background: var(--muted); border: 2px solid var(--bg); }
  .tl-dot.crit { background: var(--crit); } .tl-dot.good { background: var(--good); }
  .tl-head { display: flex; align-items: center; gap: .45rem; flex-wrap: wrap; }
  .tl-head .ver { font-weight: 600; font-variant-numeric: tabular-nums; }
  .tl-when { font-size: 12px; color: var(--muted); }
  .tl-hash { font-family: ui-monospace, monospace; font-size: 11px; color: var(--muted); margin-top: .15rem; }
  .tl-note { font-size: 12.5px; color: var(--muted); margin-top: .3rem; }
  .badge { font-size: 10px; font-weight: 700; padding: .08rem .42rem; border-radius: 999px;
           text-transform: uppercase; letter-spacing: .04em; }
  .badge.crit { background: var(--crit-soft); color: var(--crit); }
  .badge.good { background: var(--good-soft); color: var(--good); }
  .badge.mute { background: var(--surface-2); color: var(--muted); }
  .changes { list-style: none; margin: .45rem 0 0; padding: 0; }
  .changes li { font-family: ui-monospace, monospace; font-size: 11.5px; line-height: 1.5;
                padding: .08rem .4rem; border-radius: 4px; white-space: pre-wrap; word-break: break-word; }
  .changes li.add { color: var(--good); background: var(--good-soft); }
  .changes li.rm  { color: var(--crit); background: var(--crit-soft); }
  .changes li.ch  { color: var(--warn); background: var(--warn-soft); }
  .changes li.sub { background: none; padding-left: 1.6rem; opacity: .95; }
`;

export function injectStyles() {
  const style = document.createElement('style');
  style.textContent = styles;
  document.head.appendChild(style);
}
