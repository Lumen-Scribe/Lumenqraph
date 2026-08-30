import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const distDir = path.join(__dirname, '../dist');
const jsFile = path.join(distDir, 'explorer.js');
const outputDir = path.join(__dirname, '..');
const outputHtml = path.join(outputDir, 'index.html');
const outputJs = path.join(outputDir, 'app.js');

// Read the generated JavaScript
const js = fs.readFileSync(jsFile, 'utf-8');

// Write app.js as a standalone external file.
// This is required for a strict Content-Security-Policy that disallows inline
// scripts (script-src 'self').  The API serves explorer/ with:
//   Content-Security-Policy: default-src 'self'; script-src 'self'; …
// An external <script src="app.js"> satisfies 'self'; an inline <script> block
// would not, and represents a stored-XSS risk through unsanitised contract data.
fs.writeFileSync(outputJs, js);
console.log(`Written ${outputJs}`);

// Generate the final HTML file referencing the external script.
const html = `<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Lumenqraph Explorer</title>
  <link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg%20xmlns%3D%27http%3A%2F%2Fwww.w3.org%2F2000%2Fsvg%27%20viewBox%3D%270%200%2032%2032%27%3E%3Crect%20width%3D%2732%27%20height%3D%2732%27%20rx%3D%277%27%20fill%3D%27%235566ff%27%2F%3E%3Cpath%20d%3D%27M8%2024L16%209L24%2019%27%20fill%3D%27none%27%20stroke%3D%27%23fff%27%20stroke-width%3D%273%27%20stroke-linecap%3D%27round%27%20stroke-linejoin%3D%27round%27%2F%3E%3Ccircle%20cx%3D%278%27%20cy%3D%2724%27%20r%3D%274%27%20fill%3D%27%23fff%27%2F%3E%3Ccircle%20cx%3D%2716%27%20cy%3D%279%27%20r%3D%274%27%20fill%3D%27%23fff%27%2F%3E%3Ccircle%20cx%3D%2724%27%20cy%3D%2719%27%20r%3D%274%27%20fill%3D%27%23fff%27%2F%3E%3C%2Fsvg%3E" />
</head>
<body>
<div class="container">
  <header class="app">
    <div class="brand">
      <h1>Lumenqraph</h1>
      <span class="tag">Explorer &amp; self-host dashboard</span>
    </div>
    <div class="conn">
      <input id="base" size="26" value="" placeholder="API base (blank = same origin)" title="API base URL — leave blank when the page is served by/behind the API" />
      <input id="key" size="16" placeholder="x-api-key (optional)" />
      <select id="senet" title="Network — auto-detected from the connected API; pick one to switch to its remembered API base">
        <option value="public">mainnet</option>
        <option value="testnet">testnet</option>
      </select>
    </div>
  </header>

  <section class="kpis" id="kpis">
    <div class="kpi"><div class="label">Indexer</div><div class="value small"><span class="status"><span id="statusDot" class="dot"></span><span id="statusText">—</span></span></div></div>
    <div class="kpi"><div class="label">Lag (ledgers)</div><div class="value" id="kLag">—</div></div>
    <div class="kpi"><div class="label">Processed ledger</div><div class="value small" id="kProcessed">—</div></div>
    <div class="kpi"><div class="label">Chain tip</div><div class="value small" id="kTip">—</div></div>
    <div class="kpi"><div class="label">Events indexed</div><div class="value" id="kEvents">—</div></div>
    <div class="kpi"><div class="label">Errors</div><div class="value" id="kErrors">—</div></div>
  </section>

  <div class="grid">
    <aside class="panel">
      <h2>Indexed contracts</h2>
      <ul class="clist" id="clist"><li class="muted" style="cursor:default">Loading…</li></ul>
    </aside>

    <main class="panel">
      <div class="tabs" id="tabs">
        <button data-tab="events" class="active">Events</button>
        <button data-tab="transfers">Transfers</button>
        <button data-tab="state">State</button>
        <button data-tab="holders">Holders</button>
        <button data-tab="interface">Interface</button>
        <button data-tab="upgrades">Upgrades</button>
      </div>
      <div class="detail-head" id="detailHead">
        <div class="decodebar">
          <span class="muted">Decode any contract:</span>
          <input id="anyCid" size="40" placeholder="contract id (C…)" />
          <button id="decodeBtn">Decode</button>
        </div>
        <div id="detailTitle" class="detail-title"></div>
      </div>
      <div class="body" id="detail"><div class="empty">No contract selected.</div></div>
    </main>
  </div>
</div>
<div class="toast" id="toast"></div>

<!-- External script satisfies Content-Security-Policy: script-src 'self'
     served by the API.  Do NOT move this back to an inline <script> block. -->
<script src="app.js"></script>
</body>
</html>`;

fs.writeFileSync(outputHtml, html);
console.log(`Generated ${outputHtml}`);

