import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const distDir = path.join(__dirname, '../dist');
const jsFile = path.join(distDir, 'explorer.js');
const explorerDir = path.join(__dirname, '..');
const htmlFile = path.join(explorerDir, 'index.html');
const outputJs = path.join(explorerDir, 'app.js');

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

// index.html is a hand-maintained source file and is NOT generated here, so
// edits to it are never overwritten by the build. We only verify that it still
// loads the bundle this script produces.
const html = fs.readFileSync(htmlFile, 'utf-8');
if (!html.includes('<script src="app.js"></script>')) {
  console.error(`${htmlFile} must load the bundle with <script src="app.js"></script>`);
  process.exit(1);
}
console.log(`Checked ${htmlFile} (not modified by the build)`);
