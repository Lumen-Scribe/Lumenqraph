// --- begin doc example ---
const crypto = require("crypto");

const TOLERANCE_SECS = 300;

// Verifies an X-Lumenqraph-Signature header: "t=<unix>,v1=<hex>[,v1=<hex>...]".
// `rawBody` must be the exact bytes of the HTTP body (a string or Buffer).
function verifyWebhookSignature(rawBody, signatureHeader, secret, nowSecs = Math.floor(Date.now() / 1000)) {
  // Parse "t=<ts>,v1=<hex>[,v1=<hex>...]"
  let timestampText = null;
  const signatures = [];
  for (const part of signatureHeader.split(",")) {
    const idx = part.indexOf("=");
    if (idx === -1) continue;
    const key = part.slice(0, idx).trim();
    const value = part.slice(idx + 1).trim();
    if (key === "t") timestampText = value;
    else if (key === "v1") signatures.push(value);
  }

  const timestamp = Number(timestampText);
  if (timestampText === null || !Number.isFinite(timestamp) || signatures.length === 0) {
    return false;
  }

  // Reject stale or future-dated deliveries (replay protection)
  if (Math.abs(nowSecs - timestamp) > TOLERANCE_SECS) {
    return false;
  }

  // HMAC-SHA256 over "{t}.{body}", using the timestamp text exactly as sent
  const expected = crypto
    .createHmac("sha256", secret)
    .update(`${timestampText}.`)
    .update(rawBody)
    .digest();

  // Accept if any v1 matches (supports secret rotation)
  return signatures.some((sig) => {
    const a = Buffer.from(sig.toLowerCase(), "hex");
    return a.length === expected.length && crypto.timingSafeEqual(a, expected);
  });
}

// Usage in Express.js: the raw body must be captured before JSON parsing.
//
// app.post("/webhooks/lumenqraph", express.raw({ type: "application/json" }), (req, res) => {
//   const signature = req.headers["x-lumenqraph-signature"];
//   const secret = process.env.LUMENQRAPH_WEBHOOK_SECRET;
//
//   if (!signature || !verifyWebhookSignature(req.body, signature, secret)) {
//     return res.status(401).json({ error: "Invalid signature" });
//   }
//
//   const payload = JSON.parse(req.body);
//   console.log("Delivery:", payload.id, payload.type);
//   res.json({ status: "ok" });
// });
// --- end doc example ---

module.exports = { verifyWebhookSignature, TOLERANCE_SECS };
