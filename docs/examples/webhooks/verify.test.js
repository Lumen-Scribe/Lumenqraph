// Checks verify.js against the committed cross-language test vector.
// Run with: node --test docs/examples/webhooks/verify.test.js
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const path = require("path");
const { verifyWebhookSignature, TOLERANCE_SECS } = require("./verify.js");

const vector = JSON.parse(
  fs.readFileSync(path.join(__dirname, "test_vector.json"), "utf8")
);

test("accepts the committed vector (with a rotated, non-matching v1 first)", () => {
  assert.equal(
    verifyWebhookSignature(vector.body, vector.header, vector.secret, vector.now),
    true
  );
});

test("rejects a tampered body", () => {
  assert.equal(
    verifyWebhookSignature(vector.body + " ", vector.header, vector.secret, vector.now),
    false
  );
});

test("rejects a wrong secret", () => {
  assert.equal(
    verifyWebhookSignature(vector.body, vector.header, "wrong-secret", vector.now),
    false
  );
});

test("rejects a delivery outside the tolerance window", () => {
  assert.equal(
    verifyWebhookSignature(
      vector.body,
      vector.header,
      vector.secret,
      vector.timestamp + TOLERANCE_SECS + 1
    ),
    false
  );
});

test("rejects a header without a timestamp or signature", () => {
  assert.equal(verifyWebhookSignature(vector.body, "v1=abc", vector.secret, vector.now), false);
  assert.equal(verifyWebhookSignature(vector.body, `t=${vector.timestamp}`, vector.secret, vector.now), false);
});
