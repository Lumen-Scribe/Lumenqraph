import { test, expect, type Route } from '@playwright/test';

// Issue #636: the API returns the Page<T> envelope for list endpoints. These
// tests stub the API with two pages per list, so they check that the explorer
// renders `data`, follows `next_cursor`, and never iterates the envelope.
// They do not need a running indexer.

const C1 = 'CAAAA1111111111111111111111111111111111111111111111111111111';
const C2 = 'CBBBB2222222222222222222222222222222222222222222222222222222';

const json = (route: Route, body: unknown) =>
  route.fulfill({ status: 200, contentType: 'application/json', body: JSON.stringify(body) });

const contract = (contract_id: string) => ({
  contract_id,
  event_count: 2,
  first_seen_ledger: 1,
  last_seen_ledger: 100,
});

const event = (ledger: number) => ({
  ledger,
  event_name: 'transfer',
  event_type: 'contract',
  enriched: null,
  decoded_value: { amount: ledger },
  tx_hash: 'f'.repeat(63) + String(ledger % 10),
});

const transfer = (ledger: number) => ({
  ledger,
  from_addr: 'GAAA' + 'A'.repeat(50),
  to_addr: 'GBBB' + 'B'.repeat(50),
  amount: String(ledger),
});

test.describe('paginated list endpoints (#636)', () => {
  test.beforeEach(async ({ page }) => {
    await page.route('**/health', (route) =>
      json(route, { status: 'ok', network: 'testnet', lag_ledgers: 0, last_processed_ledger: 100, chain_tip_ledger: 100 }),
    );

    // GET /contracts and its cursor pages. The regex does not match /contracts/<id>/...
    await page.route(/\/contracts(\?.*)?$/, (route) => {
      const after = new URL(route.request().url()).searchParams.get('after');
      if (!after) return json(route, { data: [contract(C1)], has_more: true, next_cursor: 'cursor-2' });
      return json(route, { data: [contract(C2)], has_more: false, next_cursor: null });
    });

    await page.route(/\/contracts\/[^/]+\/events(\?.*)?$/, (route) => {
      const after = new URL(route.request().url()).searchParams.get('after');
      if (!after) return json(route, { data: [event(100)], has_more: true, next_cursor: 'evt-2' });
      return json(route, { data: [event(99)], has_more: false, next_cursor: null });
    });

    await page.route(/\/contracts\/[^/]+\/transfers(\?.*)?$/, (route) => {
      const after = new URL(route.request().url()).searchParams.get('after');
      if (!after) return json(route, { data: [transfer(50)], has_more: true, next_cursor: 'xfer-2' });
      return json(route, { data: [transfer(49)], has_more: false, next_cursor: null });
    });
  });

  test('contract list shows the first page and loads the next one on demand', async ({ page }) => {
    const pageErrors: Error[] = [];
    page.on('pageerror', (err) => pageErrors.push(err));

    await page.goto('/explorer/index.html');
    await expect(page.locator('#clist .cid').first()).toContainText('CAAAA11111');
    await expect(page.locator('#clist')).not.toContainText('CBBBB22222');

    await page.locator('#clist li.more').click();
    await expect(page.locator('#clist')).toContainText('CBBBB22222');
    // Both pages are now loaded and there are no more, so the control is gone.
    await expect(page.locator('#clist li.more')).toHaveCount(0);

    expect(pageErrors).toEqual([]);
  });

  test('events table renders page.data and loads the next page with its cursor', async ({ page }) => {
    const pageErrors: Error[] = [];
    page.on('pageerror', (err) => pageErrors.push(err));

    await page.goto('/explorer/index.html');
    // The first contract is selected automatically, and events is the default tab.
    await expect(page.locator('#detail tbody tr')).toHaveCount(1);
    await expect(page.locator('#loadMoreBtn')).toBeVisible();

    const nextRequest = page.waitForRequest((req) => req.url().includes('/events') && req.url().includes('after=evt-2'));
    await page.locator('#loadMoreBtn').click();
    await nextRequest;

    await expect(page.locator('#detail tbody tr')).toHaveCount(2);
    await expect(page.locator('#detail tbody')).toContainText('99');
    await expect(page.locator('#loadMoreBtn')).toHaveCount(0);

    expect(pageErrors).toEqual([]);
  });

  test('transfers table pages the same way', async ({ page }) => {
    await page.goto('/explorer/index.html');
    await page.locator('.tabs button', { hasText: 'Transfers' }).click();

    await expect(page.locator('#detail tbody tr')).toHaveCount(1);
    await page.locator('#loadMoreBtn').click();
    await expect(page.locator('#detail tbody tr')).toHaveCount(2);
    await expect(page.locator('#detail tbody')).toContainText('49');
  });
});
