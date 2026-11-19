import { test, expect, type Page } from '@playwright/test';

// Issue #638: the API key must not persist in localStorage unless the user opts in.
const PAGE = '/explorer/index.html';
const KEY = 'test-api-key-123';

async function typeKey(page: Page, value: string): Promise<void> {
  await page.locator('#key').fill(value);
  await page.locator('#key').dispatchEvent('change');
}

async function storageDump(page: Page): Promise<{ local: Record<string, string>; session: Record<string, string> }> {
  return page.evaluate(() => {
    const dump = (s: Storage) => Object.fromEntries(Object.keys(s).map(k => [k, s.getItem(k) ?? '']));
    return { local: dump(localStorage), session: dump(sessionStorage) };
  });
}

test.describe('API key storage', () => {
  test('key is kept for the session only by default', async ({ page }) => {
    await page.goto(PAGE);
    await expect(page.locator('#rememberKey')).not.toBeChecked();

    await typeKey(page, KEY);

    const stored = await storageDump(page);
    expect(stored.local).not.toHaveProperty('lq.key');
    expect(stored.local).not.toHaveProperty('lq.key.remembered');
    expect(stored.session['lq.key']).toBe(KEY);

    // Survives a reload in the same tab (sessionStorage), but is not in localStorage.
    await page.reload();
    await expect(page.locator('#key')).toHaveValue(KEY);
  });

  test('"Remember on this device" persists the key to localStorage', async ({ page }) => {
    await page.goto(PAGE);
    await page.locator('#rememberKey').check();
    await typeKey(page, KEY);

    const stored = await storageDump(page);
    expect(stored.local['lq.key.remember']).toBe('1');
    expect(stored.local['lq.key.remembered']).toBe(KEY);

    await page.reload();
    await expect(page.locator('#rememberKey')).toBeChecked();
    await expect(page.locator('#key')).toHaveValue(KEY);
  });

  test('unticking "Remember" removes the persisted copy', async ({ page }) => {
    await page.goto(PAGE);
    await page.locator('#rememberKey').check();
    await typeKey(page, KEY);
    await page.locator('#rememberKey').uncheck();

    const stored = await storageDump(page);
    expect(stored.local).not.toHaveProperty('lq.key.remembered');
    expect(stored.local).not.toHaveProperty('lq.key.remember');
  });

  test('"Forget key" clears the key from the page and all storage', async ({ page }) => {
    await page.goto(PAGE);
    await page.locator('#rememberKey').check();
    await typeKey(page, KEY);

    await page.locator('#forgetKey').click();

    await expect(page.locator('#key')).toHaveValue('');
    await expect(page.locator('#rememberKey')).not.toBeChecked();
    const stored = await storageDump(page);
    expect(stored.local).not.toHaveProperty('lq.key.remembered');
    expect(stored.local).not.toHaveProperty('lq.key.remember');
    expect(stored.session).not.toHaveProperty('lq.key');
  });

  test('a key written by an older build is removed from localStorage', async ({ page }) => {
    await page.goto(PAGE);
    await page.evaluate((k) => localStorage.setItem('lq.key', k), KEY);

    await page.reload();

    await expect(page.locator('#key')).toHaveValue('');
    expect((await storageDump(page)).local).not.toHaveProperty('lq.key');
  });
});
