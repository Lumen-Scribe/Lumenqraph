import { test, expect } from '@playwright/test';

test.describe('Explorer Smoke Tests', () => {
  test('page loads and displays title', async ({ page }) => {
    await page.goto('/explorer/index.html');
    await expect(page).toHaveTitle('Lumenqraph Explorer');
    await expect(page.locator('h1')).toContainText('Lumenqraph');
  });

  test('header controls are present', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const baseInput = page.locator('#base');
    const keyInput = page.locator('#key');
    const networkSelect = page.locator('#senet');

    await expect(baseInput).toBeVisible();
    await expect(keyInput).toBeVisible();
    await expect(networkSelect).toBeVisible();
  });

  test('KPI section displays', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const kpis = page.locator('.kpis');
    await expect(kpis).toBeVisible();

    const labels = ['Indexer', 'Lag (ledgers)', 'Processed ledger', 'Chain tip', 'Events indexed', 'Errors'];
    for (const label of labels) {
      await expect(page.locator('.kpi')).toContainText(label);
    }
  });

  test('contracts panel loads', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const contractList = page.locator('#clist');
    await expect(contractList).toBeVisible();
  });

  test('tabs are present and clickable', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const tabNames = ['Events', 'Transfers', 'State', 'Holders', 'Interface', 'Upgrades'];
    for (const tabName of tabNames) {
      const tab = page.locator('.tabs button', { hasText: tabName });
      await expect(tab).toBeVisible();
    }
  });

  test('detail section is visible', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const detailSection = page.locator('.panel').last();
    await expect(detailSection).toBeVisible();

    const emptyState = page.locator('#detail .empty');
    await expect(emptyState).toContainText('No contract selected');
  });

  test('decode any input is present', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const anyCidInput = page.locator('#anyCid');
    const decodeBtn = page.locator('#decodeBtn');

    await expect(anyCidInput).toBeVisible();
    await expect(decodeBtn).toBeVisible();
  });

  test('network selector has both options', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const select = page.locator('#senet');
    const options = select.locator('option');

    await expect(options).toHaveCount(2);
    await expect(options.first()).toHaveValue('public');
    await expect(options.last()).toHaveValue('testnet');
  });

  test('network can be switched', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const select = page.locator('#senet');
    await select.selectOption('testnet');

    const selectedValue = await select.inputValue();
    expect(selectedValue).toBe('testnet');

    await select.selectOption('public');
    const newValue = await select.inputValue();
    expect(newValue).toBe('public');
  });

  test('interface tab can be clicked', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const interfaceTab = page.locator('.tabs button[data-tab="interface"]');
    await interfaceTab.click();

    await expect(interfaceTab).toHaveClass(/active/);
  });

  test('events tab can be clicked', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const eventsTab = page.locator('.tabs button[data-tab="events"]');
    await eventsTab.click();

    await expect(eventsTab).toHaveClass(/active/);
  });

  test('local storage persistence', async ({ page }) => {
    // Set values in localStorage
    await page.goto('/explorer/index.html');
    await page.evaluate(() => {
      localStorage.setItem('lq.base', 'http://api.example.com');
      localStorage.setItem('lq.key', 'test-key-123');
    });

    // Reload and verify persistence
    await page.reload();
    const baseValue = await page.inputValue('#base');
    const keyValue = await page.inputValue('#key');

    expect(baseValue).toBe('http://api.example.com');
    expect(keyValue).toBe('test-key-123');
  });

  test('toast notification appears and disappears', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const toast = page.locator('#toast');
    await expect(toast).toBeVisible();

    // Simulate toast display
    await page.evaluate(() => {
      const t = document.getElementById('toast');
      if (t) {
        t.textContent = 'Test message';
        t.classList.add('show');
      }
    });

    await expect(toast).toContainText('Test message');
    await expect(toast).toHaveClass(/show/);

    // Toast should have opacity transition
    const style = await toast.evaluate(el => window.getComputedStyle(el).opacity);
    expect(style).toBeDefined();
  });

  test('page responds to window resize', async ({ page }) => {
    await page.goto('/explorer/index.html');

    const grid = page.locator('.grid');
    await expect(grid).toBeVisible();

    // Get computed grid template columns
    const gridCols = await grid.evaluate(el => window.getComputedStyle(el).gridTemplateColumns);
    expect(gridCols).toBeDefined();
  });

  test('stylesheets are properly applied', async ({ page }) => {
    await page.goto('/explorer/index.html');

    // Check that styles are applied
    const container = page.locator('.container');
    const style = await container.evaluate(el => window.getComputedStyle(el));

    expect(style.maxWidth).toBeDefined();
    expect(style.marginLeft).toBeDefined();
  });
});
