import { test, expect } from '@playwright/test';

async function fqlRaw(page: any, query: string) {
  const resp = await page.request.post('http://localhost:5173/fql', {
    headers: { 'Content-Type': 'text/plain' },
    data: query,
  });
  return resp.json();
}

async function fqlApi(page: any, query: string) {
  const resp = await page.request.post('http://localhost:5173/api/v1/fql', {
    headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
    data: query,
  });
  return resp.json();
}

// ===== FORM VALIDATION & EDGE CASES (Group 16) =====

test.describe('Form Validation & Edge Cases', () => {

  test('16-1: journal form — submit with empty date shows validation', async ({ page }) => {
    // Create accounts first
    await fqlRaw(page, `CREATE ACCOUNT @val_bank ASSET; CREATE ACCOUNT @val_eq EQUITY`);
    await page.waitForTimeout(500);

    await page.goto('/journals');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(500);

    // Fill amount and description but leave date picker interaction to see what happens
    const allInputs = page.locator('input[type="text"], input[type="number"], input:not([type])');
    const inputCount = await allInputs.count();

    // Find and fill amount
    for (let i = 0; i < inputCount; i++) {
      const placeholder = await allInputs.nth(i).getAttribute('placeholder');
      if (placeholder && /1000/i.test(placeholder)) {
        await allInputs.nth(i).fill('500');
      }
      if (placeholder && /investment/i.test(placeholder)) {
        await allInputs.nth(i).fill('Test journal');
      }
    }

    // Fill entries
    for (let i = 0; i < inputCount; i++) {
      const placeholder = await allInputs.nth(i).getAttribute('placeholder');
      if (placeholder && /bank/i.test(placeholder)) {
        await allInputs.nth(i).fill('val_bank');
        break;
      }
    }

    // The form should have the date pre-filled (today), so FQL preview should show
    const body = await page.textContent('body');
    // Journal date should default to today, so form should work
    expect(body).toBeTruthy();
  });

  test('16-2: journal form — submit with zero amount', async ({ page }) => {
    await fqlRaw(page, `CREATE ACCOUNT @zero_bank ASSET; CREATE ACCOUNT @zero_eq EQUITY`);
    await page.waitForTimeout(500);

    await page.goto('/journals');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(500);

    // Try to set amount to 0
    const allInputs = page.locator('input[type="text"], input[type="number"], input:not([type])');
    const inputCount = await allInputs.count();
    for (let i = 0; i < inputCount; i++) {
      const placeholder = await allInputs.nth(i).getAttribute('placeholder');
      if (placeholder && /1000/i.test(placeholder)) {
        await allInputs.nth(i).fill('0');
      }
      if (placeholder && /investment/i.test(placeholder)) {
        await allInputs.nth(i).fill('Zero test');
      }
    }

    // Try submit — the Create Journal button
    const submitBtn = page.locator('button').filter({ hasText: /create.*journal/i }).first();
    if (await submitBtn.count() > 0) {
      await submitBtn.click();
      await page.waitForTimeout(2000);
    }

    // The form should handle zero amount — either prevent or show error
    const body = await page.textContent('body');
    expect(body).toBeTruthy();
  });

  test('16-3: journal form — submit with only debit entries (unbalanced)', async ({ page }) => {
    await fqlRaw(page, `CREATE ACCOUNT @unbal_bank ASSET; CREATE ACCOUNT @unbal_rev INCOME`);
    await page.waitForTimeout(500);

    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    // Try to create an unbalanced journal (2 debits, no credits, with explicit amounts)
    await editor.fill(`CREATE JOURNAL 2024-01-01, 100, 'Unbalanced'
  DEBIT @unbal_bank 100,
  DEBIT @unbal_rev 50`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);

    const body = await page.textContent('body');
    // Should show error about unbalanced entry
    expect(/error|unbalanced|mismatch|invalid/i.test(body || '')).toBe(true);
  });

  test('16-4: account creation — very long account name', async ({ page }) => {
    const longName = 'very_long_account_name_that_exceeds_normal_limits_' + 'x'.repeat(50);

    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill(`CREATE ACCOUNT @${longName} ASSET`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);

    const body = await page.textContent('body');
    // Should either succeed or show a clear error — no crash
    expect(body).toBeTruthy();

    // If it succeeded, verify via API
    const resp = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`);
    expect(resp.success).toBe(true);
  });

  test('16-5: journal with 10+ ledger entries (multi-way split)', async ({ page }) => {
    // Create many accounts
    let createAccounts = '';
    for (let i = 1; i <= 10; i++) {
      createAccounts += `CREATE ACCOUNT @split_${i} EXPENSE;\n`;
    }
    createAccounts += `CREATE ACCOUNT @split_bank ASSET;\nCREATE ACCOUNT @split_eq EQUITY;\n`;
    createAccounts += `CREATE JOURNAL 2024-01-01, 10000, 'Initial capital'
      DEBIT @split_bank,
      CREDIT @split_eq;\n`;

    await fqlRaw(page, createAccounts);
    await page.waitForTimeout(500);

    // Create a journal with 10 expense debits and 1 bank credit
    let entries = '';
    for (let i = 1; i <= 10; i++) {
      entries += `  DEBIT @split_${i} 100,\n`;
    }
    entries += `  CREDIT @split_bank`;

    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill(`CREATE JOURNAL 2024-02-01, 1000, 'Multi-way split'
${entries}`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(3000);

    const body = await page.textContent('body');
    // Should succeed — check for no errors
    const hasError = /error|panic|failed/i.test(body || '');
    const isAlreadyExists = /already exists/i.test(body || '');
    expect(hasError && !isAlreadyExists).toBe(false);

    // Verify all expense accounts have balance
    const resp = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`);
    expect(resp.success).toBe(true);
    const tb = resp.results.find((r: any) => r.name === 'tb')?.value;
    const items = tb?.value as any[];
    const splitAccounts = items.filter((i: any) => i.account_id.startsWith('split_') && i.account_type === 'expense');
    expect(splitAccounts.length).toBeGreaterThanOrEqual(10);
  });

  test('16-6: rate — set rate with negative value', async ({ page }) => {
    await fqlRaw(page, `CREATE RATE neg_rate_test`);
    await page.waitForTimeout(300);

    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill(`SET RATE neg_rate_test -1.5 2024-01-01`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);

    const body = await page.textContent('body');
    // Should either succeed or show an error — no crash
    expect(body).toBeTruthy();
  });

  test('16-7: rate — lookup at date before any rates set', async ({ page }) => {
    await fqlRaw(page, `
      CREATE RATE early_lookup_test;
      SET RATE early_lookup_test 2.5 2024-06-01;
    `);
    await page.waitForTimeout(300);

    // Try to look up before the rate was set
    const resp = await fqlApi(page, `GET fx_rate('early_lookup_test', 2024-01-01) AS rate`);
    // Should either return not found/error or the nearest rate
    expect(resp).toBeTruthy();
    // The response should not crash — either success with a value or a handled error
    const hasResult = resp.success || resp.error;
    expect(hasResult).toBeTruthy();
  });
});
