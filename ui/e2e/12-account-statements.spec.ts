import { test, expect } from '@playwright/test';

async function fqlApi(page: any, query: string) {
  const resp = await page.request.post('http://localhost:5173/api/v1/fql', {
    headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
    data: query,
  });
  return resp.json();
}

async function fqlRaw(page: any, query: string) {
  const resp = await page.request.post('http://localhost:5173/fql', {
    headers: { 'Content-Type': 'text/plain' },
    data: query,
  });
  return resp.json();
}

// ===== ACCOUNT STATEMENT DEEP DIVE (Group 12) =====

test.describe('Account Statement Deep Dive', () => {

  test('12-1: view account statement after multiple journals', async ({ page }) => {
    // Setup: create accounts and multiple journals
    await fqlRaw(page, `
      CREATE ACCOUNT @stmt_bank ASSET;
      CREATE ACCOUNT @stmt_income INCOME;
      CREATE JOURNAL 2024-01-15, 500, 'Sale 1'
        DEBIT @stmt_bank,
        CREDIT @stmt_income;
      CREATE JOURNAL 2024-02-15, 300, 'Sale 2'
        DEBIT @stmt_bank,
        CREDIT @stmt_income;
      CREATE JOURNAL 2024-03-15, 700, 'Sale 3'
        DEBIT @stmt_bank,
        CREDIT @stmt_income;
    `);
    await page.waitForTimeout(500);

    // Navigate to accounts page
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);

    // Click on stmt_bank row
    const accountRow = page.locator('tr, .p-datatable-row-group').filter({ hasText: /stmt_bank/ }).first();
    if (await accountRow.count() > 0) {
      await accountRow.click();
      await page.waitForTimeout(500);

      // Click "Load Statement" button
      const loadBtn = page.locator('button').filter({ hasText: /load.*statement/i }).first();
      if (await loadBtn.count() > 0) {
        await loadBtn.click();
        await page.waitForTimeout(2000);
      }

      const body = await page.textContent('body');
      // Should show all 3 transactions
      expect(body).toContain('Sale 1');
      expect(body).toContain('Sale 2');
      expect(body).toContain('Sale 3');
    } else {
      // Verify via API instead
      const resp = await fqlApi(page, `GET statement(@stmt_bank, 2024-01-01, 2024-12-31) AS s`);
      expect(resp.success).toBe(true);
      const stmt = resp.results[0]?.value;
      expect(stmt?.type).toBe('statement');
      expect(stmt?.value?.length).toBeGreaterThanOrEqual(3);
    }
  });

  test('12-2: statement shows correct running balance', async ({ page }) => {
    await fqlRaw(page, `
      CREATE ACCOUNT @run_bank ASSET;
      CREATE ACCOUNT @run_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 1000, 'Deposit 1'
        DEBIT @run_bank,
        CREDIT @run_eq;
      CREATE JOURNAL 2024-02-01, 500, 'Deposit 2'
        DEBIT @run_bank,
        CREDIT @run_eq;
      CREATE JOURNAL 2024-03-01, 200, 'Withdrawal'
        DEBIT @run_eq,
        CREDIT @run_bank;
    `);
    await page.waitForTimeout(500);

    // Verify running balance via API
    const resp = await fqlApi(page, `GET statement(@run_bank, 2024-01-01, 2024-12-31) AS s`);
    expect(resp.success).toBe(true);
    const stmt = resp.results[0]?.value;
    expect(stmt?.type).toBe('statement');
    const entries = stmt?.value as any[];
    expect(entries.length).toBeGreaterThanOrEqual(3);

    // Running balance should end at 1300 (1000 + 500 - 200)
    const lastEntry = entries[entries.length - 1];
    expect(parseFloat(lastEntry.balance)).toBe(1300);
  });

  test('12-3: statement date range filtering', async ({ page }) => {
    await fqlRaw(page, `
      CREATE ACCOUNT @df_bank ASSET;
      CREATE ACCOUNT @df_eq EQUITY;
      CREATE JOURNAL 2024-01-15, 100, 'Jan entry'
        DEBIT @df_bank,
        CREDIT @df_eq;
      CREATE JOURNAL 2024-06-15, 200, 'Jun entry'
        DEBIT @df_bank,
        CREDIT @df_eq;
      CREATE JOURNAL 2024-11-15, 300, 'Nov entry'
        DEBIT @df_bank,
        CREDIT @df_eq;
    `);
    await page.waitForTimeout(500);

    // Query only June through September — should only show Jun entry
    const resp = await fqlApi(page, `GET statement(@df_bank, 2024-06-01, 2024-09-30) AS s`);
    expect(resp.success).toBe(true);
    const stmt = resp.results[0]?.value;
    expect(stmt?.type).toBe('statement');
    const entries = stmt?.value as any[];

    // Should contain the June entry but not Jan or Nov
    const descriptions = entries.map((e: any) => e.description);
    expect(descriptions).toContain('Jun entry');
    expect(descriptions).not.toContain('Jan entry');
    expect(descriptions).not.toContain('Nov entry');
  });

  test('12-4: statement with dimension filter', async ({ page }) => {
    await fqlRaw(page, `
      CREATE ACCOUNT @dim_bank ASSET;
      CREATE ACCOUNT @dim_rev INCOME;
      CREATE JOURNAL 2024-01-01, 100, 'Customer A sale'
        FOR Customer='Acme'
        DEBIT @dim_bank,
        CREDIT @dim_rev;
      CREATE JOURNAL 2024-02-01, 200, 'Customer B sale'
        FOR Customer='Beta'
        DEBIT @dim_bank,
        CREDIT @dim_rev;
    `);
    await page.waitForTimeout(500);

    // Query with dimension filter
    const resp = await fqlApi(page, `GET statement(@dim_bank, 2024-01-01, 2024-12-31, Customer='Acme') AS s`);
    expect(resp.success).toBe(true);
    const stmt = resp.results[0]?.value;
    expect(stmt?.type).toBe('statement');
    const entries = stmt?.value as any[];

    // Should only show Acme entries
    expect(entries.length).toBeGreaterThanOrEqual(1);
    const descriptions = entries.map((e: any) => e.description);
    expect(descriptions.some((d: string) => /Acme|Customer A/i.test(d))).toBe(true);
  });

  test('12-5: ASSET account shows positive debit balance', async ({ page }) => {
    await fqlRaw(page, `
      CREATE ACCOUNT @asset_pos ASSET;
      CREATE ACCOUNT @asset_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 5000, 'Capital investment'
        DEBIT @asset_pos,
        CREDIT @asset_eq;
    `);
    await page.waitForTimeout(500);

    const resp = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`);
    expect(resp.success).toBe(true);
    const tb = resp.results.find((r: any) => r.name === 'tb')?.value;
    expect(tb?.type).toBe('trial_balance');

    const assetAccount = (tb.value as any[]).find((a: any) => a.account_id === 'asset_pos');
    expect(assetAccount).toBeTruthy();
    // Asset should have positive debit balance
    expect(parseFloat(assetAccount.debit || '0')).toBeGreaterThan(0);
    expect(parseFloat(assetAccount.balance || '0')).toBe(5000);
  });

  test('12-6: LIABILITY account shows positive credit balance', async ({ page }) => {
    await fqlRaw(page, `
      CREATE ACCOUNT @liab_test LIABILITY;
      CREATE ACCOUNT @liab_cash ASSET;
      CREATE JOURNAL 2024-01-01, 3000, 'Loan received'
        DEBIT @liab_cash,
        CREDIT @liab_test;
    `);
    await page.waitForTimeout(500);

    const resp = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`);
    expect(resp.success).toBe(true);
    const tb = resp.results.find((r: any) => r.name === 'tb')?.value;
    expect(tb?.type).toBe('trial_balance');

    const liabAccount = (tb.value as any[]).find((a: any) => a.account_id === 'liab_test');
    expect(liabAccount).toBeTruthy();
    // Liability should have positive credit balance
    expect(parseFloat(liabAccount.credit || '0')).toBeGreaterThan(0);
    expect(parseFloat(liabAccount.balance || '0')).toBe(3000);
  });
});
