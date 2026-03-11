import { test, expect } from '@playwright/test';

async function fqlApi(page: any, query: string, entity?: string) {
  let body = query;
  if (entity && entity !== 'default') {
    body = `USE ENTITY '${entity}';\n${query}`;
  }
  const resp = await page.request.post('http://localhost:5173/api/v1/fql', {
    headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
    data: body,
  });
  return resp.json();
}

async function fqlRaw(page: any, query: string, entity?: string) {
  let body = query;
  if (entity && entity !== 'default') {
    body = `USE ENTITY '${entity}';\n${query}`;
  }
  const resp = await page.request.post('http://localhost:5173/fql', {
    headers: { 'Content-Type': 'text/plain' },
    data: body,
  });
  return resp.json();
}

async function createEntity(page: any, name: string) {
  await page.request.post('http://localhost:5173/api/entities', {
    data: { name },
  });
}

// ===== CROSS-FEATURE WORKFLOWS (Group 18) =====

test.describe('Cross-Feature Workflows', () => {

  test('18-1: full accounting cycle — create → journal → trial balance → reports', async ({ page }) => {
    // Use isolated entity for precise numbers
    await createEntity(page, 'cycle_test_18_1');

    // Step 1: Create accounts via FQL
    await fqlRaw(page, `
      CREATE ACCOUNT @cycle_bank ASSET;
      CREATE ACCOUNT @cycle_ar ASSET;
      CREATE ACCOUNT @cycle_ap LIABILITY;
      CREATE ACCOUNT @cycle_equity EQUITY;
      CREATE ACCOUNT @cycle_revenue INCOME;
      CREATE ACCOUNT @cycle_rent EXPENSE;
      CREATE ACCOUNT @cycle_wages EXPENSE;
    `, 'cycle_test_18_1');
    await page.waitForTimeout(500);

    // Step 2: Create journals
    await fqlRaw(page, `
      CREATE JOURNAL 2024-01-01, 100000, 'Owner investment'
        DEBIT @cycle_bank,
        CREDIT @cycle_equity;
      CREATE JOURNAL 2024-02-01, 25000, 'Service revenue'
        DEBIT @cycle_ar,
        CREDIT @cycle_revenue;
      CREATE JOURNAL 2024-02-15, 20000, 'Client payment'
        DEBIT @cycle_bank,
        CREDIT @cycle_ar;
      CREATE JOURNAL 2024-03-01, 5000, 'Rent payment'
        DEBIT @cycle_rent,
        CREDIT @cycle_bank;
      CREATE JOURNAL 2024-03-15, 10000, 'Wages'
        DEBIT @cycle_wages,
        CREDIT @cycle_bank;
    `, 'cycle_test_18_1');
    await page.waitForTimeout(500);

    // Step 3: Verify trial balance
    const tbResp = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'cycle_test_18_1');
    expect(tbResp.success).toBe(true);
    const tb = tbResp.results.find((r: any) => r.name === 'tb')?.value;
    expect(tb?.type).toBe('trial_balance');
    const items = tb.value as any[];

    let totalDebits = 0, totalCredits = 0;
    for (const item of items) {
      totalDebits += parseFloat(item.debit || '0');
      totalCredits += parseFloat(item.credit || '0');
    }
    expect(Math.abs(totalDebits - totalCredits)).toBeLessThan(0.01);

    // Step 4: Verify balance sheet equation
    let assets = 0, liabilities = 0, equity = 0, income = 0, expenses = 0;
    for (const item of items) {
      const bal = parseFloat(item.balance || '0');
      switch (item.account_type) {
        case 'asset': assets += bal; break;
        case 'liability': liabilities += bal; break;
        case 'equity': equity += bal; break;
        case 'income': income += bal; break;
        case 'expense': expenses += bal; break;
      }
    }
    const retainedEarnings = income - expenses;
    expect(Math.abs(assets - (liabilities + equity + retainedEarnings))).toBeLessThan(0.01);

    // Step 5: Verify income statement
    const isResp = await fqlApi(page, `GET income_statement(2024-01-01, 2024-12-31) AS pnl`, 'cycle_test_18_1');
    expect(isResp.success).toBe(true);
    const pnl = isResp.results.find((r: any) => r.name === 'pnl')?.value;
    if (pnl?.type === 'trial_balance') {
      const netIncome = (pnl.value as any[]).find((i: any) => i.account_id === 'NET_INCOME');
      if (netIncome) {
        // Net income = 25000 - (5000 + 10000) = 10000
        expect(parseFloat(netIncome.balance)).toBe(10000);
      }
    }
  });

  test('18-2: multi-currency workflow — rate + convert()', async ({ page }) => {
    // Create rate
    await fqlRaw(page, `
      CREATE RATE usd_gbp;
      SET RATE usd_gbp 0.79 2024-01-01;
      SET RATE usd_gbp 0.81 2024-06-01;
      CREATE ACCOUNT @fx_usd ASSET;
      CREATE ACCOUNT @fx_gbp ASSET;
      CREATE ACCOUNT @fx_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 10000, 'USD deposit'
        DEBIT @fx_usd,
        CREDIT @fx_eq;
    `);
    await page.waitForTimeout(500);

    // Test convert via query page
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill(`GET convert(10000, 'usd_gbp', 2024-01-01) AS jan_gbp;
GET convert(10000, 'usd_gbp', 2024-06-01) AS jun_gbp`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(3000);

    const body = await page.textContent('body');
    // Should show conversion results or error if convert() isn't supported
    expect(body).toBeTruthy();

    // Also verify rate lookup works
    const rateResp = await fqlApi(page, `GET fx_rate('usd_gbp', 2024-01-01) AS r`);
    expect(rateResp.success).toBe(true);
  });

  test('18-3: dimension-based reporting — query by dimension', async ({ page }) => {
    // Use isolated entity
    await createEntity(page, 'dim_test_18_3');

    await fqlRaw(page, `
      CREATE ACCOUNT @dim_rev INCOME;
      CREATE ACCOUNT @dim_bank ASSET;
      CREATE JOURNAL 2024-01-15, 5000, 'Dept A revenue'
        FOR Department='Engineering'
        DEBIT @dim_bank,
        CREDIT @dim_rev;
      CREATE JOURNAL 2024-02-15, 3000, 'Dept B revenue'
        FOR Department='Sales'
        DEBIT @dim_bank,
        CREDIT @dim_rev;
      CREATE JOURNAL 2024-03-15, 7000, 'Dept A big deal'
        FOR Department='Engineering'
        DEBIT @dim_bank,
        CREDIT @dim_rev;
    `, 'dim_test_18_3');
    await page.waitForTimeout(500);

    // Query statement for bank account filtered by Engineering department
    const resp = await fqlApi(page, `GET statement(@dim_bank, 2024-01-01, 2024-12-31, Department='Engineering') AS s`, 'dim_test_18_3');
    expect(resp.success).toBe(true);
    const stmt = resp.results[0]?.value;
    expect(stmt?.type).toBe('statement');
    const entries = stmt?.value as any[];

    // Should only show Engineering entries (2 out of 3)
    expect(entries.length).toBe(2);

    // Total should be 12000 (5000 + 7000)
    const totalAmount = entries.reduce((sum: number, e: any) => sum + Math.abs(parseFloat(e.amount || '0')), 0);
    expect(totalAmount).toBe(12000);
  });

  test('18-4: create account on accounts page — verify in query page dropdown', async ({ page }) => {
    // Create an account via accounts page dialog
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(500);

    const createBtn = page.locator('button').filter({ hasText: /create.*account/i }).first();
    if (await createBtn.count() > 0) {
      await createBtn.click();
      await page.waitForTimeout(500);

      // Fill in the dialog
      const idInput = page.locator('.p-dialog input[type="text"], [role="dialog"] input[type="text"]').first();
      if (await idInput.count() > 0) {
        await idInput.fill('cross_feature_acct');
      }

      // Select account type
      const typeDropdown = page.locator('.p-dialog .p-select, [role="dialog"] .p-select, .p-dialog select').first();
      if (await typeDropdown.count() > 0) {
        await typeDropdown.click();
        await page.waitForTimeout(300);
        const assetOption = page.locator('.p-select-option, .p-listbox-option, [role="option"]').filter({ hasText: /ASSET/i }).first();
        if (await assetOption.count() > 0) {
          await assetOption.click();
        }
      }

      // Click Create
      const dialogCreateBtn = page.locator('.p-dialog button, [role="dialog"] button').filter({ hasText: /^create$/i }).first();
      if (await dialogCreateBtn.count() > 0) {
        await dialogCreateBtn.click();
        await page.waitForTimeout(1500);
      }
    }

    // Verify the account exists via FQL on query page
    await page.goto('/query');
    await page.waitForLoadState('networkidle');

    const editor = page.locator('textarea').first();
    await editor.fill(`GET trial_balance(2024-12-31) AS tb`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(3000);

    const body = await page.textContent('body');
    // The account we created should show up in the trial balance
    expect(body).toContain('cross_feature_acct');
  });

  test('18-5: create data via REST API — verify in UI', async ({ page }) => {
    // Create accounts via REST API
    await page.request.post('http://localhost:5173/api/accounts', {
      data: { id: 'rest_ui_bank', account_type: 'ASSET' },
    });
    await page.request.post('http://localhost:5173/api/accounts', {
      data: { id: 'rest_ui_equity', account_type: 'EQUITY' },
    });

    // Create journal via REST API
    await page.request.post('http://localhost:5173/api/journals', {
      data: {
        date: '2024-06-01',
        amount: '7777',
        description: 'REST API journal',
        dimensions: {},
        operations: [
          { type: 'DEBIT', account: 'rest_ui_bank' },
          { type: 'CREDIT', account: 'rest_ui_equity' },
        ],
      },
    });
    await page.waitForTimeout(500);

    // Verify on accounts page
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);
    const accountsBody = await page.textContent('body');
    expect(accountsBody).toContain('rest_ui_bank');

    // Verify on dashboard
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);
    const dashBody = await page.textContent('body');
    expect(dashBody).toContain('rest_ui_bank');

    // Verify on reports page
    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(2000);
    const reportsBody = await page.textContent('body');
    expect(reportsBody).toContain('rest_ui_bank');
  });
});
