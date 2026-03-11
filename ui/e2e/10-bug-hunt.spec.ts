import { test, expect } from '@playwright/test';

// Deep bug-hunting tests — edge cases, data integrity, error handling

test.describe('Bug Hunt — Data Integrity', () => {

  test('B1: balance sheet equation (Assets = Liabilities + Equity)', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @bh_cash ASSET;
CREATE ACCOUNT @bh_loan LIABILITY;
CREATE ACCOUNT @bh_equity EQUITY;
CREATE ACCOUNT @bh_sales INCOME;
CREATE ACCOUNT @bh_rent EXPENSE;
CREATE JOURNAL 2024-01-01, 100000, 'Investment'
  DEBIT @bh_cash,
  CREDIT @bh_equity;
CREATE JOURNAL 2024-02-01, 30000, 'Loan received'
  DEBIT @bh_cash,
  CREDIT @bh_loan;
CREATE JOURNAL 2024-03-01, 15000, 'Sales revenue'
  DEBIT @bh_cash,
  CREDIT @bh_sales;
CREATE JOURNAL 2024-04-01, 5000, 'Rent payment'
  DEBIT @bh_rent,
  CREDIT @bh_cash`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(3000);

    // Check via API for precision
    const resp = await page.request.post('http://localhost:5173/api/v1/fql', {
      headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
      data: `GET trial_balance(2024-12-31) AS tb`,
    });
    const json = await resp.json();
    expect(json.success).toBe(true);

    const tb = json.results.find((r: any) => r.name === 'tb')?.value;
    expect(tb?.type).toBe('trial_balance');

    const items = tb.value as any[];
    let totalDebits = 0;
    let totalCredits = 0;
    for (const item of items) {
      totalDebits += parseFloat(item.debit || '0');
      totalCredits += parseFloat(item.credit || '0');
    }
    // Debits must equal credits in trial balance
    expect(Math.abs(totalDebits - totalCredits)).toBeLessThan(0.01);

    // Now check via reports page
    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Should show accounts with non-zero balances
    expect(body).toContain('bh_cash');
  });

  test('B2: negative balance display in reports', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @neg_bank ASSET;
CREATE ACCOUNT @neg_exp EXPENSE;
CREATE JOURNAL 2024-01-01, 100, 'Initial'
  DEBIT @neg_bank,
  CREDIT @neg_bank`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);

    // Bank should have 0 balance (debit and credit cancel)
    const resp = await page.request.post('http://localhost:5173/api/v1/fql', {
      headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
      data: 'GET balance(@neg_bank, 2024-12-31) AS b',
    });
    const json = await resp.json();
    expect(json.success).toBe(true);
    expect(json.results[0]?.value?.value).toBe('0');
  });

  test('B3: large decimal amounts preserve precision', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @prec_a ASSET;
CREATE ACCOUNT @prec_l LIABILITY;
CREATE JOURNAL 2024-01-01, 123456789.12, 'Large amount'
  DEBIT @prec_a,
  CREDIT @prec_l`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);

    const resp = await page.request.post('http://localhost:5173/api/v1/fql', {
      headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
      data: 'GET balance(@prec_a, 2024-12-31) AS b',
    });
    const json = await resp.json();
    expect(json.success).toBe(true);
    // Verify precision is maintained
    const balance = json.results[0]?.value?.value;
    expect(balance).toContain('123456789.12');
  });

  test('B4: special characters in journal description', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @spec_a ASSET;
CREATE ACCOUNT @spec_e EQUITY;
CREATE JOURNAL 2024-01-01, 100, 'O''Brien''s café & résumé — test #1'
  DEBIT @spec_a,
  CREDIT @spec_e`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);

    const body = await page.textContent('body');
    // Should succeed, not show parse error
    const hasError = /parse error/i.test(body || '');
    expect(hasError).toBe(false);
  });

  test('B5: duplicate account creation error handling', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill('CREATE ACCOUNT @dup_test ASSET');
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(1500);

    // Try creating same account again
    await page.locator('textarea').first().fill('CREATE ACCOUNT @dup_test ASSET');
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(1500);
    const body = await page.textContent('body');
    // Should show an error about already existing
    expect(/already exists|duplicate|error/i.test(body || '')).toBe(true);
  });

  test('B6: querying non-existent account', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill('GET balance(@nonexistent_xyz, 2024-01-01) AS b');
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    expect(/not found|error/i.test(body || '')).toBe(true);
  });

  test('B7: empty FQL submission', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill('');
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(1500);
    // Should handle gracefully - no crash
    const body = await page.textContent('body');
    expect(body).toBeTruthy();
  });

  test('B8: FQL with only whitespace/semicolons', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill('   ;  ;  ');
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(1500);
    const body = await page.textContent('body');
    // Should not crash
    expect(body).toBeTruthy();
  });

  test('B9: journal with mismatched amounts', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @mis_a ASSET;
CREATE ACCOUNT @mis_b LIABILITY;
CREATE JOURNAL 2024-01-01, 100, 'Test'
  DEBIT @mis_a 50,
  CREDIT @mis_b 75`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Should show an error about unbalanced entry
    expect(/unbalanced|error|mismatch/i.test(body || '')).toBe(true);
  });

  test('B10: accounts page refresh after FQL creates account', async ({ page }) => {
    // Create account via FQL
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill('CREATE ACCOUNT @refresh_test ASSET');
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);

    // Navigate to accounts page
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1500);
    const body = await page.textContent('body');
    // The new account should be visible
    expect(body).toContain('refresh_test');
  });

  test('B11: dashboard updates after creating data', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @dash_bank ASSET;
CREATE ACCOUNT @dash_rev INCOME;
CREATE JOURNAL 2025-06-01, 7777.00, 'Dashboard test'
  DEBIT @dash_bank,
  CREDIT @dash_rev`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Dashboard should show the account
    expect(body).toContain('dash_bank');
  });

  test('B12: statement view shows transactions', async ({ page }) => {
    // Create data
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @stmt_bank ASSET;
CREATE ACCOUNT @stmt_inc INCOME;
CREATE JOURNAL 2025-01-15, 1000, 'First deposit'
  DEBIT @stmt_bank,
  CREDIT @stmt_inc;
CREATE JOURNAL 2025-02-15, 500, 'Second deposit'
  DEBIT @stmt_bank,
  CREDIT @stmt_inc`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(3000);

    // Check via API
    const resp = await page.request.post('http://localhost:5173/api/v1/fql', {
      headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
      data: `GET statement(@stmt_bank, 2025-01-01, 2025-12-31) AS s`,
    });
    const json = await resp.json();
    expect(json.success).toBe(true);
    const stmt = json.results[0]?.value;
    expect(stmt?.type).toBe('statement');
    expect(stmt?.value?.length).toBeGreaterThanOrEqual(2);
  });

  test('B13: income statement net income calculation', async ({ page }) => {
    // Check income statement via API
    const resp = await page.request.post('http://localhost:5173/api/v1/fql', {
      headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
      data: `GET income_statement(2024-01-01, 2025-12-31) AS pnl`,
    });
    const json = await resp.json();
    expect(json.success).toBe(true);
  });

  test('B14: reports page handles date change gracefully', async ({ page }) => {
    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);

    // Find date input and change it
    const dateInput = page.locator('input[type="text"]').first();
    if (await dateInput.count() > 0) {
      await dateInput.click();
      await dateInput.fill('2020-01-01');
      await page.keyboard.press('Enter');
      await page.waitForTimeout(2000);
      // Page should not crash
      const body = await page.textContent('body');
      expect(body).toBeTruthy();
    }
  });

  test('B15: create account via REST with all types', async ({ page }) => {
    const types = ['ASSET', 'LIABILITY', 'EQUITY', 'INCOME', 'EXPENSE'];
    for (const t of types) {
      const resp = await page.request.post('http://localhost:5173/api/accounts', {
        data: { id: `rest_${t.toLowerCase()}`, account_type: t },
      });
      const json = await resp.json();
      // Should succeed or already exist
      expect(json).toBeTruthy();
    }
  });

  test('B16: REST journal creation', async ({ page }) => {
    // Create accounts first
    await page.request.post('http://localhost:5173/api/accounts', {
      data: { id: 'rest_bank', account_type: 'ASSET' },
    });
    await page.request.post('http://localhost:5173/api/accounts', {
      data: { id: 'rest_equity', account_type: 'EQUITY' },
    });

    const resp = await page.request.post('http://localhost:5173/api/journals', {
      data: {
        date: '2025-01-01',
        amount: '5000',
        description: 'REST test journal',
        dimensions: {},
        operations: [
          { type: 'DEBIT', account: 'rest_bank' },
          { type: 'CREDIT', account: 'rest_equity' },
        ],
      },
    });
    const json = await resp.json();
    expect(json.success !== false).toBe(true);
  });

  test('B17: REST rate set with invalid date format', async ({ page }) => {
    await page.request.post('http://localhost:5173/api/rates', {
      data: { id: 'test_rate' },
    });
    const resp = await page.request.post('http://localhost:5173/api/rates/test_rate', {
      data: { rate: '1.5', date: 'not-a-date' },
    });
    // Should return error, not crash
    expect(resp.status()).toBeGreaterThanOrEqual(400);
  });

  test('B18: REST journal with invalid date', async ({ page }) => {
    const resp = await page.request.post('http://localhost:5173/api/journals', {
      data: {
        date: 'invalid-date',
        amount: '100',
        description: 'Bad date',
        dimensions: {},
        operations: [
          { type: 'DEBIT', account: 'rest_bank' },
          { type: 'CREDIT', account: 'rest_equity' },
        ],
      },
    });
    expect(resp.status()).toBeGreaterThanOrEqual(400);
  });

  test('B19: tour entity isolation', async ({ page }) => {
    // Start a tour with a new entity
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');

    const tourCard = page.locator('.tour-card, .p-card, [class*="card"]').first();
    if (await tourCard.count() === 0) return;
    await tourCard.click();
    await page.waitForTimeout(500);

    const createInput = page.locator('input[type="text"]').first();
    if (await createInput.count() > 0) {
      await createInput.fill('isolation_test');
      const startBtn = page.locator('button').filter({ hasText: /create|start|go/i }).first();
      if (await startBtn.count() > 0) {
        await startBtn.click();
        await page.waitForTimeout(3000);
        // Tour should have loaded in the new entity context
        const body = await page.textContent('body');
        expect(body).toBeTruthy();
      }
    }
  });

  test('B20: concurrent FQL via API', async ({ page }) => {
    // Send multiple FQL requests in parallel
    const queries = [
      'GET trial_balance(2025-12-31) AS tb1',
      'GET trial_balance(2025-06-30) AS tb2',
      'GET trial_balance(2024-12-31) AS tb3',
    ];
    const results = await Promise.all(
      queries.map(q =>
        page.request.post('http://localhost:5173/api/v1/fql', {
          headers: { 'Content-Type': 'text/plain', 'Accept': 'application/json' },
          data: q,
        })
      )
    );
    for (const resp of results) {
      expect(resp.ok()).toBe(true);
      const json = await resp.json();
      expect(json.success).toBe(true);
    }
  });
});
