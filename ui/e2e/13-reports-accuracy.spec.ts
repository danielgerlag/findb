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

// ===== REPORTS DATA ACCURACY (Group 13) =====

test.describe('Reports Data Accuracy', () => {

  test('13-1: balance sheet equation after complex scenario', async ({ page }) => {
    await fqlRaw(page, `
      CREATE ACCOUNT @bs_cash ASSET;
      CREATE ACCOUNT @bs_ar ASSET;
      CREATE ACCOUNT @bs_loan LIABILITY;
      CREATE ACCOUNT @bs_equity EQUITY;
      CREATE ACCOUNT @bs_revenue INCOME;
      CREATE ACCOUNT @bs_wages EXPENSE;
      CREATE JOURNAL 2024-01-01, 50000, 'Owner investment'
        DEBIT @bs_cash,
        CREDIT @bs_equity;
      CREATE JOURNAL 2024-02-01, 20000, 'Loan received'
        DEBIT @bs_cash,
        CREDIT @bs_loan;
      CREATE JOURNAL 2024-03-01, 15000, 'Service revenue'
        DEBIT @bs_ar,
        CREDIT @bs_revenue;
      CREATE JOURNAL 2024-04-01, 8000, 'Wages paid'
        DEBIT @bs_wages,
        CREDIT @bs_cash;
      CREATE JOURNAL 2024-05-01, 10000, 'Client payment received'
        DEBIT @bs_cash,
        CREDIT @bs_ar;
    `);
    await page.waitForTimeout(500);

    // Verify on reports page
    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(2000);

    const body = await page.textContent('body');
    expect(body).toContain('bs_cash');

    // Verify equation via API: Assets = Liabilities + Equity + Retained Earnings
    const resp = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`);
    expect(resp.success).toBe(true);
    const tb = resp.results.find((r: any) => r.name === 'tb')?.value;
    const items = tb?.value as any[];

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
    // Assets = Liabilities + Equity + Retained Earnings
    expect(Math.abs(assets - (liabilities + equity + retainedEarnings))).toBeLessThan(0.01);
  });

  test('13-2: income statement — Revenue - Expenses = Net Income', async ({ page }) => {
    // Use isolated entity to get precise numbers
    await createEntity(page, 'is_test_13_2');

    await fqlRaw(page, `
      CREATE ACCOUNT @is_bank ASSET;
      CREATE ACCOUNT @is_sales INCOME;
      CREATE ACCOUNT @is_consulting INCOME;
      CREATE ACCOUNT @is_rent EXPENSE;
      CREATE ACCOUNT @is_utilities EXPENSE;
      CREATE JOURNAL 2024-01-15, 10000, 'Product sales'
        DEBIT @is_bank,
        CREDIT @is_sales;
      CREATE JOURNAL 2024-02-15, 5000, 'Consulting revenue'
        DEBIT @is_bank,
        CREDIT @is_consulting;
      CREATE JOURNAL 2024-03-15, 3000, 'Rent payment'
        DEBIT @is_rent,
        CREDIT @is_bank;
      CREATE JOURNAL 2024-04-15, 500, 'Utilities'
        DEBIT @is_utilities,
        CREDIT @is_bank;
    `, 'is_test_13_2');
    await page.waitForTimeout(500);

    // Verify via API using isolated entity
    const resp = await fqlApi(page, `GET income_statement(2024-01-01, 2024-12-31) AS pnl`, 'is_test_13_2');
    expect(resp.success).toBe(true);
    const pnl = resp.results.find((r: any) => r.name === 'pnl')?.value;
    if (pnl?.type === 'trial_balance') {
      const items = pnl.value as any[];
      const netIncomeItem = items.find((i: any) => i.account_id === 'NET_INCOME');
      if (netIncomeItem) {
        // Net Income = Revenue - Expenses = (10000 + 5000) - (3000 + 500) = 11500
        expect(parseFloat(netIncomeItem.balance)).toBe(11500);
      }
    }
  });

  test('13-3: balance sheet date change updates all values', async ({ page }) => {
    await fqlRaw(page, `
      CREATE ACCOUNT @bsdt_cash ASSET;
      CREATE ACCOUNT @bsdt_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 1000, 'Jan deposit'
        DEBIT @bsdt_cash,
        CREDIT @bsdt_eq;
      CREATE JOURNAL 2024-07-01, 2000, 'Jul deposit'
        DEBIT @bsdt_cash,
        CREDIT @bsdt_eq;
    `);
    await page.waitForTimeout(500);

    // Check balance at June — should be 1000
    const respJun = await fqlApi(page, `GET trial_balance(2024-06-30) AS tb`);
    expect(respJun.success).toBe(true);
    const tbJun = respJun.results.find((r: any) => r.name === 'tb')?.value;
    const cashJun = (tbJun?.value as any[])?.find((a: any) => a.account_id === 'bsdt_cash');
    expect(parseFloat(cashJun?.balance || '0')).toBe(1000);

    // Check balance at Dec — should be 3000
    const respDec = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`);
    expect(respDec.success).toBe(true);
    const tbDec = respDec.results.find((r: any) => r.name === 'tb')?.value;
    const cashDec = (tbDec?.value as any[])?.find((a: any) => a.account_id === 'bsdt_cash');
    expect(parseFloat(cashDec?.balance || '0')).toBe(3000);
  });

  test('13-4: income statement with zero-revenue period', async ({ page }) => {
    // Use isolated entity so we only see our data
    await createEntity(page, 'zr_test_13_4');

    await fqlRaw(page, `
      CREATE ACCOUNT @zr_bank ASSET;
      CREATE ACCOUNT @zr_rev INCOME;
      CREATE JOURNAL 2024-01-15, 1000, 'Only sale'
        DEBIT @zr_bank,
        CREDIT @zr_rev;
    `, 'zr_test_13_4');
    await page.waitForTimeout(500);

    // Query income statement for a period with no activity (e.g., Mar-Jun)
    const resp = await fqlApi(page, `GET income_statement(2024-03-01, 2024-06-30) AS pnl`, 'zr_test_13_4');
    expect(resp.success).toBe(true);
    const pnl = resp.results.find((r: any) => r.name === 'pnl')?.value;

    if (pnl?.type === 'trial_balance') {
      const items = pnl.value as any[];
      // No revenue items for this period, or all have 0 balance
      const totalRevenue = items
        .filter((i: any) => i.account_type === 'income' && i.account_id !== 'NET_INCOME')
        .reduce((sum: number, i: any) => sum + parseFloat(i.balance || '0'), 0);
      expect(totalRevenue).toBe(0);
    }

    // Verify on UI
    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);
    const body = await page.textContent('body');
    expect(body).toBeTruthy();
  });

  test('13-5: balance sheet with hierarchical accounts', async ({ page }) => {
    // Use isolated entity to get precise totals
    await createEntity(page, 'hier_test_13_5');

    await fqlRaw(page, `
      CREATE ACCOUNT @assets_bank ASSET;
      CREATE ACCOUNT @assets_cash ASSET;
      CREATE ACCOUNT @liab_ap LIABILITY;
      CREATE ACCOUNT @equity_cap EQUITY;
      CREATE JOURNAL 2024-01-01, 10000, 'Investment'
        DEBIT @assets_bank,
        CREDIT @equity_cap;
      CREATE JOURNAL 2024-02-01, 5000, 'Cash deposit'
        DEBIT @assets_cash,
        CREDIT @equity_cap;
      CREATE JOURNAL 2024-03-01, 2000, 'AP accrual'
        DEBIT @assets_bank,
        CREDIT @liab_ap;
    `, 'hier_test_13_5');
    await page.waitForTimeout(500);

    // Verify totals using isolated entity
    const resp = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'hier_test_13_5');
    expect(resp.success).toBe(true);
    const tb = resp.results.find((r: any) => r.name === 'tb')?.value;
    const items = tb?.value as any[];

    // Check accounts exist
    const bankAccount = items.find((i: any) => i.account_id === 'assets_bank');
    const cashAccount = items.find((i: any) => i.account_id === 'assets_cash');
    expect(bankAccount).toBeTruthy();
    expect(cashAccount).toBeTruthy();

    const totalAssets = items
      .filter((i: any) => i.account_type === 'asset')
      .reduce((sum: number, i: any) => sum + parseFloat(i.balance || '0'), 0);
    // assets_bank: 12000, assets_cash: 5000 => total 17000
    expect(totalAssets).toBe(17000);
  });

  test('13-6: reports after entity switch show entity-specific data', async ({ page }) => {
    // Create two entities with different data
    await createEntity(page, 'rpt_entity_a');
    await createEntity(page, 'rpt_entity_b');

    await fqlRaw(page, `
      CREATE ACCOUNT @ea_cash ASSET;
      CREATE ACCOUNT @ea_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 5000, 'Entity A capital'
        DEBIT @ea_cash,
        CREDIT @ea_eq;
    `, 'rpt_entity_a');

    await fqlRaw(page, `
      CREATE ACCOUNT @eb_cash ASSET;
      CREATE ACCOUNT @eb_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 9999, 'Entity B capital'
        DEBIT @eb_cash,
        CREDIT @eb_eq;
    `, 'rpt_entity_b');
    await page.waitForTimeout(500);

    // Check entity A via API
    const respA = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'rpt_entity_a');
    expect(respA.success).toBe(true);
    const tbA = respA.results.find((r: any) => r.name === 'tb')?.value;
    const itemsA = tbA?.value as any[];
    const hasEaCash = itemsA?.some((i: any) => i.account_id === 'ea_cash');
    const hasEbCash = itemsA?.some((i: any) => i.account_id === 'eb_cash');
    expect(hasEaCash).toBe(true);
    expect(hasEbCash).toBe(false);

    // Check entity B via API
    const respB = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'rpt_entity_b');
    expect(respB.success).toBe(true);
    const tbB = respB.results.find((r: any) => r.name === 'tb')?.value;
    const itemsB = tbB?.value as any[];
    const hasEbCash2 = itemsB?.some((i: any) => i.account_id === 'eb_cash');
    expect(hasEbCash2).toBe(true);
  });
});
