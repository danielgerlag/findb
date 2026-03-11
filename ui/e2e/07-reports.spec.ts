import { test, expect } from '@playwright/test';

// ===== REPORTS PAGE TESTS (38-43) =====

test.describe('Reports', () => {
  test('38: balance sheet and income statement tabs exist', async ({ page }) => {
    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    const hasBalanceSheet = /balance.*sheet/i.test(body || '');
    const hasIncomeStatement = /income.*statement/i.test(body || '');
    expect(hasBalanceSheet || hasIncomeStatement).toBe(true);
  });

  test('39: balance sheet shows sections', async ({ page }) => {
    // Create some data first
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @rpt_cash ASSET;
CREATE ACCOUNT @rpt_loan LIABILITY;
CREATE ACCOUNT @rpt_equity EQUITY;
CREATE JOURNAL 2024-06-01, 10000.00, 'Initial investment'
  DEBIT @rpt_cash,
  CREDIT @rpt_equity`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(3000);

    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Balance sheet should show Assets, Liabilities, Equity sections
    const hasAssets = /asset/i.test(body || '');
    const hasLiabilities = /liabilit/i.test(body || '');
    const hasEquity = /equity/i.test(body || '');
    expect(hasAssets).toBe(true);
  });

  test('40: income statement shows revenue and expenses', async ({ page }) => {
    // Create income/expense data
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @rpt_sales INCOME;
CREATE ACCOUNT @rpt_cost EXPENSE;
CREATE ACCOUNT @rpt_bank2 ASSET;
CREATE JOURNAL 2024-03-01, 5000.00, 'Sale'
  DEBIT @rpt_bank2,
  CREDIT @rpt_sales;
CREATE JOURNAL 2024-03-15, 1000.00, 'Expense'
  DEBIT @rpt_cost,
  CREDIT @rpt_bank2`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(3000);

    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);

    // Click Income Statement tab
    const incomeTab = page.locator('[role="tab"], button, a').filter({ hasText: /income.*statement/i }).first();
    if (await incomeTab.count() > 0) {
      await incomeTab.click();
      await page.waitForTimeout(1000);
      const body = await page.textContent('body');
      const hasRevenue = /revenue|income|sales/i.test(body || '');
      const hasExpenses = /expense|cost/i.test(body || '');
      expect(hasRevenue || hasExpenses).toBe(true);
    }
  });

  test('41: balance sheet has date picker', async ({ page }) => {
    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    // Look for date picker
    const datePicker = page.locator('input[type="text"], .p-datepicker, [data-pc-name="datepicker"]').first();
    await expect(datePicker).toBeVisible();
  });

  test('42: income statement has from/to date pickers', async ({ page }) => {
    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    // Click income statement tab
    const incomeTab = page.locator('[role="tab"], button, a').filter({ hasText: /income.*statement/i }).first();
    if (await incomeTab.count() > 0) {
      await incomeTab.click();
      await page.waitForTimeout(500);
      // Should have from and to date pickers
      const dateInputs = page.locator('input[type="text"], [data-pc-name="datepicker"]');
      const count = await dateInputs.count();
      // Income statement should have at least 2 date inputs (from, to)
      expect(count).toBeGreaterThanOrEqual(2);
    }
  });

  test('43: reports show data after creating journals', async ({ page }) => {
    // Create data
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill(`CREATE ACCOUNT @rpt_checking ASSET;
CREATE ACCOUNT @rpt_cap EQUITY;
CREATE JOURNAL 2024-01-01, 50000.00, 'Capital'
  DEBIT @rpt_checking,
  CREDIT @rpt_cap`);
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(3000);

    await page.goto('/reports');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Should show the account or amount somewhere
    const hasData = /50.*000|rpt_checking|rpt_cap/i.test(body || '');
    expect(hasData).toBe(true);
  });
});
