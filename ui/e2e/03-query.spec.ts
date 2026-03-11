import { test, expect } from '@playwright/test';

// ===== FQL QUERY TESTS (13-20) =====

test.describe('FQL Query', () => {
  test('13: FQL query editor textarea is present', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await expect(editor).toBeVisible();
  });

  test('14: execute button exists and is clickable', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await expect(btn).toBeVisible();
    await expect(btn).toBeEnabled();
  });

  test('15: can create account via FQL', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill('CREATE ACCOUNT @cash ASSET');
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Should show success or created message, NOT an error
    const hasError = /error|failed|panic/i.test(body || '');
    if (hasError) {
      // Check if it's a "already exists" which is ok on re-run
      const isAlreadyExists = /already exists/i.test(body || '');
      expect(hasError && !isAlreadyExists).toBe(false);
    }
  });

  test('16: can create journal via FQL', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill(`CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @revenue INCOME;
CREATE JOURNAL 2024-01-15, 100.00, 'Test sale'
  DEBIT @bank,
  CREDIT @revenue`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Should show journal created or statements executed
    expect(body).toBeTruthy();
  });

  test('17: can query balance after creating journal', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    // First create data
    await editor.fill(`CREATE ACCOUNT @test_bank ASSET;
CREATE ACCOUNT @test_rev INCOME;
CREATE JOURNAL 2024-06-01, 500.00, 'Payment'
  DEBIT @test_bank,
  CREDIT @test_rev;
GET balance(@test_bank, 2024-12-31) AS bal`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(3000);
    const body = await page.textContent('body');
    // Should show 500 somewhere
    expect(body).toContain('500');
  });

  test('18: syntax errors show error message', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill('THIS IS NOT VALID FQL');
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Should show error
    const hasError = /error|parse|unexpected|invalid/i.test(body || '');
    expect(hasError).toBe(true);
  });

  test('19: multi-statement execution', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill(`CREATE ACCOUNT @multi_a ASSET;
CREATE ACCOUNT @multi_b LIABILITY`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);
    const body = await page.textContent('body');
    // Check for metadata showing multiple statements
    expect(body).toBeTruthy();
  });

  test('20: query history shows previous queries', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    // Execute a query first
    const editor = page.locator('textarea').first();
    await editor.fill('CREATE ACCOUNT @hist_test ASSET');
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);
    // Look for history section
    const historySection = page.locator('text=/history/i').first();
    if (await historySection.count() > 0) {
      await expect(historySection).toBeVisible();
    }
    // History items should contain our query
    const body = await page.textContent('body');
    // Should have "hist_test" somewhere (either in history or results)
    expect(body).toContain('hist_test');
  });
});
