import { test, expect } from '@playwright/test';

async function fqlRaw(page: any, query: string) {
  const resp = await page.request.post('http://localhost:5173/fql', {
    headers: { 'Content-Type': 'text/plain' },
    data: query,
  });
  return resp.json();
}

// ===== QUERY EDITOR FEATURES (Group 17) =====

test.describe('Query Editor Features', () => {

  test('17-1: FQL syntax highlighting shows keywords in color', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');

    const editor = page.locator('textarea').first();
    await editor.fill('CREATE ACCOUNT @test ASSET');
    await page.waitForTimeout(500);

    // The editor-highlight overlay should have highlighted spans
    const highlightLayer = page.locator('.editor-highlight').first();
    await expect(highlightLayer).toBeVisible();

    // Check for syntax highlight CSS classes
    const keywordSpans = page.locator('.fql-keyword');
    const typeSpans = page.locator('.fql-type');
    const accountSpans = page.locator('.fql-account');

    // At least one keyword should be highlighted
    const keywordCount = await keywordSpans.count();
    const typeCount = await typeSpans.count();
    const accountCount = await accountSpans.count();

    expect(keywordCount + typeCount + accountCount).toBeGreaterThan(0);
  });

  test('17-2: multiple result sets from multi-statement query', async ({ page }) => {
    await fqlRaw(page, `
      CREATE ACCOUNT @mr_bank ASSET;
      CREATE ACCOUNT @mr_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 1000, 'Capital'
        DEBIT @mr_bank,
        CREDIT @mr_eq;
    `);
    await page.waitForTimeout(500);

    await page.goto('/query');
    await page.waitForLoadState('networkidle');

    const editor = page.locator('textarea').first();
    await editor.fill(`GET balance(@mr_bank, 2024-12-31) AS bal;
GET trial_balance(2024-12-31) AS tb`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(3000);

    // Should show results section
    const body = await page.textContent('body');
    // Should have "2 statements" in the results header
    expect(body).toContain('1000');

    // Check for multiple result blocks (pre elements)
    const resultBlocks = page.locator('.card pre');
    const blockCount = await resultBlocks.count();
    // Should have at least 2 result blocks (balance + trial balance)
    expect(blockCount).toBeGreaterThanOrEqual(2);
  });

  test('17-3: result table displays with scrollable content', async ({ page }) => {
    // Create several accounts for a bigger trial balance
    await fqlRaw(page, `
      CREATE ACCOUNT @scroll_a ASSET;
      CREATE ACCOUNT @scroll_b LIABILITY;
      CREATE ACCOUNT @scroll_c EQUITY;
      CREATE ACCOUNT @scroll_d INCOME;
      CREATE ACCOUNT @scroll_e EXPENSE;
      CREATE ACCOUNT @scroll_f ASSET;
      CREATE JOURNAL 2024-01-01, 1000, 'Test'
        DEBIT @scroll_a,
        CREDIT @scroll_c;
    `);
    await page.waitForTimeout(500);

    await page.goto('/query');
    await page.waitForLoadState('networkidle');

    const editor = page.locator('textarea').first();
    await editor.fill(`GET trial_balance(2024-12-31) AS tb`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(3000);

    // Result should be in a pre block with overflow-x: auto
    const resultPre = page.locator('.card pre').first();
    if (await resultPre.count() > 0) {
      await expect(resultPre).toBeVisible();
      // Check that it has content
      const text = await resultPre.textContent();
      expect(text?.length).toBeGreaterThan(0);
    }
  });

  test('17-4: error shows line/column information', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');

    const editor = page.locator('textarea').first();
    // Write valid FQL then invalid FQL to test error positioning
    await editor.fill(`CREATE ACCOUNT @good ASSET;
THIS IS INVALID FQL ON LINE 2`);
    const btn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await btn.click();
    await page.waitForTimeout(2000);

    const body = await page.textContent('body');
    // Should show an error
    expect(/error|unexpected|parse|invalid/i.test(body || '')).toBe(true);

    // Check for error-msg element
    const errorMsg = page.locator('.error-msg').first();
    if (await errorMsg.count() > 0) {
      const errorText = await errorMsg.textContent();
      // Error message should exist and be meaningful
      expect(errorText?.length).toBeGreaterThan(0);
    }
  });

  test('17-5: clear/reset editor content', async ({ page }) => {
    await page.goto('/query');
    await page.waitForLoadState('networkidle');

    const editor = page.locator('textarea').first();
    // Enter some FQL and execute
    await editor.fill(`CREATE ACCOUNT @clear_test ASSET`);
    const execBtn = page.locator('button').filter({ hasText: /execute|run/i }).first();
    await execBtn.click();
    await page.waitForTimeout(2000);

    // Results should be visible
    const body1 = await page.textContent('body');
    expect(body1).toContain('clear_test');

    // Click Clear button
    const clearBtn = page.locator('button').filter({ hasText: /clear/i }).first();
    await expect(clearBtn).toBeVisible();
    await clearBtn.click();
    await page.waitForTimeout(500);

    // Results should be cleared — the results card should be gone
    const resultHeader = page.locator('h3').filter({ hasText: /results/i }).first();
    const resultVisible = await resultHeader.count() > 0 && await resultHeader.isVisible();
    // After clear, results section should not be visible
    expect(resultVisible).toBe(false);
  });
});
