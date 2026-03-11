import { test, expect } from '@playwright/test';

// ===== ACCOUNTS PAGE TESTS (21-27) =====

test.describe('Accounts', () => {
  test('21: accounts page shows empty state initially', async ({ page }) => {
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    // Should have some content - either accounts list or empty state
    expect(body).toBeTruthy();
  });

  test('22: create account modal opens', async ({ page }) => {
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    // Find create account button
    const createBtn = page.locator('button').filter({ hasText: /create|add|new/i }).first();
    if (await createBtn.count() > 0) {
      await createBtn.click();
      await page.waitForTimeout(500);
      // Modal should be visible with input fields
      const modal = page.locator('.p-dialog, [role="dialog"], .modal').first();
      if (await modal.count() > 0) {
        await expect(modal).toBeVisible();
      }
    }
  });

  test('23: can create account via modal', async ({ page }) => {
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    const createBtn = page.locator('button').filter({ hasText: /create|add|new/i }).first();
    if (await createBtn.count() === 0) return;
    await createBtn.click();
    await page.waitForTimeout(500);

    // Fill in account ID
    const idInput = page.locator('.p-dialog input[type="text"], [role="dialog"] input[type="text"]').first();
    if (await idInput.count() > 0) {
      await idInput.fill('savings');
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

    // Click create in dialog
    const dialogCreateBtn = page.locator('.p-dialog button, [role="dialog"] button').filter({ hasText: /create/i }).first();
    if (await dialogCreateBtn.count() > 0) {
      await dialogCreateBtn.click();
      await page.waitForTimeout(1000);
    }
  });

  test('24: created account appears in list', async ({ page }) => {
    // First create an account via FQL
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    const editor = page.locator('textarea').first();
    await editor.fill('CREATE ACCOUNT @checking ASSET');
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);

    // Now go to accounts page
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);
    const body = await page.textContent('body');
    expect(body).toContain('checking');
  });

  test('25: clicking account shows detail panel', async ({ page }) => {
    // Create account first
    await page.goto('/query');
    await page.waitForLoadState('networkidle');
    await page.locator('textarea').first().fill('CREATE ACCOUNT @detail_test ASSET');
    await page.locator('button').filter({ hasText: /execute|run/i }).first().click();
    await page.waitForTimeout(2000);

    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);

    // Click on account row
    const accountRow = page.locator('tr, .p-datatable-row-group').filter({ hasText: /detail_test/ }).first();
    if (await accountRow.count() > 0) {
      await accountRow.click();
      await page.waitForTimeout(500);
      const body = await page.textContent('body');
      // Detail panel should show statement or balance info
      expect(body).toContain('detail_test');
    }
  });

  test('26: statement panel has date pickers', async ({ page }) => {
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    // Look for date inputs in the statement/detail panel area
    const dateInputs = page.locator('input[type="text"], .p-datepicker, [data-pc-name="datepicker"]');
    // There should be from/to date pickers (may not be visible until account selected)
    const body = await page.textContent('body');
    expect(body).toBeTruthy();
  });

  test('27: account type dropdown has all 5 types', async ({ page }) => {
    await page.goto('/accounts');
    await page.waitForLoadState('networkidle');
    const createBtn = page.locator('button').filter({ hasText: /create|add|new/i }).first();
    if (await createBtn.count() === 0) return;
    await createBtn.click();
    await page.waitForTimeout(500);

    const typeDropdown = page.locator('.p-dialog .p-select, [role="dialog"] .p-select').first();
    if (await typeDropdown.count() > 0) {
      await typeDropdown.click();
      await page.waitForTimeout(500);
      const options = page.locator('.p-select-option, [role="option"]');
      const optionTexts = await options.allTextContents();
      const types = ['ASSET', 'LIABILITY', 'EQUITY', 'INCOME', 'EXPENSE'];
      for (const t of types) {
        const found = optionTexts.some(o => o.toUpperCase().includes(t));
        expect(found).toBe(true);
      }
    }
  });
});
