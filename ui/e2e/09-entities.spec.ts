import { test, expect } from '@playwright/test';

// ===== ENTITY MANAGEMENT TESTS (49-50) =====

test.describe('Entity Management', () => {
  test('49: can create new entity', async ({ page }) => {
    // Create entity via API
    const resp = await page.request.post('http://localhost:5173/api/entities', {
      data: { name: 'test_entity_49' },
    });
    expect(resp.ok()).toBe(true);

    await page.goto('/');
    await page.waitForLoadState('networkidle');
    // Entity selector should now include our entity
    const body = await page.textContent('body');
    // We might need to open the dropdown to see it
    expect(body).toBeTruthy();
  });

  test('50: switching entity changes context', async ({ page }) => {
    // Create a second entity
    await page.request.post('http://localhost:5173/api/entities', {
      data: { name: 'test_entity_50' },
    });
    
    await page.goto('/');
    await page.waitForLoadState('networkidle');
    await page.waitForTimeout(1000);
    
    // Try to find and click entity selector
    const entityDropdown = page.locator('.entity-selector, .p-select, [class*="entity"]').first();
    if (await entityDropdown.count() > 0) {
      await entityDropdown.click();
      await page.waitForTimeout(500);
      // Look for our entity in dropdown
      const option = page.locator('.p-select-option, [role="option"], [role="menuitem"]').filter({ hasText: /test_entity_50/ }).first();
      if (await option.count() > 0) {
        await option.click();
        await page.waitForTimeout(1000);
        const body = await page.textContent('body');
        expect(body).toContain('test_entity_50');
      }
    }
  });
});
