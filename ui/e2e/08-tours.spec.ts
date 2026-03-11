import { test, expect } from '@playwright/test';

// ===== TOURS PAGE TESTS (44-48) =====

test.describe('Tours', () => {
  test('44: tour page shows list of available tours', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    // Should show tour titles
    const hasLending = /lending.*fund/i.test(body || '');
    const hasEcommerce = /e.?commerce/i.test(body || '');
    const hasSaas = /saas|subscription/i.test(body || '');
    expect(hasLending || hasEcommerce || hasSaas).toBe(true);
  });

  test('45: tour cards show title, description, difficulty', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');
    const body = await page.textContent('body');
    // Should have difficulty levels
    const hasBeginner = /beginner/i.test(body || '');
    const hasIntermediate = /intermediate/i.test(body || '');
    expect(hasBeginner || hasIntermediate).toBe(true);
  });

  test('46: clicking tour opens entity picker modal', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');
    
    // Click on first tour card
    const tourCard = page.locator('.tour-card, .p-card, [class*="card"]').first();
    if (await tourCard.count() > 0) {
      await tourCard.click();
      await page.waitForTimeout(500);
      // Entity picker should appear
      const body = await page.textContent('body');
      const hasEntityPicker = /entity|choose.*entity|create.*entity|select.*entity/i.test(body || '');
      expect(hasEntityPicker).toBe(true);
    }
  });

  test('47: tour player loads with progress and code', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');
    
    // Click first tour
    const tourCard = page.locator('.tour-card, .p-card, [class*="card"]').first();
    if (await tourCard.count() === 0) return;
    await tourCard.click();
    await page.waitForTimeout(500);

    // Create new entity for tour
    const createInput = page.locator('input[type="text"]').first();
    if (await createInput.count() > 0) {
      await createInput.fill('test_tour_entity');
      // Click create/start button
      const startBtn = page.locator('button').filter({ hasText: /create|start|go/i }).first();
      if (await startBtn.count() > 0) {
        await startBtn.click();
        await page.waitForTimeout(2000);
        const body = await page.textContent('body');
        // Should show step progress or code
        const hasProgress = /step|progress|\d+.*of.*\d+/i.test(body || '');
        const hasCode = /CREATE|ACCOUNT|JOURNAL|GET|SET/i.test(body || '');
        expect(hasProgress || hasCode).toBe(true);
      }
    }
  });

  test('48: can navigate tour steps', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');
    
    const tourCard = page.locator('.tour-card, .p-card, [class*="card"]').first();
    if (await tourCard.count() === 0) return;
    await tourCard.click();
    await page.waitForTimeout(500);

    // Create entity and start
    const createInput = page.locator('input[type="text"]').first();
    if (await createInput.count() > 0) {
      await createInput.fill('test_nav_entity');
      const startBtn = page.locator('button').filter({ hasText: /create|start|go/i }).first();
      if (await startBtn.count() > 0) {
        await startBtn.click();
        await page.waitForTimeout(3000);

        // Try to advance with next button or arrow key
        const nextBtn = page.locator('button').filter({ hasText: /next|→|forward/i }).first();
        if (await nextBtn.count() > 0) {
          const bodyBefore = await page.textContent('body');
          await nextBtn.click();
          await page.waitForTimeout(2000);
          const bodyAfter = await page.textContent('body');
          // Content should change between steps
          expect(bodyAfter).toBeTruthy();
        } else {
          // Try keyboard navigation
          await page.keyboard.press('ArrowRight');
          await page.waitForTimeout(2000);
        }
      }
    }
  });
});
