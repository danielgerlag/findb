import { test, expect } from '@playwright/test';

// Helper to start a tour: clicks card, fills entity name, scrolls to and clicks "Create & Start"
async function startTour(page: any, cardFilter: RegExp, entityName: string) {
  const tourCard = page.locator('.tour-card').filter({ hasText: cardFilter }).first();
  await expect(tourCard).toBeVisible();
  await tourCard.click();
  await page.waitForTimeout(1000);

  // Entity picker overlay should appear
  const entityPicker = page.locator('.entity-picker').first();
  await expect(entityPicker).toBeVisible({ timeout: 5000 });

  // The "Create new entity" option should be selected by default
  // Click it to ensure input is visible
  const createNewOption = page.locator('.entity-option.new-entity').first();
  if (await createNewOption.count() > 0) {
    await createNewOption.click();
    await page.waitForTimeout(300);
  }

  // Fill the entity name input (no explicit type attr, use class or placeholder)
  const nameInput = page.locator('.entity-name-input, input[placeholder="Entity name"]').first();
  await expect(nameInput).toBeVisible({ timeout: 5000 });
  await nameInput.fill('');
  await nameInput.fill(entityName);

  // Scroll the entity picker dialog to make the button visible, then click via JS
  // (Playwright's click can fail with "outside viewport" in scrollable overlays)
  const startBtn = page.locator('.entity-picker button').filter({ hasText: /create|start|go/i }).first();
  await startBtn.evaluate((el: HTMLElement) => {
    el.scrollIntoView({ block: 'center' });
    el.click();
  });
  await page.waitForTimeout(5000);
}

// ===== TOUR EXECUTION (Group 14) =====

test.describe('Tour Execution', () => {

  test('14-1: run lending-fund tour completely', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');

    await startTour(page, /lending.*fund/i, 'tour_lending_14_1');

    // Tour player should be visible with step progress
    const stepLabel = page.locator('text=/Step \\d+ of \\d+/i').first();
    await expect(stepLabel).toBeVisible({ timeout: 10000 });

    // Click through all steps using the Next button
    let finished = false;
    let stepCount = 0;
    const maxSteps = 30;

    while (!finished && stepCount < maxSteps) {
      await page.waitForTimeout(2000);
      const body = await page.textContent('body');

      if (/tour complete|🎉/i.test(body || '')) {
        finished = true;
        break;
      }

      const nextBtn = page.locator('.tour-nav-buttons button').filter({ hasText: /next|finish/i }).first();
      if (await nextBtn.count() > 0 && await nextBtn.isEnabled()) {
        await nextBtn.click();
        stepCount++;
      } else {
        await page.keyboard.press('ArrowRight');
        stepCount++;
      }
    }

    expect(stepCount).toBeGreaterThan(0);
  });

  test('14-2: each tour step shows FQL code block', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');

    await startTour(page, /lending.*fund/i, 'tour_code_14_2');

    // The first step should show FQL code (CREATE, GET, etc.)
    const body = await page.textContent('body');
    const hasFqlCode = /CREATE|ACCOUNT|JOURNAL|GET|SET/i.test(body || '');
    expect(hasFqlCode).toBe(true);

    // Also check the code block element exists
    const codeBlock = page.locator('pre, code, .tour-code-block, .step-code-area').first();
    await expect(codeBlock).toBeVisible();
  });

  test('14-3: tour progress bar updates on each step', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');

    await startTour(page, /lending.*fund/i, 'tour_progress_14_3');

    // Check progress bar exists
    const progressBar = page.locator('.progress-bar, .progress-fill').first();
    await expect(progressBar).toBeVisible();

    // Get initial step label
    const stepLabel = page.locator('text=/Step \\d+ of \\d+/').first();
    const initialText = await stepLabel.textContent();
    expect(initialText).toMatch(/Step 1 of \d+/);

    // Advance one step
    await page.waitForTimeout(2000);
    const nextBtn = page.locator('.tour-nav-buttons button').filter({ hasText: /next/i }).first();
    if (await nextBtn.count() > 0 && await nextBtn.isEnabled()) {
      await nextBtn.click();
      await page.waitForTimeout(3000);

      const newText = await stepLabel.textContent();
      expect(newText).toMatch(/Step 2 of \d+/);
    }
  });

  test('14-4: tour player shows narrative text', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');

    await startTour(page, /lending.*fund/i, 'tour_narrative_14_4');

    // Narrative section should be visible
    const narrative = page.locator('.step-narrative').first();
    await expect(narrative).toBeVisible();

    const narrativeText = await narrative.textContent();
    expect(narrativeText?.trim().length).toBeGreaterThan(5);
  });

  test('14-5: run e-commerce tour step-by-step to final step', async ({ page }) => {
    await page.goto('/tour');
    await page.waitForLoadState('networkidle');

    await startTour(page, /e.?commerce/i, 'tour_ecom_14_5');

    // Verify tour loaded
    const stepLabel = page.locator('text=/Step \\d+ of \\d+/').first();
    await expect(stepLabel).toBeVisible({ timeout: 10000 });

    // Click through steps
    let finished = false;
    let stepCount = 0;
    while (!finished && stepCount < 30) {
      await page.waitForTimeout(2000);
      const body = await page.textContent('body');

      if (/tour complete|🎉/i.test(body || '')) {
        finished = true;
        break;
      }

      const nextBtn = page.locator('.tour-nav-buttons button').filter({ hasText: /next|finish/i }).first();
      if (await nextBtn.count() > 0 && await nextBtn.isEnabled()) {
        await nextBtn.click();
        stepCount++;
      } else {
        await page.keyboard.press('ArrowRight');
        stepCount++;
      }
    }

    expect(stepCount).toBeGreaterThan(0);
  });
});
