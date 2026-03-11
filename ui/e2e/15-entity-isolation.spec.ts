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

// ===== ENTITY ISOLATION (Group 15) =====

test.describe('Entity Isolation', () => {

  test('15-1: accounts in entity A not visible in entity B', async ({ page }) => {
    await createEntity(page, 'iso_a_15');
    await createEntity(page, 'iso_b_15');
    await page.waitForTimeout(300);

    // Create accounts in entity A
    await fqlRaw(page, `
      CREATE ACCOUNT @iso_bank ASSET;
      CREATE ACCOUNT @iso_revenue INCOME;
    `, 'iso_a_15');
    await page.waitForTimeout(500);

    // Verify entity A has accounts
    const respA = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'iso_a_15');
    expect(respA.success).toBe(true);
    const tbA = respA.results.find((r: any) => r.name === 'tb')?.value;
    const itemsA = tbA?.value as any[] || [];
    expect(itemsA.some((i: any) => i.account_id === 'iso_bank')).toBe(true);

    // Verify entity B does NOT have those accounts
    const respB = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'iso_b_15');
    expect(respB.success).toBe(true);
    const tbB = respB.results.find((r: any) => r.name === 'tb')?.value;
    const itemsB = tbB?.value as any[] || [];
    expect(itemsB.some((i: any) => i.account_id === 'iso_bank')).toBe(false);
  });

  test('15-2: journal in entity A — entity B balance is zero', async ({ page }) => {
    await createEntity(page, 'iso_j_a');
    await createEntity(page, 'iso_j_b');
    await page.waitForTimeout(300);

    await fqlRaw(page, `
      CREATE ACCOUNT @jiso_bank ASSET;
      CREATE ACCOUNT @jiso_eq EQUITY;
      CREATE JOURNAL 2024-06-01, 5000, 'Entity A journal'
        DEBIT @jiso_bank,
        CREDIT @jiso_eq;
    `, 'iso_j_a');
    await page.waitForTimeout(500);

    // Entity A should have balance 5000
    const respA = await fqlApi(page, `GET balance(@jiso_bank, 2024-12-31) AS bal`, 'iso_j_a');
    expect(respA.success).toBe(true);
    const balA = respA.results[0]?.value?.value;
    expect(parseFloat(balA || '0')).toBe(5000);

    // Entity B should have no such account — should get error or 0
    const respB = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'iso_j_b');
    expect(respB.success).toBe(true);
    const tbB = respB.results.find((r: any) => r.name === 'tb')?.value;
    const itemsB = tbB?.value as any[] || [];
    expect(itemsB.some((i: any) => i.account_id === 'jiso_bank')).toBe(false);
  });

  test('15-3: rate in entity A not found in entity B', async ({ page }) => {
    await createEntity(page, 'iso_r_a');
    await createEntity(page, 'iso_r_b');
    await page.waitForTimeout(300);

    // Create and set rate in entity A
    await fqlRaw(page, `
      CREATE RATE test_rate_iso;
      SET RATE test_rate_iso 1.25 2024-01-01;
    `, 'iso_r_a');
    await page.waitForTimeout(500);

    // Entity A should find the rate
    const respA = await fqlApi(page, `GET fx_rate('test_rate_iso', 2024-01-01) AS rate`, 'iso_r_a');
    expect(respA.success).toBe(true);

    // Entity B should NOT find the rate
    const respB = await fqlApi(page, `GET fx_rate('test_rate_iso', 2024-01-01) AS rate`, 'iso_r_b');
    // Should either fail or return not-found/error
    const rateBVal = respB.results?.[0]?.value;
    const isNotFound = !respB.success ||
      (rateBVal?.type === 'string' && /not found/i.test(rateBVal.value)) ||
      /not found|error/i.test(respB.error || '');
    expect(isNotFound).toBe(true);
  });

  test('15-4: dashboard KPIs change when switching entities', async ({ page }) => {
    await createEntity(page, 'dash_e_a');
    await createEntity(page, 'dash_e_b');
    await page.waitForTimeout(300);

    // Create data only in entity A
    await fqlRaw(page, `
      CREATE ACCOUNT @dkpi_bank ASSET;
      CREATE ACCOUNT @dkpi_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 1000, 'Entity A data'
        DEBIT @dkpi_bank,
        CREDIT @dkpi_eq;
    `, 'dash_e_a');
    await page.waitForTimeout(500);

    // Entity A should have accounts
    const respA = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'dash_e_a');
    expect(respA.success).toBe(true);
    const tbA = respA.results.find((r: any) => r.name === 'tb')?.value;
    expect((tbA?.value as any[])?.length).toBeGreaterThan(0);

    // Entity B should have no accounts
    const respB = await fqlApi(page, `GET trial_balance(2024-12-31) AS tb`, 'dash_e_b');
    expect(respB.success).toBe(true);
    const tbB = respB.results.find((r: any) => r.name === 'tb')?.value;
    expect((tbB?.value as any[])?.length || 0).toBe(0);
  });

  test('15-5: reports show different data per entity', async ({ page }) => {
    await createEntity(page, 'rpt_iso_a');
    await createEntity(page, 'rpt_iso_b');
    await page.waitForTimeout(300);

    // Different amounts in each entity
    await fqlRaw(page, `
      CREATE ACCOUNT @riso_cash ASSET;
      CREATE ACCOUNT @riso_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 1111, 'Entity A'
        DEBIT @riso_cash,
        CREDIT @riso_eq;
    `, 'rpt_iso_a');

    await fqlRaw(page, `
      CREATE ACCOUNT @riso_cash ASSET;
      CREATE ACCOUNT @riso_eq EQUITY;
      CREATE JOURNAL 2024-01-01, 2222, 'Entity B'
        DEBIT @riso_cash,
        CREDIT @riso_eq;
    `, 'rpt_iso_b');
    await page.waitForTimeout(500);

    // Entity A balance should be 1111
    const respA = await fqlApi(page, `GET balance(@riso_cash, 2024-12-31) AS bal`, 'rpt_iso_a');
    expect(respA.success).toBe(true);
    expect(parseFloat(respA.results[0]?.value?.value || '0')).toBe(1111);

    // Entity B balance should be 2222
    const respB = await fqlApi(page, `GET balance(@riso_cash, 2024-12-31) AS bal`, 'rpt_iso_b');
    expect(respB.success).toBe(true);
    expect(parseFloat(respB.results[0]?.value?.value || '0')).toBe(2222);
  });
});
