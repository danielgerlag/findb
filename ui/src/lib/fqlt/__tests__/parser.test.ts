import { describe, it, expect } from 'vitest'
import { parseTour, extractFql } from '../parser'

describe('parseTour', () => {
  it('returns empty tour for empty input', () => {
    const tour = parseTour('')
    expect(tour.meta).toEqual({})
    expect(tour.steps).toEqual([])
  })

  it('parses frontmatter only', () => {
    const tour = parseTour(`---
title: "My Tour"
author: Daniel
difficulty: beginner
tags: [lending, accruals]
version: 1
---
`)
    expect(tour.meta.title).toBe('My Tour')
    expect(tour.meta.author).toBe('Daniel')
    expect(tour.meta.difficulty).toBe('beginner')
    expect(tour.meta.tags).toEqual(['lending', 'accruals'])
    expect(tour.meta.version).toBe(1)
    expect(tour.steps).toEqual([])
  })

  it('parses single step with title and code', () => {
    const tour = parseTour(`--@ step: Create Accounts
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @equity EQUITY;
`)
    expect(tour.steps).toHaveLength(1)
    expect(tour.steps[0]!.title).toBe('Create Accounts')
    expect(tour.steps[0]!.code).toBe('CREATE ACCOUNT @bank ASSET;\nCREATE ACCOUNT @equity EQUITY;')
  })

  it('parses multi-line text with continuation lines', () => {
    const tour = parseTour(`--@ step: Test
--@ text: Line one of the text
--@   and line two continues here
--@   and line three as well
CREATE ACCOUNT @bank ASSET;
`)
    expect(tour.steps[0]!.text).toBe(
      'Line one of the text\nand line two continues here\nand line three as well'
    )
  })

  it('parses all directive types', () => {
    const tour = parseTour(`--@ step: Full Step
--@ text: Some narrative
--@ note: A callout note
--@ caption: Inline label
--@ highlight: @bank, CREDIT, ASSET
--@ focus: lines:1-2
--@ reveal: typewriter
--@ run: click
--@ pause: 5
--@ expect: 1 account created
--@ assert: result = 100
--@ assert: tb contains bank
--@ show: result, tb
--@ hide-output
--@ layout: split
CREATE ACCOUNT @bank ASSET;
`)
    const step = tour.steps[0]!
    expect(step.title).toBe('Full Step')
    expect(step.text).toBe('Some narrative')
    expect(step.note).toBe('A callout note')
    expect(step.caption).toBe('Inline label')
    expect(step.highlight).toEqual(['@bank', 'CREDIT', 'ASSET'])
    expect(step.focus).toBe('lines:1-2')
    expect(step.reveal).toBe('typewriter')
    expect(step.run).toBe('click')
    expect(step.pause).toBe(5)
    expect(step.expect).toBe('1 account created')
    expect(step.assert).toEqual([
      { variable: 'result', operator: '=', expected: '100' },
      { variable: 'tb', operator: 'contains', expected: 'bank' },
    ])
    expect(step.show).toEqual(['result', 'tb'])
    expect(step.hideOutput).toBe(true)
    expect(step.layout).toBe('split')
  })

  it('parses multiple steps', () => {
    const tour = parseTour(`--@ step: First
CREATE ACCOUNT @bank ASSET;

--@ step: Second
CREATE ACCOUNT @equity EQUITY;

--@ step: Third
GET trial_balance(2023-12-31) AS tb
`)
    expect(tour.steps).toHaveLength(3)
    expect(tour.steps[0]!.title).toBe('First')
    expect(tour.steps[1]!.title).toBe('Second')
    expect(tour.steps[2]!.title).toBe('Third')
    expect(tour.steps[0]!.code).toBe('CREATE ACCOUNT @bank ASSET;')
    expect(tour.steps[1]!.code).toBe('CREATE ACCOUNT @equity EQUITY;')
  })

  it('handles bare --@ as paragraph spacer in text', () => {
    const tour = parseTour(`--@ step: Test
--@ text: Line one
--@
--@   Line three after blank
CREATE ACCOUNT @bank ASSET;
`)
    expect(tour.steps[0]!.text).toBe('Line one\n\nLine three after blank')
    expect(tour.steps[0]!.code).toBe('CREATE ACCOUNT @bank ASSET;')
  })

  it('creates implicit step for code before any step directive', () => {
    const tour = parseTour(`CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @equity EQUITY;
`)
    expect(tour.steps).toHaveLength(1)
    expect(tour.steps[0]!.title).toBe('')
    expect(tour.steps[0]!.code).toContain('CREATE ACCOUNT @bank ASSET;')
  })

  it('preserves regular comments as code', () => {
    const tour = parseTour(`--@ step: Test
-- This is a regular FQL comment
CREATE ACCOUNT @bank ASSET;
`)
    expect(tour.steps[0]!.code).toBe('-- This is a regular FQL comment\nCREATE ACCOUNT @bank ASSET;')
  })

  it('ignores unknown directives for forward compatibility', () => {
    const tour = parseTour(`--@ step: Test
--@ future-directive: some value
CREATE ACCOUNT @bank ASSET;
`)
    expect(tour.steps).toHaveLength(1)
    expect(tour.steps[0]!.code).toBe('CREATE ACCOUNT @bank ASSET;')
  })

  it('handles valueless directives (wait, hide-output)', () => {
    const tour = parseTour(`--@ step: Test
--@ wait
--@ hide-output
CREATE ACCOUNT @bank ASSET;
`)
    expect(tour.steps[0]!.wait).toBe(true)
    expect(tour.steps[0]!.hideOutput).toBe(true)
  })

  it('parses frontmatter with single-quoted values', () => {
    const tour = parseTour(`---
title: 'Single Quoted'
---
--@ step: Test
CREATE ACCOUNT @bank ASSET;
`)
    expect(tour.meta.title).toBe('Single Quoted')
  })

  it('handles assert with contains operator', () => {
    const tour = parseTour(`--@ step: Test
--@ assert: result contains interest_earned
CREATE ACCOUNT @bank ASSET;
`)
    expect(tour.steps[0]!.assert).toEqual([
      { variable: 'result', operator: 'contains', expected: 'interest_earned' },
    ])
  })

  it('parses the lending fund sample end-to-end', () => {
    const source = `---
title: "Building a Lending Fund"
difficulty: beginner
tags: [lending, accruals]
---

--@ step: Setup
--@ text: Create accounts.
--@ highlight: ASSET
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @loans ASSET;

--@ step: Rate
--@ text: Define interest rate.
CREATE RATE prime;
SET RATE prime 0.05 2023-01-01;

--@ step: Results
--@ show: Total
--@ assert: Total = 1506.34
GET balance(@loans, 2023-03-01) AS Total
`
    const tour = parseTour(source)
    expect(tour.meta.title).toBe('Building a Lending Fund')
    expect(tour.meta.difficulty).toBe('beginner')
    expect(tour.meta.tags).toEqual(['lending', 'accruals'])
    expect(tour.steps).toHaveLength(3)
    expect(tour.steps[0]!.highlight).toEqual(['ASSET'])
    expect(tour.steps[2]!.assert).toEqual([
      { variable: 'Total', operator: '=', expected: '1506.34' },
    ])
  })
})

describe('extractFql', () => {
  it('strips frontmatter and directives, preserves code', () => {
    const source = `---
title: "Test"
---

--@ step: Create Accounts
--@ text: We create accounts.
CREATE ACCOUNT @bank ASSET;
CREATE ACCOUNT @equity EQUITY;

--@ step: Query
--@ text: Check balances.
-- This is a regular comment
GET balance(@bank, 2023-12-31) AS result
`
    const fql = extractFql(source)
    expect(fql).not.toContain('---')
    expect(fql).not.toContain('--@')
    expect(fql).toContain('CREATE ACCOUNT @bank ASSET;')
    expect(fql).toContain('CREATE ACCOUNT @equity EQUITY;')
    expect(fql).toContain('-- This is a regular comment')
    expect(fql).toContain('GET balance(@bank, 2023-12-31) AS result')
  })

  it('returns code for a plain FQL file with no directives', () => {
    const source = `CREATE ACCOUNT @bank ASSET;
GET balance(@bank, 2023-12-31) AS result
`
    const fql = extractFql(source)
    expect(fql).toBe('CREATE ACCOUNT @bank ASSET;\nGET balance(@bank, 2023-12-31) AS result\n')
  })
})
