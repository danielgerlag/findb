import type { Tour, TourStep, TourMeta, TourAssert } from './types'

/**
 * Parse a .fqlt source string into a Tour AST.
 */
export function parseTour(source: string): Tour {
  const lines = source.replace(/\r\n/g, '\n').split('\n')
  let cursor = 0

  // --- Pass 1: frontmatter ---
  const meta = parseFrontmatter(lines, (pos) => { cursor = pos })

  // --- Pass 2: steps ---
  const steps: TourStep[] = []
  let current: BuildingStep | null = null
  let lastDirective: string | null = null

  for (let i = cursor; i < lines.length; i++) {
    const line = lines[i]!
    const contMatch = line.match(/^--@\s{2,}(.*)$/)
    if (contMatch) {
      // Continuation line — append to last directive value
      if (current && lastDirective) {
        appendDirective(current, lastDirective, contMatch[1]!)
      }
      continue
    }

    // Bare --@ line (empty continuation / paragraph spacer in text)
    if (line.match(/^--@\s*$/)) {
      if (current && lastDirective) {
        appendDirective(current, lastDirective, '')
      }
      continue
    }

    const dirMatch = line.match(/^--@\s*(\w[\w-]*):\s*(.*)$/)
    if (dirMatch) {
      const key = dirMatch[1]!
      const value = dirMatch[2]!

      if (key === 'step') {
        // Flush previous step
        if (current) steps.push(finalizeStep(current))
        current = createBuildingStep(value)
      } else {
        if (!current) current = createBuildingStep('')
        setDirective(current, key, value)
      }
      lastDirective = key
      continue
    }

    // Valueless directive (e.g. --@ wait, --@ hide-output)
    const valuelessMatch = line.match(/^--@\s*(\w[\w-]*)\s*$/)
    if (valuelessMatch) {
      const key = valuelessMatch[1]!
      if (!current) current = createBuildingStep('')
      setDirective(current, key, '')
      lastDirective = key
      continue
    }

    // Regular line (FQL code or blank)
    if (current) {
      current.codeLines.push(line)
    } else if (line.trim().length > 0) {
      // Code before any step directive → implicit step 0
      current = createBuildingStep('')
      current.codeLines.push(line)
    }
    lastDirective = null
  }

  if (current) steps.push(finalizeStep(current))

  return { meta, steps }
}

/**
 * Strip all directives and frontmatter, returning only runnable FQL.
 */
export function extractFql(source: string): string {
  const lines = source.replace(/\r\n/g, '\n').split('\n')
  const result: string[] = []
  let inFrontmatter = false

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i]!

    // Frontmatter handling
    if (i === 0 && line.trim() === '---') {
      inFrontmatter = true
      continue
    }
    if (inFrontmatter) {
      if (line.trim() === '---') {
        inFrontmatter = false
      }
      continue
    }

    // Skip directive lines
    if (line.match(/^--@\s/)) continue

    result.push(line)
  }

  return result.join('\n').trim() + '\n'
}

// --- Internal helpers ---

interface BuildingStep {
  title: string
  text: string[]
  note: string[]
  caption: string[]
  expect: string[]
  codeLines: string[]
  highlight?: string[]
  focus?: string
  reveal?: 'instant' | 'typewriter' | 'line-by-line'
  run?: 'auto' | 'click' | 'skip'
  pause?: number
  wait?: boolean
  asserts: TourAssert[]
  show?: string[]
  hideOutput?: boolean
  layout?: 'stacked' | 'split' | 'full-code'
}

function createBuildingStep(title: string): BuildingStep {
  return {
    title,
    text: [],
    note: [],
    caption: [],
    expect: [],
    codeLines: [],
    asserts: [],
  }
}

function setDirective(step: BuildingStep, key: string, value: string): void {
  switch (key) {
    case 'text':
      step.text.push(value)
      break
    case 'note':
      step.note.push(value)
      break
    case 'caption':
      step.caption.push(value)
      break
    case 'expect':
      step.expect.push(value)
      break
    case 'highlight':
      step.highlight = value.split(',').map((s) => s.trim()).filter(Boolean)
      break
    case 'focus':
      step.focus = value
      break
    case 'reveal':
      if (value === 'typewriter' || value === 'line-by-line' || value === 'instant') {
        step.reveal = value
      }
      break
    case 'run':
      if (value === 'auto' || value === 'click' || value === 'skip') {
        step.run = value
      }
      break
    case 'pause':
      step.pause = parseFloat(value) || undefined
      break
    case 'wait':
      step.wait = true
      break
    case 'assert':
      step.asserts.push(parseAssert(value))
      break
    case 'show':
      step.show = value.split(',').map((s) => s.trim()).filter(Boolean)
      break
    case 'hide-output':
      step.hideOutput = true
      break
    case 'layout':
      if (value === 'stacked' || value === 'split' || value === 'full-code') {
        step.layout = value
      }
      break
    // Unknown directives are silently ignored (forward compat)
  }
}

function appendDirective(step: BuildingStep, key: string, value: string): void {
  switch (key) {
    case 'text':
      step.text.push(value)
      break
    case 'note':
      step.note.push(value)
      break
    case 'caption':
      step.caption.push(value)
      break
    case 'expect':
      step.expect.push(value)
      break
    // Other directives don't support continuation
  }
}

function parseAssert(value: string): TourAssert {
  const containsMatch = value.match(/^(\w+)\s+contains\s+(.+)$/)
  if (containsMatch) {
    return { variable: containsMatch[1]!, operator: 'contains', expected: containsMatch[2]!.trim() }
  }
  const eqMatch = value.match(/^(\w+)\s*=\s*(.+)$/)
  if (eqMatch) {
    return { variable: eqMatch[1]!, operator: '=', expected: eqMatch[2]!.trim() }
  }
  return { variable: value, operator: '=', expected: '' }
}

function finalizeStep(step: BuildingStep): TourStep {
  // Trim leading/trailing blank lines from code
  const code = trimBlankLines(step.codeLines).join('\n')
  const result: TourStep = {
    title: step.title,
    code,
  }

  if (step.text.length > 0) result.text = step.text.join('\n')
  if (step.note.length > 0) result.note = step.note.join('\n')
  if (step.caption.length > 0) result.caption = step.caption.join('\n')
  if (step.expect.length > 0) result.expect = step.expect.join('\n')
  if (step.highlight) result.highlight = step.highlight
  if (step.focus) result.focus = step.focus
  if (step.reveal) result.reveal = step.reveal
  if (step.run) result.run = step.run
  if (step.pause !== undefined) result.pause = step.pause
  if (step.wait) result.wait = true
  if (step.asserts.length > 0) result.assert = step.asserts
  if (step.show) result.show = step.show
  if (step.hideOutput) result.hideOutput = true
  if (step.layout) result.layout = step.layout

  return result
}

function trimBlankLines(lines: string[]): string[] {
  let start = 0
  while (start < lines.length && lines[start]!.trim() === '') start++
  let end = lines.length - 1
  while (end >= start && lines[end]!.trim() === '') end--
  return lines.slice(start, end + 1)
}

function parseFrontmatter(
  lines: string[],
  setCursor: (pos: number) => void
): TourMeta {
  if (lines.length === 0 || lines[0]!.trim() !== '---') {
    setCursor(0)
    return {}
  }

  let end = -1
  for (let i = 1; i < lines.length; i++) {
    if (lines[i]!.trim() === '---') {
      end = i
      break
    }
  }

  if (end === -1) {
    // No closing fence — treat entire file as body
    setCursor(0)
    return {}
  }

  setCursor(end + 1)
  const yamlLines = lines.slice(1, end)
  return parseSimpleYaml(yamlLines)
}

/** Minimal YAML parser — handles flat key: value and key: [a, b, c]. */
function parseSimpleYaml(lines: string[]): TourMeta {
  const meta: TourMeta = {}

  for (const line of lines) {
    const match = line.match(/^(\w+)\s*:\s*(.+)$/)
    if (!match) continue
    const key = match[1]!
    let value = match[2]!.trim()

    // Strip surrounding quotes
    if ((value.startsWith('"') && value.endsWith('"')) ||
        (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1)
    }

    switch (key) {
      case 'title':
        meta.title = value
        break
      case 'description':
        meta.description = value
        break
      case 'author':
        meta.author = value
        break
      case 'difficulty':
        if (value === 'beginner' || value === 'intermediate' || value === 'advanced') {
          meta.difficulty = value
        }
        break
      case 'tags': {
        // Parse [a, b, c]
        const arrMatch = value.match(/^\[(.+)\]$/)
        if (arrMatch) {
          meta.tags = arrMatch[1]!.split(',').map((s) => s.trim())
        }
        break
      }
      case 'version':
        meta.version = parseInt(value, 10) || undefined
        break
    }
  }

  return meta
}
