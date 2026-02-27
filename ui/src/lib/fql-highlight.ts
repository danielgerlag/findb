/**
 * FQL syntax highlighter — returns HTML with <span> tokens for styling.
 *
 * Token classes:
 *   .fql-keyword    — statement/clause keywords (CREATE, GET, FOR, AS …)
 *   .fql-type       — account types (ASSET, LIABILITY …)
 *   .fql-account    — @references
 *   .fql-string     — single-quoted strings
 *   .fql-number     — integers, decimals, percentages
 *   .fql-date       — date literals (YYYY-MM-DD)
 *   .fql-operator   — comparison/arithmetic operators
 *   .fql-comment    — -- line comments
 *   .fql-function   — function calls like balance(…)
 *   .fql-param      — $param placeholders
 *   .fql-punctuation — ; , ( )
 *   .fql-bool       — TRUE, FALSE, NULL
 */

const KEYWORDS = new Set([
  'CREATE', 'GET', 'SET', 'ACCRUE', 'BEGIN', 'COMMIT', 'ROLLBACK',
  'ACCOUNT', 'JOURNAL', 'RATE', 'BALANCE',
  'DEBIT', 'CREDIT',
  'COMPOUND', 'DAILY', 'CONTINUOUS',
  'FOR', 'FROM', 'TO', 'BY', 'INTO', 'WHERE', 'WITH', 'RETURN', 'AS', 'IN',
  'AND', 'OR', 'NOT', 'IS', 'EXISTS',
  'CASE', 'WHEN', 'THEN', 'ELSE', 'END',
  'ID', 'LABEL',
])

const TYPES = new Set([
  'ASSET', 'LIABILITY', 'INCOME', 'EXPENSE', 'EQUITY',
])

const BOOLS = new Set(['TRUE', 'FALSE', 'NULL'])

// Built-in function names that appear before (
const FUNCTIONS = new Set([
  'balance', 'trial_balance', 'statement', 'journal_count',
  'account_count', 'rate',
])

// Order matters — longer patterns first, most specific first.
// Each rule: [regex, className]
const RULES: [RegExp, string][] = [
  // Comments (must be first — everything after -- is comment)
  [/--(?!@).*/, 'fql-comment'],
  // Strings (single-quoted, may contain escaped quotes)
  [/'(?:[^'\\]|\\.)*'/, 'fql-string'],
  // Account references
  [/@[a-zA-Z_]\w*/, 'fql-account'],
  // Parameter placeholders
  [/\$[a-zA-Z_]\w*/, 'fql-param'],
  // Date literals (YYYY-MM-DD)
  [/\b\d{4}-\d{2}-\d{2}\b/, 'fql-date'],
  // Numbers: decimal, integer, percentage
  [/-?\b\d+(?:\.\d+)?%/, 'fql-number'],
  [/-?\b\d+\.\d+\b/, 'fql-number'],
  [/\b\d+\b/, 'fql-number'],
  // Multi-char operators
  [/[<>!]=|<>/, 'fql-operator'],
  // Single-char operators
  [/[+\-*/%^=<>]/, 'fql-operator'],
  // Punctuation
  [/[;,()[\]]/, 'fql-punctuation'],
  // Words — classified after extraction
  [/[a-zA-Z_]\w*/, '_word'],
]

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
}

/**
 * Highlight a single line of FQL, returning an HTML string.
 */
export function highlightFql(line: string): string {
  const parts: string[] = []
  let pos = 0

  while (pos < line.length) {
    // Skip whitespace — preserve it verbatim
    if (line[pos] === ' ' || line[pos] === '\t') {
      let end = pos
      while (end < line.length && (line[end] === ' ' || line[end] === '\t')) end++
      parts.push(line.slice(pos, end))
      pos = end
      continue
    }

    let matched = false
    for (const [re, cls] of RULES) {
      // Anchor the regex at current position
      const anchored = new RegExp(re.source, 'y')
      anchored.lastIndex = pos
      const m = anchored.exec(line)
      if (m) {
        const text = m[0]
        if (cls === '_word') {
          // Classify the word
          const upper = text.toUpperCase()
          // Check if it's a function call: word followed by (
          const afterWord = line.slice(pos + text.length).trimStart()
          if (FUNCTIONS.has(text) || afterWord.startsWith('(') && /^[a-z]/.test(text)) {
            parts.push(`<span class="fql-function">${escapeHtml(text)}</span>`)
          } else if (KEYWORDS.has(upper)) {
            parts.push(`<span class="fql-keyword">${escapeHtml(text)}</span>`)
          } else if (TYPES.has(upper)) {
            parts.push(`<span class="fql-type">${escapeHtml(text)}</span>`)
          } else if (BOOLS.has(upper)) {
            parts.push(`<span class="fql-bool">${escapeHtml(text)}</span>`)
          } else {
            parts.push(escapeHtml(text))
          }
        } else {
          parts.push(`<span class="${cls}">${escapeHtml(text)}</span>`)
        }
        pos += text.length
        matched = true
        break
      }
    }

    if (!matched) {
      // Unknown character — emit escaped
      parts.push(escapeHtml(line[pos]!))
      pos++
    }
  }

  return parts.join('')
}

/**
 * Highlight a complete FQL code block (multi-line).
 * Returns an array of HTML strings, one per line.
 */
export function highlightFqlLines(code: string): string[] {
  return code.split('\n').map(highlightFql)
}
