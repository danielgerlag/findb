const BASE = ''

export interface FqlResponse {
  success: boolean
  results: string[]
  error?: string
  metadata: {
    statements_executed: number
    journals_created: number
  }
}

export interface TrialBalanceItem {
  account_id: string
  debit: string
  credit: string
}

export interface StatementTxn {
  date: string
  description: string
  amount: string
  balance: string
}

async function safeJson<T>(res: Response): Promise<T> {
  const text = await res.text()
  if (!text) {
    throw new Error(`Server returned empty response (HTTP ${res.status})`)
  }
  try {
    return JSON.parse(text)
  } catch {
    throw new Error(`Server returned non-JSON response (HTTP ${res.status}): ${text.slice(0, 200)}`)
  }
}

export async function executeFql(query: string): Promise<FqlResponse> {
  const res = await fetch(`${BASE}/fql`, {
    method: 'POST',
    headers: { 'Content-Type': 'text/plain' },
    body: query,
  })
  return safeJson<FqlResponse>(res)
}

export async function getHealth(): Promise<{ status: string; version: string }> {
  const res = await fetch(`${BASE}/health`)
  return safeJson(res)
}

// Parse a box-drawing table from FQL output into rows of string arrays.
// Handles lines like: | Account | Debit | Credit |
// Skips separator lines: +---+---+---+
function parseBoxTable(text: string): { headers: string[]; rows: string[][] } {
  const lines = text.replace(/\r\n/g, '\n').trim().split('\n')
  const dataLines = lines.filter(
    (l) => l.includes('|') && !l.match(/^\+[-+]+\+$/)
  )
  if (dataLines.length === 0) return { headers: [], rows: [] }
  const parse = (line: string) =>
    line.split('|').map((s) => s.trim()).filter((_s, i, a) => i > 0 && i < a.length - 1)
  const headers = parse(dataLines[0]!)
  const rows = dataLines
    .slice(1)
    .map(parse)
    .filter((cells) => cells.some((c) => c.length > 0))
  return { headers, rows }
}

// Parse trial balance from FQL results string
// Output format: | Account | Debit | Credit |
export function parseTrialBalance(text: string): TrialBalanceItem[] {
  const { rows } = parseBoxTable(text)
  return rows.map((cells) => ({
    account_id: cells[0] || '',
    debit: cells[1] || '',
    credit: cells[2] || '',
  }))
}

// Parse statement from FQL results string
// Output format: | Date | Description | Amount | Balance |
export function parseStatement(text: string): StatementTxn[] {
  const { rows } = parseBoxTable(text)
  return rows.map((cells) => ({
    date: cells[0] || '',
    description: cells[1] || '',
    amount: cells[2] || '',
    balance: cells[3] || '',
  }))
}

// Parse a scalar value from FQL result like "varName: value\n"
export function parseScalar(text: string): Record<string, string> {
  const result: Record<string, string> = {}
  const lines = text.trim().split('\n')
  for (const line of lines) {
    const match = line.match(/^(\w+):\s*(.+)$/)
    if (match && match[1] && match[2]) {
      result[match[1]] = match[2].trim()
    }
  }
  return result
}

// REST API wrappers

export async function createAccount(id: string, accountType: string): Promise<any> {
  const res = await fetch(`${BASE}/api/accounts`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id, account_type: accountType }),
  })
  return safeJson(res)
}

export async function createRate(id: string): Promise<any> {
  const res = await fetch(`${BASE}/api/rates`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ id }),
  })
  return safeJson(res)
}

export async function setRate(id: string, value: string, date: string): Promise<any> {
  const res = await fetch(`${BASE}/api/rates/${id}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ rate: value, date }),
  })
  return safeJson(res)
}

export async function createJournal(req: {
  date: string
  amount: string
  description: string
  dimensions: Record<string, string>
  operations: { type: string; account: string; amount?: string }[]
}): Promise<any> {
  const res = await fetch(`${BASE}/api/journals`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(req),
  })
  return safeJson(res)
}
