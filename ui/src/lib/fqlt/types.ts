/** Metadata from YAML frontmatter at the top of an .fqlt file. */
export interface TourMeta {
  title?: string
  description?: string
  author?: string
  difficulty?: 'beginner' | 'intermediate' | 'advanced'
  tags?: string[]
  version?: number
}

/** A machine-checkable assertion on a query result variable. */
export interface TourAssert {
  variable: string
  operator: '=' | 'contains'
  expected: string
}

/** A single step in a tour — narrative + code + presentation directives. */
export interface TourStep {
  title: string
  text?: string
  note?: string
  caption?: string
  code: string
  highlight?: string[]
  focus?: string
  reveal?: 'instant' | 'typewriter' | 'line-by-line'
  run?: 'auto' | 'click' | 'skip'
  pause?: number
  wait?: boolean
  expect?: string
  assert?: TourAssert[]
  show?: string[]
  hideOutput?: boolean
  layout?: 'stacked' | 'split' | 'full-code'
}

/** A parsed .fqlt tour file. */
export interface Tour {
  meta: TourMeta
  steps: TourStep[]
}
