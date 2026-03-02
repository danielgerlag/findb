import { defineConfig } from 'vitepress'

export default defineConfig({
  title: 'DblEntry',
  description: 'A Layer 2 database for double-entry bookkeeping',
  themeConfig: {
    nav: [
      { text: 'Guide', link: '/guide/getting-started' },
      { text: 'FQL Reference', link: '/reference/statements' },
      { text: 'Functions', link: '/reference/functions' },
    ],
    sidebar: [
      {
        text: 'Guide',
        items: [
          { text: 'Getting Started', link: '/guide/getting-started' },
          { text: 'Core Concepts', link: '/guide/core-concepts' },
          { text: 'Entities', link: '/guide/entities' },
          { text: 'Transactions', link: '/guide/transactions' },
        ],
      },
      {
        text: 'FQL Reference',
        items: [
          { text: 'Statements', link: '/reference/statements' },
          { text: 'Functions', link: '/reference/functions' },
          { text: 'Expressions & Operators', link: '/reference/expressions' },
          { text: 'Grammar', link: '/reference/grammar' },
        ],
      },
      {
        text: 'Cookbook',
        items: [
          { text: 'Lending Fund', link: '/cookbook/lending-fund' },
          { text: 'Multi-Currency', link: '/cookbook/multi-currency' },
        ],
      },
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/danielgerlag/dblentry' },
    ],
  },
})
