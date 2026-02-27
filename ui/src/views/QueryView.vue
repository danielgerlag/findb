<template>
  <div>
    <div class="page-header">
      <h1>FQL Query Editor</h1>
    </div>

    <div class="card fql-editor">
      <div class="editor-wrapper">
        <pre class="editor-highlight" aria-hidden="true"><code><span v-for="(line, i) in highlightedLines" :key="i" v-html="line + '\n'"></span><span>&nbsp;</span></code></pre>
        <textarea
          v-model="query"
          placeholder="Enter FQL statements...&#10;&#10;Examples:&#10;  CREATE ACCOUNT @bank ASSET;&#10;  GET balance(@bank, 2023-12-31) AS result;&#10;  GET trial_balance(2023-12-31) AS tb;"
          @keydown.ctrl.enter="executeQuery"
          @scroll="syncScroll"
          ref="textareaEl"
          spellcheck="false"
        ></textarea>
      </div>
      <div class="toolbar">
        <Button label="Execute" icon="pi pi-play" @click="executeQuery" :loading="loading" />
        <Button label="Clear" icon="pi pi-trash" severity="secondary" @click="clearResults" />
        <span style="color: var(--text-muted); font-size: 0.85rem;">Ctrl+Enter to execute</span>
      </div>
    </div>

    <div v-if="error" class="error-msg">{{ error }}</div>

    <div v-if="executed && !error" class="card">
      <h3>Results ({{ metadata.statements_executed }} statements, {{ metadata.journals_created }} journals)</h3>
      <div v-if="results.length > 0">
        <div v-for="(result, i) in results" :key="i" style="margin-bottom: 1rem;">
          <pre style="background: var(--bg-surface); padding: 0.75rem; border-radius: 6px; overflow-x: auto; font-size: 0.85rem; border: 1px solid var(--border);">{{ result }}</pre>
        </div>
      </div>
      <div v-else class="success-msg">
        ✓ Executed {{ metadata.statements_executed }} statement{{ metadata.statements_executed !== 1 ? 's' : '' }} successfully<span v-if="metadata.journals_created > 0"> — {{ metadata.journals_created }} journal{{ metadata.journals_created !== 1 ? 's' : '' }} created</span>
      </div>
    </div>

    <div v-if="history.length > 0" class="card">
      <h3>Recent Queries</h3>
      <div v-for="(item, i) in history" :key="i" style="margin-bottom: 0.5rem;">
        <Button :label="item.slice(0, 80) + (item.length > 80 ? '...' : '')" severity="secondary" text size="small" @click="query = item" />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { executeFql } from '../api/client'
import { highlightFqlLines } from '../lib/fql-highlight'
import Button from 'primevue/button'
import { useToast } from 'primevue/usetoast'

const toast = useToast()
const query = ref('')
const results = ref<string[]>([])
const error = ref<string | null>(null)
const loading = ref(false)
const executed = ref(false)
const metadata = ref({ statements_executed: 0, journals_created: 0 })
const history = ref<string[]>([])
const textareaEl = ref<HTMLTextAreaElement | null>(null)

const highlightedLines = computed(() => highlightFqlLines(query.value))

function syncScroll(e: Event) {
  const ta = e.target as HTMLTextAreaElement
  const pre = ta.previousElementSibling as HTMLElement
  if (pre) {
    pre.scrollTop = ta.scrollTop
    pre.scrollLeft = ta.scrollLeft
  }
}

onMounted(() => {
  const saved = localStorage.getItem('dblentry-query-history')
  if (saved) history.value = JSON.parse(saved)
})

async function executeQuery() {
  if (!query.value.trim()) return
  loading.value = true
  error.value = null
  executed.value = false
  results.value = []

  try {
    const resp = await executeFql(query.value)
    metadata.value = resp.metadata
    if (resp.success) {
      results.value = (resp.results || []).filter((r) => r.trim().length > 0)
      executed.value = true
    } else {
      error.value = resp.error || 'Unknown error'
    }

    // Save to history
    const q = query.value.trim()
    history.value = [q, ...history.value.filter((h) => h !== q)].slice(0, 20)
    localStorage.setItem('dblentry-query-history', JSON.stringify(history.value))
  } catch (e: any) {
    error.value = e.message
    toast.add({ severity: 'error', summary: 'Connection error', detail: 'Could not reach DblEntry server. Is it running?', life: 5000 })
  } finally {
    loading.value = false
  }
}

function clearResults() {
  results.value = []
  error.value = null
  executed.value = false
}
</script>

<style scoped>
.editor-wrapper {
  position: relative;
}
.editor-highlight {
  position: absolute;
  top: 0; left: 0; right: 0; bottom: 0;
  margin: 0;
  padding: 1rem;
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  font-size: 0.9rem;
  line-height: 1.5;
  background: var(--bg-input);
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: auto;
  pointer-events: none;
  white-space: pre-wrap;
  word-wrap: break-word;
  color: #e2e8f0;
}
.editor-highlight code {
  font-family: inherit;
  font-size: inherit;
}
.fql-editor textarea {
  position: relative;
  color: transparent;
  caret-color: var(--accent);
  background: transparent;
  z-index: 1;
  line-height: 1.5;
}
/* FQL syntax colors */
:deep(.fql-keyword) { color: #c084fc; font-weight: 500; }
:deep(.fql-type) { color: #67e8f9; font-weight: 500; }
:deep(.fql-account) { color: #34d399; }
:deep(.fql-string) { color: #fbbf24; }
:deep(.fql-number) { color: #fb923c; }
:deep(.fql-date) { color: #60a5fa; }
:deep(.fql-operator) { color: #94a3b8; }
:deep(.fql-comment) { color: #64748b; font-style: italic; }
:deep(.fql-function) { color: #38bdf8; }
:deep(.fql-param) { color: #f472b6; }
:deep(.fql-punctuation) { color: #94a3b8; }
:deep(.fql-bool) { color: #fb923c; font-weight: 500; }
</style>
