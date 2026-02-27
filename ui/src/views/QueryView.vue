<template>
  <div>
    <div class="page-header">
      <h1>FQL Query Editor</h1>
    </div>

    <div class="card fql-editor">
      <textarea
        v-model="query"
        placeholder="Enter FQL statements...&#10;&#10;Examples:&#10;  CREATE ACCOUNT @bank ASSET;&#10;  GET balance(@bank, 2023-12-31) AS result;&#10;  GET trial_balance(2023-12-31) AS tb;"
        @keydown.ctrl.enter="executeQuery"
      ></textarea>
      <div class="toolbar">
        <Button label="Execute" icon="pi pi-play" @click="executeQuery" :loading="loading" />
        <Button label="Clear" icon="pi pi-trash" severity="secondary" @click="clearResults" />
        <span style="color: #94a3b8; font-size: 0.85rem;">Ctrl+Enter to execute</span>
      </div>
    </div>

    <div v-if="error" class="error-msg">{{ error }}</div>

    <div v-if="executed && !error" class="card">
      <h3>Results ({{ metadata.statements_executed }} statements, {{ metadata.journals_created }} journals)</h3>
      <div v-if="results.length > 0">
        <div v-for="(result, i) in results" :key="i" style="margin-bottom: 1rem;">
          <pre style="background: #f1f5f9; padding: 0.75rem; border-radius: 6px; overflow-x: auto; font-size: 0.85rem;">{{ result }}</pre>
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
import { ref, onMounted } from 'vue'
import { executeFql } from '../api/client'
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
