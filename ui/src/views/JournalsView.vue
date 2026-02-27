<template>
  <div>
    <div class="page-header">
      <h1>Create Journal</h1>
    </div>

    <div class="card">
      <div class="form-row">
        <div class="form-field">
          <label>Date</label>
          <DatePicker v-model="journalDate" dateFormat="yy-mm-dd" showIcon />
        </div>
        <div class="form-field">
          <label>Amount</label>
          <InputText v-model="amount" placeholder="1000" />
        </div>
        <div class="form-field" style="flex: 2;">
          <label>Description</label>
          <InputText v-model="description" placeholder="Investment" />
        </div>
      </div>

      <!-- Dimensions -->
      <h3 style="margin: 1rem 0 0.5rem;">Dimensions</h3>
      <div v-for="(dim, i) in dimensions" :key="i" class="form-row">
        <div class="form-field">
          <label>Key</label>
          <InputText v-model="dim.key" placeholder="Customer" />
        </div>
        <div class="form-field">
          <label>Value</label>
          <InputText v-model="dim.value" placeholder="Acme" />
        </div>
        <Button icon="pi pi-trash" severity="danger" text @click="dimensions.splice(i, 1)" />
      </div>
      <Button label="Add Dimension" icon="pi pi-plus" severity="secondary" text size="small" @click="dimensions.push({ key: '', value: '' })" />

      <!-- Ledger Entries -->
      <h3 style="margin: 1rem 0 0.5rem;">Ledger Entries</h3>
      <div v-for="(entry, i) in entries" :key="i" class="form-row">
        <div class="form-field">
          <label>Type</label>
          <Select v-model="entry.op_type" :options="['CREDIT', 'DEBIT']" />
        </div>
        <div class="form-field">
          <label>Account</label>
          <InputText v-model="entry.account" placeholder="bank" />
        </div>
        <div class="form-field">
          <label>Amount (optional)</label>
          <InputText v-model="entry.amount" placeholder="Uses journal amount" />
        </div>
        <Button icon="pi pi-trash" severity="danger" text @click="entries.splice(i, 1)" />
      </div>
      <Button label="Add Entry" icon="pi pi-plus" severity="secondary" text size="small"
              @click="entries.push({ op_type: 'DEBIT', account: '', amount: '' })" />

      <!-- FQL Preview -->
      <div v-if="fqlPreview" style="margin-top: 1rem;">
        <h3>Generated FQL</h3>
        <pre class="fql-preview"><code><span v-for="(line, i) in highlightedPreview" :key="i" v-html="line + '\n'"></span></code></pre>
      </div>

      <div class="toolbar" style="margin-top: 1rem;">
        <Button label="Create Journal" icon="pi pi-check" @click="submit" :loading="loading" />
        <Button label="Reset" icon="pi pi-undo" severity="secondary" @click="reset" />
      </div>
    </div>

    <div v-if="error" class="error-msg">{{ error }}</div>
    <div v-if="success" class="success-msg">
      <p>✓ Journal created successfully</p>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { executeFql, escapeFql } from '../api/client'
import { highlightFqlLines } from '../lib/fql-highlight'
import Button from 'primevue/button'
import InputText from 'primevue/inputtext'
import Select from 'primevue/select'
import DatePicker from 'primevue/datepicker'
import { useToast } from 'primevue/usetoast'

const toast = useToast()
const journalDate = ref(new Date())
const amount = ref('')
const description = ref('')
const dimensions = ref<{ key: string; value: string }[]>([])
const entries = ref<{ op_type: string; account: string; amount: string }[]>([
  { op_type: 'CREDIT', account: '', amount: '' },
  { op_type: 'DEBIT', account: '', amount: '' },
])
const loading = ref(false)
const error = ref<string | null>(null)
const success = ref(false)

function formatDate(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

const fqlPreview = computed(() => {
  if (!amount.value || !description.value || entries.value.length === 0) return ''
  const date = formatDate(journalDate.value)
  let fql = `CREATE JOURNAL ${date}, ${amount.value}, '${escapeFql(description.value)}'`

  const dims = dimensions.value.filter((d) => d.key && d.value)
  if (dims.length > 0) {
    fql += `\nFOR ${dims.map((d) => `${d.key}='${escapeFql(d.value)}'`).join(', ')}`
  }

  const ops = entries.value
    .filter((e) => e.account)
    .map((e) => {
      let s = `${e.op_type} @${e.account}`
      if (e.amount) s += ` ${e.amount}`
      return s
    })
  fql += `\n${ops.join(',\n')};`
  return fql
})

const highlightedPreview = computed(() => highlightFqlLines(fqlPreview.value))

async function submit() {
  if (!fqlPreview.value) return
  loading.value = true
  error.value = null
  success.value = false

  try {
    const resp = await executeFql(fqlPreview.value)
    if (resp.success) {
      success.value = true
      toast.add({ severity: 'success', summary: 'Journal created', detail: `${resp.metadata.journals_created} journal(s) created`, life: 3000 })
      reset()
      success.value = true
    } else {
      error.value = resp.error || 'Unknown error'
      toast.add({ severity: 'error', summary: 'Journal error', detail: resp.error || 'Unknown error', life: 5000 })
    }
  } catch (e: any) {
    error.value = e.message
    toast.add({ severity: 'error', summary: 'Request failed', detail: e.message, life: 5000 })
  } finally {
    loading.value = false
  }
}

function reset() {
  amount.value = ''
  description.value = ''
  dimensions.value = []
  entries.value = [
    { op_type: 'CREDIT', account: '', amount: '' },
    { op_type: 'DEBIT', account: '', amount: '' },
  ]
  error.value = null
  success.value = false
}
</script>

<style scoped>
.fql-preview {
  background: #0f172a;
  padding: 0.75rem;
  border-radius: 6px;
  font-size: 0.85rem;
  border: 1px solid var(--border);
  font-family: 'JetBrains Mono', 'Fira Code', monospace;
  color: #e2e8f0;
  overflow-x: auto;
}
.fql-preview code { font-family: inherit; }
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
