<template>
  <div>
    <div class="page-header">
      <h1>Accounts</h1>
    </div>

    <div class="card">
      <div class="toolbar">
        <Button label="Create Account" icon="pi pi-plus" @click="showCreate = true" />
        <Button label="Refresh" icon="pi pi-refresh" severity="secondary" @click="loadAccounts" />
      </div>

      <DataTable :value="accounts" stripedRows size="small" :loading="loading"
                 selectionMode="single" v-model:selection="selectedAccount" @row-select="onAccountSelect">
        <template #empty>
          <div class="empty-state">No accounts found. Create one to get started.</div>
        </template>
        <Column field="account_id" header="Account" sortable />
        <Column field="debit" header="Debit" sortable>
          <template #body="{ data }">{{ formatMoney(data.debit) }}</template>
        </Column>
        <Column field="credit" header="Credit" sortable>
          <template #body="{ data }">{{ formatMoney(data.credit) }}</template>
        </Column>
      </DataTable>
    </div>

    <!-- Account Detail -->
    <div v-if="selectedAccount" class="card">
      <h3>{{ selectedAccount.account_id }}</h3>

      <div class="form-row">
        <div class="form-field">
          <label>From</label>
          <DatePicker v-model="stmtFrom" dateFormat="yy-mm-dd" showIcon />
        </div>
        <div class="form-field">
          <label>To</label>
          <DatePicker v-model="stmtTo" dateFormat="yy-mm-dd" showIcon />
        </div>
        <div class="form-field">
          <label>Dimension Key</label>
          <InputText v-model="dimKey" placeholder="e.g. Customer" />
        </div>
        <div class="form-field">
          <label>Dimension Value</label>
          <InputText v-model="dimValue" placeholder="e.g. Acme" />
        </div>
        <Button label="Load Statement" icon="pi pi-search" @click="loadStatement" />
      </div>

      <DataTable :value="statement" stripedRows size="small" v-if="statement.length > 0">
        <Column field="date" header="Date" />
        <Column field="description" header="Description" />
        <Column field="amount" header="Amount">
          <template #body="{ data }">{{ formatMoney(data.amount) }}</template>
        </Column>
        <Column field="balance" header="Balance">
          <template #body="{ data }">{{ formatMoney(data.balance) }}</template>
        </Column>
      </DataTable>
    </div>

    <!-- Create Dialog -->
    <Dialog v-model:visible="showCreate" header="Create Account" :modal="true" :style="{ width: '400px' }">
      <div class="form-field" style="margin-bottom: 0.75rem;">
        <label>Account ID</label>
        <InputText v-model="newId" placeholder="e.g. bank" />
      </div>
      <div class="form-field" style="margin-bottom: 0.75rem;">
        <label>Account Type</label>
        <Select v-model="newType" :options="accountTypes" placeholder="Select type" />
      </div>
      <template #footer>
        <Button label="Cancel" severity="secondary" @click="showCreate = false" />
        <Button label="Create" icon="pi pi-check" @click="doCreate" />
      </template>
    </Dialog>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { executeFql, parseTrialBalance, parseStatement, createAccount, type TrialBalanceItem, type StatementTxn } from '../api/client'
import DataTable from 'primevue/datatable'
import Column from 'primevue/column'
import Button from 'primevue/button'
import Dialog from 'primevue/dialog'
import InputText from 'primevue/inputtext'
import Select from 'primevue/select'
import DatePicker from 'primevue/datepicker'
import { useToast } from 'primevue/usetoast'

const toast = useToast()
const accounts = ref<TrialBalanceItem[]>([])
const loading = ref(false)
const selectedAccount = ref<TrialBalanceItem | null>(null)
const statement = ref<StatementTxn[]>([])
const showCreate = ref(false)
const newId = ref('')
const newType = ref('ASSET')
const accountTypes = ['ASSET', 'LIABILITY', 'EQUITY', 'INCOME', 'EXPENSE']
const stmtFrom = ref(new Date(new Date().getFullYear(), 0, 1))
const stmtTo = ref(new Date())
const dimKey = ref('')
const dimValue = ref('')

function formatMoney(val: string) {
  if (!val) return ''
  const n = parseFloat(val)
  return isNaN(n) ? val : n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function formatDate(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

async function loadAccounts() {
  loading.value = true
  try {
    const today = formatDate(new Date())
    const resp = await executeFql(`GET trial_balance(${today}) AS tb`)
    if (resp.success && resp.results.length > 0 && resp.results[0]) {
      accounts.value = parseTrialBalance(resp.results[0])
    }
  } catch (e: any) {
    toast.add({ severity: 'error', summary: 'Failed to load accounts', detail: e.message, life: 5000 })
  } finally {
    loading.value = false
  }
}

function onAccountSelect() {
  statement.value = []
}

async function loadStatement() {
  if (!selectedAccount.value) return
  const id = selectedAccount.value.account_id
  const from = formatDate(stmtFrom.value)
  const to = formatDate(stmtTo.value)
  let dim = ''
  if (dimKey.value && dimValue.value) {
    dim = `, ${dimKey.value}='${dimValue.value}'`
  }
  try {
    const resp = await executeFql(`GET statement(@${id}, ${from}, ${to}${dim}) AS stmt`)
    if (resp.success && resp.results.length > 0 && resp.results[0]) {
      statement.value = parseStatement(resp.results[0])
      if (statement.value.length === 0) {
        toast.add({ severity: 'info', summary: 'No transactions', detail: `No entries found for @${id} in this period`, life: 3000 })
      }
    } else if (!resp.success) {
      toast.add({ severity: 'error', summary: 'Statement error', detail: resp.error || 'Unknown error', life: 5000 })
    }
  } catch (e: any) {
    toast.add({ severity: 'error', summary: 'Failed to load statement', detail: e.message, life: 5000 })
  }
}

async function doCreate() {
  if (!newId.value) return
  try {
    await createAccount(newId.value, newType.value)
    toast.add({ severity: 'success', summary: 'Account created', detail: `@${newId.value} (${newType.value})`, life: 3000 })
    showCreate.value = false
    newId.value = ''
    await loadAccounts()
  } catch (e: any) {
    toast.add({ severity: 'error', summary: 'Error', detail: e.message, life: 5000 })
  }
}

onMounted(loadAccounts)
</script>
