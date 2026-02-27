<template>
  <div>
    <div class="page-header">
      <h1>Dashboard</h1>
    </div>

    <div class="toolbar">
      <div class="form-field">
        <label>Effective Date</label>
        <DatePicker v-model="dateModel" dateFormat="yy-mm-dd" showIcon @date-select="onDateChange" />
      </div>
    </div>

    <div class="kpi-grid">
      <div class="kpi-card">
        <div class="label">Total Accounts</div>
        <div class="value">{{ store.accountCount }}</div>
      </div>
      <div class="kpi-card">
        <div class="label">Total Debits</div>
        <div class="value">{{ totalDebits }}</div>
      </div>
      <div class="kpi-card">
        <div class="label">Total Credits</div>
        <div class="value">{{ totalCredits }}</div>
      </div>
    </div>

    <div class="grid-2">
      <div class="card">
        <h3>Trial Balance</h3>
        <DataTable :value="store.trialBalance" stripedRows size="small" :loading="store.loading">
          <template #empty>
            <div class="empty-state">No accounts yet. Create accounts and journals to see data here.</div>
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

      <div class="card">
        <h3>Balance Composition</h3>
        <Chart type="doughnut" :data="chartData" :options="chartOptions" v-if="chartData.labels.length > 0" />
        <p v-else style="color: var(--text-muted);">No data yet. Connect to a running DblEntry instance.</p>
      </div>
    </div>

    <div v-if="store.error" class="error-msg">{{ store.error }}</div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useDblEntryStore } from '../stores/dblentry'
import DataTable from 'primevue/datatable'
import Column from 'primevue/column'
import DatePicker from 'primevue/datepicker'
import Chart from 'primevue/chart'

const store = useDblEntryStore()
const dateModel = ref(new Date())

function formatMoney(val: string) {
  if (!val) return ''
  const n = parseFloat(val)
  return isNaN(n) ? val : n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function onDateChange() {
  if (dateModel.value) {
    const d = dateModel.value
    store.effectiveDate = `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
    store.fetchDashboard()
  }
}

const totalDebits = computed(() => {
  return formatMoney(
    store.trialBalance
      .reduce((sum, i) => sum + parseFloat(i.debit || '0'), 0)
      .toString()
  )
})

const totalCredits = computed(() => {
  return formatMoney(
    store.trialBalance
      .reduce((sum, i) => sum + parseFloat(i.credit || '0'), 0)
      .toString()
  )
})

const chartData = computed(() => {
  const items = store.trialBalance.filter(
    (i) => parseFloat(i.debit || '0') > 0 || parseFloat(i.credit || '0') > 0
  )
  return {
    labels: items.map((i) => i.account_id),
    datasets: [
      {
        data: items.map((i) => Math.abs(parseFloat(i.debit || '0') || parseFloat(i.credit || '0'))),
        backgroundColor: ['#38bdf8', '#f87171', '#34d399', '#fbbf24', '#a78bfa', '#fb923c', '#e879f9', '#22d3ee'],
      },
    ],
  }
})

const chartOptions = {
  plugins: {
    legend: { position: 'bottom' as const },
  },
}

onMounted(() => store.fetchDashboard())
</script>
