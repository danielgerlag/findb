<template>
  <div>
    <div class="page-header">
      <h1>Reports</h1>
    </div>

    <div class="toolbar">
      <div class="form-row">
        <div class="form-field" v-if="activeTab === 1">
          <label>From Date</label>
          <DatePicker v-model="fromDate" dateFormat="yy-mm-dd" showIcon />
        </div>
        <div class="form-field">
          <label>{{ activeTab === 0 ? 'Effective Date' : 'To Date' }}</label>
          <DatePicker v-model="asOfDate" dateFormat="yy-mm-dd" showIcon />
        </div>
      </div>
    </div>

    <TabView v-model:activeIndex="activeTab">
      <TabPanel header="Balance Sheet" value="0">
        <div class="report-card card">
          <div v-if="loading" class="report-loading">Loading report data…</div>
          <div v-else-if="error" class="error-msg">{{ error }}</div>
          <div v-else-if="!hasBalanceSheetData" class="empty-state">
            No balance sheet data available. Create accounts and journals first.
          </div>
          <div v-else class="report-body">
            <h2 class="report-title">Balance Sheet &mdash; {{ fmtDate(asOfDate) }}</h2>

            <div class="report-section">
              <div class="section-header">Assets</div>
              <DataTable :value="assets" size="small">
                <template #empty><div class="empty-section">No asset accounts</div></template>
                <Column field="account_id" header="Account" />
                <Column field="balance" header="Balance" style="text-align: right; width: 180px">
                  <template #body="{ data }">
                    <span :class="{ negative: parseFloat(data.balance) < 0 }">
                      {{ formatMoney(data.balance) }}
                    </span>
                  </template>
                </Column>
              </DataTable>
              <div class="subtotal-row">
                <span>Total Assets</span>
                <span>{{ formatMoney(totalAssets.toString()) }}</span>
              </div>
            </div>

            <div class="report-section">
              <div class="section-header">Liabilities</div>
              <DataTable :value="liabilities" size="small">
                <template #empty><div class="empty-section">No liability accounts</div></template>
                <Column field="account_id" header="Account" />
                <Column field="balance" header="Balance" style="text-align: right; width: 180px">
                  <template #body="{ data }">
                    <span :class="{ negative: parseFloat(data.balance) < 0 }">
                      {{ formatMoney(data.balance) }}
                    </span>
                  </template>
                </Column>
              </DataTable>
              <div class="subtotal-row">
                <span>Total Liabilities</span>
                <span>{{ formatMoney(totalLiabilities.toString()) }}</span>
              </div>
            </div>

            <div class="report-section">
              <div class="section-header">Equity</div>
              <DataTable :value="equityAccounts" size="small">
                <template #empty><div class="empty-section">No equity accounts</div></template>
                <Column field="account_id" header="Account" />
                <Column field="balance" header="Balance" style="text-align: right; width: 180px">
                  <template #body="{ data }">
                    <span :class="{ negative: parseFloat(data.balance) < 0 }">
                      {{ formatMoney(data.balance) }}
                    </span>
                  </template>
                </Column>
              </DataTable>
              <div class="retained-earnings-row" v-if="retainedEarnings !== 0">
                <span>Retained Earnings</span>
                <span :class="{ negative: retainedEarnings < 0 }">
                  {{ formatMoney(retainedEarnings.toString()) }}
                </span>
              </div>
              <div class="subtotal-row">
                <span>Total Equity</span>
                <span>{{ formatMoney(totalEquity.toString()) }}</span>
              </div>
            </div>

            <div class="report-grand-total">
              <div class="grand-total-row">
                <span>Total Assets</span>
                <span>{{ formatMoney(totalAssets.toString()) }}</span>
              </div>
              <div class="grand-total-row">
                <span>Total Liabilities + Equity</span>
                <span>{{ formatMoney((totalLiabilities + totalEquity).toString()) }}</span>
              </div>
            </div>
          </div>
        </div>
      </TabPanel>

      <TabPanel header="Income Statement" value="1">
        <div class="report-card card">
          <div v-if="loading" class="report-loading">Loading report data…</div>
          <div v-else-if="error" class="error-msg">{{ error }}</div>
          <div v-else-if="!hasIncomeStatementData" class="empty-state">
            No income statement data available. Create accounts and journals first.
          </div>
          <div v-else class="report-body">
            <h2 class="report-title">Income Statement &mdash; {{ fmtDate(fromDate) }} to {{ fmtDate(asOfDate) }}</h2>

            <div class="report-section">
              <div class="section-header">Revenue</div>
              <DataTable :value="revenue" size="small">
                <template #empty><div class="empty-section">No revenue accounts</div></template>
                <Column field="account_id" header="Account" />
                <Column field="balance" header="Amount" style="text-align: right; width: 180px">
                  <template #body="{ data }">
                    <span :class="{ negative: parseFloat(data.balance) < 0 }">
                      {{ formatMoney(data.balance) }}
                    </span>
                  </template>
                </Column>
              </DataTable>
              <div class="subtotal-row">
                <span>Total Revenue</span>
                <span>{{ formatMoney(totalRevenue.toString()) }}</span>
              </div>
            </div>

            <div class="report-section">
              <div class="section-header">Expenses</div>
              <DataTable :value="expenses" size="small">
                <template #empty><div class="empty-section">No expense accounts</div></template>
                <Column field="account_id" header="Account" />
                <Column field="balance" header="Amount" style="text-align: right; width: 180px">
                  <template #body="{ data }">
                    <span :class="{ negative: parseFloat(data.balance) < 0 }">
                      {{ formatMoney(data.balance) }}
                    </span>
                  </template>
                </Column>
              </DataTable>
              <div class="subtotal-row">
                <span>Total Expenses</span>
                <span>{{ formatMoney(totalExpenses.toString()) }}</span>
              </div>
            </div>

            <div class="report-grand-total" v-if="netIncomeItem">
              <div class="grand-total-row">
                <span>Net Income</span>
                <span :class="{ negative: parseFloat(netIncomeItem.balance) < 0 }">
                  {{ formatMoney(netIncomeItem.balance) }}
                </span>
              </div>
            </div>
          </div>
        </div>
      </TabPanel>
    </TabView>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import { useEntityStore } from '../stores/entity'
import { executeFqlV1, type TrialBalanceItemDto } from '../api/client'
import DataTable from 'primevue/datatable'
import Column from 'primevue/column'
import DatePicker from 'primevue/datepicker'
import TabView from 'primevue/tabview'
import TabPanel from 'primevue/tabpanel'

const entityStore = useEntityStore()

const activeTab = ref(0)
const asOfDate = ref(new Date())
const fromDate = ref(new Date(new Date().getFullYear(), 0, 1))
const loading = ref(false)
const error = ref('')
const bsItems = ref<TrialBalanceItemDto[]>([])
const isItems = ref<TrialBalanceItemDto[]>([])

function fmtDate(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

function formatMoney(val: string): string {
  if (!val) return ''
  const n = parseFloat(val)
  return isNaN(n) ? val : n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })
}

function sumBalances(items: TrialBalanceItemDto[]): number {
  return items.reduce((sum, i) => sum + parseFloat(i.balance || '0'), 0)
}

// Balance Sheet computed
const assets = computed(() => bsItems.value.filter(i => i.account_type === 'asset'))
const liabilities = computed(() => bsItems.value.filter(i => i.account_type === 'liability'))
const equityAccounts = computed(() => bsItems.value.filter(i => i.account_type === 'equity'))
const totalAssets = computed(() => sumBalances(assets.value))
const totalLiabilities = computed(() => sumBalances(liabilities.value))
const retainedEarnings = computed(() => {
  const incomeTotal = sumBalances(bsItems.value.filter(i => i.account_type === 'income'))
  const expenseTotal = sumBalances(bsItems.value.filter(i => i.account_type === 'expense'))
  return incomeTotal - expenseTotal
})
const totalEquity = computed(() => sumBalances(equityAccounts.value) + retainedEarnings.value)
const hasBalanceSheetData = computed(() => bsItems.value.length > 0)

// Income Statement computed
const revenue = computed(() => isItems.value.filter(i => i.account_type === 'income' && i.account_id !== 'NET_INCOME'))
const expenses = computed(() => isItems.value.filter(i => i.account_type === 'expense' && i.account_id !== 'NET_INCOME'))
const netIncomeItem = computed(() => isItems.value.find(i => i.account_id === 'NET_INCOME'))
const totalRevenue = computed(() => sumBalances(revenue.value))
const totalExpenses = computed(() => sumBalances(expenses.value))
const hasIncomeStatementData = computed(() => isItems.value.length > 0)

async function fetchReport() {
  loading.value = true
  error.value = ''
  try {
    const asOf = fmtDate(asOfDate.value)
    const from = fmtDate(fromDate.value)
    const query = `GET trial_balance(${asOf}) AS bs, income_statement(${from}, ${asOf}) AS pnl`
    const resp = await executeFqlV1(query, entityStore.activeEntity)
    if (!resp.success) {
      error.value = resp.error || 'Failed to fetch report data'
      return
    }
    const bsVal = resp.results.find(r => r.name === 'bs')?.value
    bsItems.value = bsVal?.type === 'trial_balance' ? bsVal.value : []
    const isVal = resp.results.find(r => r.name === 'pnl')?.value
    isItems.value = isVal?.type === 'trial_balance' ? isVal.value : []
  } catch (e: any) {
    error.value = e.message || 'An error occurred'
  } finally {
    loading.value = false
  }
}

onMounted(() => fetchReport())
watch(() => entityStore.activeEntity, () => fetchReport())
watch([asOfDate, fromDate], () => fetchReport())
</script>

<style scoped>
.form-row {
  display: flex;
  gap: 1rem;
  align-items: flex-end;
}

.report-card {
  margin-top: 0.5rem;
}

.report-title {
  text-align: center;
  font-size: 1.15rem;
  font-weight: 600;
  color: var(--text-heading, var(--text));
  margin-bottom: 1.5rem;
}

.report-body {
  padding: 0.5rem 0;
}

.report-section {
  margin-bottom: 1.5rem;
}

.section-header {
  font-size: 1.05rem;
  font-weight: 700;
  color: var(--text);
  padding: 0.5rem 1rem;
  border-top: 2px solid var(--border);
  margin-bottom: 0.25rem;
}

.report-section:first-child .section-header {
  border-top: none;
}

.subtotal-row {
  display: flex;
  justify-content: space-between;
  padding: 0.5rem 1rem;
  font-weight: 700;
  border-top: 2px solid var(--border);
  color: var(--text);
}

.retained-earnings-row {
  display: flex;
  justify-content: space-between;
  padding: 0.4rem 1rem;
  color: var(--text-muted);
  font-style: italic;
  border-top: 1px solid color-mix(in srgb, var(--border) 40%, transparent);
}

.report-grand-total {
  margin-top: 1rem;
  border-top: 3px double var(--accent);
  padding-top: 0.75rem;
}

.grand-total-row {
  display: flex;
  justify-content: space-between;
  padding: 0.4rem 1rem;
  font-weight: 700;
  font-size: 1.05rem;
  color: var(--text);
}

.negative {
  color: var(--error-text, #fca5a5);
}

.report-loading {
  padding: 2rem;
  text-align: center;
  color: var(--text-muted);
}

.empty-state {
  padding: 2rem;
  text-align: center;
  color: var(--text-muted);
}

.empty-section {
  color: var(--text-muted);
  font-style: italic;
  padding: 0.5rem 0;
}

:deep(.p-datatable) {
  background: transparent;
}

:deep(.p-datatable-thead > tr > th) {
  background: transparent !important;
  border: none !important;
  color: var(--text-muted);
  font-size: 0.8rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  padding: 0.4rem 1rem;
}

:deep(.p-datatable-thead > tr > th:last-child) {
  text-align: right;
}

:deep(.p-datatable-tbody > tr > td) {
  background: transparent !important;
  border: none !important;
  border-bottom: 1px solid color-mix(in srgb, var(--border) 40%, transparent) !important;
  padding: 0.4rem 1rem;
  color: var(--text);
}

:deep(.p-datatable-tbody > tr:last-child > td) {
  border-bottom: none !important;
}

:deep(.p-datatable-tbody > tr > td:last-child) {
  text-align: right;
}

:deep(.p-datatable-tbody > tr:hover > td) {
  background: color-mix(in srgb, var(--accent) 6%, transparent) !important;
}
</style>
