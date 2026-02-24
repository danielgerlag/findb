import { defineStore } from 'pinia'
import { ref } from 'vue'
import { executeFql, parseTrialBalance, parseScalar, type TrialBalanceItem } from '../api/client'

export const useFindbStore = defineStore('findb', () => {
  const trialBalance = ref<TrialBalanceItem[]>([])
  const accountCount = ref(0)
  const loading = ref(false)
  const error = ref<string | null>(null)
  const effectiveDate = ref(new Date().toISOString().slice(0, 10))

  async function fetchDashboard() {
    loading.value = true
    error.value = null
    try {
      const date = effectiveDate.value
      const resp = await executeFql(
        `GET trial_balance(${date}) AS tb, account_count() AS count`
      )
      if (!resp.success) {
        error.value = resp.error || 'Unknown error'
        return
      }
      for (const result of resp.results) {
        const tb = parseTrialBalance(result)
        if (tb.length > 0) {
          trialBalance.value = tb
        }
        const scalars = parseScalar(result)
        if (scalars['count']) {
          const parsed = parseInt(scalars['count'], 10)
          if (!isNaN(parsed)) {
            accountCount.value = parsed
          }
        }
      }
    } catch (e: any) {
      error.value = e.message
    } finally {
      loading.value = false
    }
  }

  return { trialBalance, accountCount, loading, error, effectiveDate, fetchDashboard }
})
