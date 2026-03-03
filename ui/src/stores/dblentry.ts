import { defineStore } from 'pinia'
import { ref } from 'vue'
import { executeFqlV1, getResultValue, type TrialBalanceItemDto } from '../api/client'
import { useEntityStore } from './entity'

export const useDblEntryStore = defineStore('dblentry', () => {
  const trialBalance = ref<TrialBalanceItemDto[]>([])
  const accountCount = ref(0)
  const loading = ref(false)
  const error = ref<string | null>(null)
  const effectiveDate = ref(new Date().toISOString().slice(0, 10))

  async function fetchDashboard() {
    loading.value = true
    error.value = null
    try {
      const entityStore = useEntityStore()
      const date = effectiveDate.value
      const resp = await executeFqlV1(
        `GET trial_balance(${date}) AS tb, account_count() AS count`,
        entityStore.activeEntity
      )
      if (!resp.success) {
        error.value = resp.error || 'Unknown error'
        return
      }
      // Reset before parsing — ensures stale data is cleared
      trialBalance.value = []
      accountCount.value = 0

      const tbVal = getResultValue(resp, 'tb')
      if (tbVal && tbVal.type === 'trial_balance') {
        trialBalance.value = tbVal.value
      }

      const countVal = getResultValue(resp, 'count')
      if (countVal && countVal.type === 'int') {
        accountCount.value = countVal.value
      }
    } catch (e: any) {
      error.value = e.message
    } finally {
      loading.value = false
    }
  }

  return { trialBalance, accountCount, loading, error, effectiveDate, fetchDashboard }
})
