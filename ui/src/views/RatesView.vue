<template>
  <div>
    <div class="page-header">
      <h1>Rates</h1>
    </div>

    <div class="grid-2">
      <div class="card">
        <h3>Create Rate</h3>
        <div class="form-row">
          <div class="form-field" style="flex: 1;">
            <label>Rate ID</label>
            <InputText v-model="newRateId" placeholder="e.g. prime, usd_eur" />
          </div>
          <Button label="Create" icon="pi pi-plus" @click="doCreateRate" />
        </div>
        <div v-if="createMsg" :class="createMsg.startsWith('✓') ? 'card' : 'error-msg'" style="margin-top: 0.5rem;">
          {{ createMsg }}
        </div>
      </div>

      <div class="card">
        <h3>Set Rate Value</h3>
        <div class="form-row">
          <div class="form-field">
            <label>Rate ID</label>
            <InputText v-model="setRateId" placeholder="prime" />
          </div>
          <div class="form-field">
            <label>Value</label>
            <InputText v-model="setRateValue" placeholder="0.05" />
          </div>
          <div class="form-field">
            <label>Date</label>
            <DatePicker v-model="setRateDate" dateFormat="yy-mm-dd" showIcon />
          </div>
          <Button label="Set" icon="pi pi-check" @click="doSetRate" />
        </div>
        <div v-if="setMsg" :class="setMsg.startsWith('✓') ? 'card' : 'error-msg'" style="margin-top: 0.5rem;">
          {{ setMsg }}
        </div>
      </div>
    </div>

    <div class="card">
      <h3>Look Up Rate</h3>
      <div class="form-row">
        <div class="form-field">
          <label>Rate ID</label>
          <InputText v-model="lookupId" placeholder="usd_eur" />
        </div>
        <div class="form-field">
          <label>Date</label>
          <DatePicker v-model="lookupDate" dateFormat="yy-mm-dd" showIcon />
        </div>
        <Button label="Look Up" icon="pi pi-search" @click="doLookup" />
      </div>
      <div v-if="lookupResult !== null" class="card" style="margin-top: 0.5rem;">
        <strong>{{ lookupId }}</strong> at {{ formatDate(lookupDate) }}: <strong>{{ lookupResult }}</strong>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import { executeFql } from '../api/client'
import Button from 'primevue/button'
import InputText from 'primevue/inputtext'
import DatePicker from 'primevue/datepicker'
import { useToast } from 'primevue/usetoast'

const toast = useToast()
const newRateId = ref('')
const createMsg = ref('')
const setRateId = ref('')
const setRateValue = ref('')
const setRateDate = ref(new Date())
const setMsg = ref('')
const lookupId = ref('')
const lookupDate = ref(new Date())
const lookupResult = ref<string | null>(null)

function formatDate(d: Date): string {
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

async function doCreateRate() {
  if (!newRateId.value) return
  try {
    const resp = await executeFql(`CREATE RATE ${newRateId.value};`)
    createMsg.value = resp.success ? `✓ Rate "${newRateId.value}" created` : resp.error || 'Error'
    if (resp.success) toast.add({ severity: 'success', summary: 'Rate created', detail: newRateId.value, life: 3000 })
  } catch (e: any) {
    createMsg.value = e.message
    toast.add({ severity: 'error', summary: 'Failed to create rate', detail: e.message, life: 5000 })
  }
}

async function doSetRate() {
  if (!setRateId.value || !setRateValue.value) return
  const date = formatDate(setRateDate.value)
  try {
    const resp = await executeFql(`SET RATE ${setRateId.value} ${setRateValue.value} ${date};`)
    setMsg.value = resp.success ? `✓ Rate set: ${setRateId.value} = ${setRateValue.value} on ${date}` : resp.error || 'Error'
    if (resp.success) toast.add({ severity: 'success', summary: 'Rate updated', detail: `${setRateId.value} = ${setRateValue.value}`, life: 3000 })
  } catch (e: any) {
    setMsg.value = e.message
    toast.add({ severity: 'error', summary: 'Failed to set rate', detail: e.message, life: 5000 })
  }
}

async function doLookup() {
  if (!lookupId.value) return
  const date = formatDate(lookupDate.value)
  try {
    const resp = await executeFql(`GET fx_rate('${lookupId.value}', ${date}) AS rate`)
    if (resp.success && resp.results.length > 0) {
      const resultText = resp.results[0] ?? ''
      const match = resultText.match(/rate:\s*(.+)/)
      lookupResult.value = match && match[1] ? match[1].trim() : resultText
    } else {
      lookupResult.value = resp.error || 'Not found'
    }
  } catch (e: any) {
    lookupResult.value = null
    toast.add({ severity: 'error', summary: 'Lookup failed', detail: e.message, life: 5000 })
  }
}
</script>
