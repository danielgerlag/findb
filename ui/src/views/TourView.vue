<template>
  <div>
    <div class="page-header">
      <h1>{{ tour ? tour.meta.title || 'Tour' : 'Tours' }}</h1>
      <p v-if="tour?.meta.description" class="tour-description">{{ tour.meta.description }}</p>
    </div>

    <div v-if="loading" class="loading"><i class="pi pi-spin pi-spinner"></i> Loading tour...</div>

    <div v-else-if="error" class="error-msg">{{ error }}</div>

    <div v-else-if="!tourFile" class="card">
      <h3>Available Tours</h3>
      <div class="tour-list">
        <div class="tour-card" @click="loadTour('lending-fund')">
          <h4>📒 Building a Lending Fund</h4>
          <p>Learn double-entry bookkeeping by building a lending fund with investor equity, loan issuance, and interest accrual.</p>
          <span class="tour-tag">beginner</span>
        </div>
      </div>
    </div>

    <TourPlayer v-else-if="tour" :tour="tour" />
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { parseTour } from '../lib/fqlt'
import type { Tour } from '../lib/fqlt'
import TourPlayer from '../components/tour/TourPlayer.vue'

const route = useRoute()
const router = useRouter()

const tour = ref<Tour | null>(null)
const tourFile = ref<string | null>(null)
const loading = ref(false)
const error = ref<string | null>(null)

onMounted(() => {
  const file = route.query.file as string | undefined
  if (file) loadTour(file)
})

async function loadTour(name: string) {
  tourFile.value = name
  loading.value = true
  error.value = null
  tour.value = null

  try {
    const res = await fetch(`/tours/${name}.fqlt`)
    if (!res.ok) throw new Error(`Tour not found: ${name}`)
    const source = await res.text()
    tour.value = parseTour(source)
    router.replace({ query: { file: name } })
  } catch (e: any) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}
</script>

<style scoped>
.tour-description {
  color: #64748b;
  margin-top: 0.25rem;
  font-size: 0.95rem;
}
.loading {
  color: #64748b;
  padding: 2rem;
  text-align: center;
}
.tour-list {
  display: grid;
  gap: 1rem;
}
.tour-card {
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 8px;
  padding: 1.25rem;
  cursor: pointer;
  transition: border-color 0.2s, box-shadow 0.2s;
}
.tour-card:hover { border-color: #3b82f6; box-shadow: 0 2px 8px rgba(59, 130, 246, 0.15); }
.tour-card h4 { margin: 0 0 0.5rem; font-size: 1.1rem; color: #1e293b; }
.tour-card p { color: #64748b; font-size: 0.9rem; margin: 0 0 0.75rem; }
.tour-tag {
  display: inline-block;
  background: #e2e8f0;
  color: #475569;
  padding: 0.2em 0.6em;
  border-radius: 4px;
  font-size: 0.75rem;
  text-transform: uppercase;
}
</style>
