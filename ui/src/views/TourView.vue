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

    <EntityPicker
      v-if="showEntityPicker && tour"
      :suggested-name="suggestedEntityName"
      @select="onEntitySelected"
      @cancel="cancelEntityPicker"
    />

    <TourPlayer v-else-if="tour && entityId" :tour="tour" :entity-id="entityId" />
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { parseTour } from '../lib/fqlt'
import type { Tour } from '../lib/fqlt'
import TourPlayer from '../components/tour/TourPlayer.vue'
import EntityPicker from '../components/tour/EntityPicker.vue'

const route = useRoute()
const router = useRouter()

const tour = ref<Tour | null>(null)
const tourFile = ref<string | null>(null)
const loading = ref(false)
const error = ref<string | null>(null)
const showEntityPicker = ref(false)
const entityId = ref<string | null>(null)

const suggestedEntityName = computed(() => {
  const title = tour.value?.meta.title || tourFile.value || 'Tour'
  return title.replace(/[^a-zA-Z0-9 _-]/g, '').trim()
})

onMounted(() => {
  const file = route.query.file as string | undefined
  if (file) loadTour(file)
})

async function loadTour(name: string) {
  tourFile.value = name
  loading.value = true
  error.value = null
  tour.value = null
  entityId.value = null

  try {
    const res = await fetch(`/tours/${name}.fqlt`)
    if (!res.ok) throw new Error(`Tour not found: ${name}`)
    const source = await res.text()
    tour.value = parseTour(source)
    router.replace({ query: { file: name } })
    showEntityPicker.value = true
  } catch (e: any) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

function onEntitySelected(id: string) {
  entityId.value = id
  showEntityPicker.value = false
}

function cancelEntityPicker() {
  showEntityPicker.value = false
  tour.value = null
  tourFile.value = null
  entityId.value = null
  router.replace({ query: {} })
}
</script>

<style scoped>
.tour-description {
  color: var(--text-secondary);
  margin-top: 0.25rem;
  font-size: 0.95rem;
}
.loading {
  color: var(--text-muted);
  padding: 2rem;
  text-align: center;
}
.tour-list {
  display: grid;
  gap: 1rem;
}
.tour-card {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: 8px;
  padding: 1.25rem;
  cursor: pointer;
  transition: border-color 0.2s, box-shadow 0.2s;
}
.tour-card:hover { border-color: #3b82f6; box-shadow: 0 2px 8px rgba(59, 130, 246, 0.25); }
.tour-card h4 { margin: 0 0 0.5rem; font-size: 1.1rem; color: var(--text-heading); }
.tour-card p { color: var(--text-secondary); font-size: 0.9rem; margin: 0 0 0.75rem; }
.tour-tag {
  display: inline-block;
  background: var(--tag-bg);
  color: var(--tag-text);
  padding: 0.2em 0.6em;
  border-radius: 4px;
  font-size: 0.75rem;
  text-transform: uppercase;
}
</style>
