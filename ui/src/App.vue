<template>
  <div class="app-layout">
    <aside class="app-sidebar">
      <h2>📒 DblEntry</h2>
      <div class="entity-selector">
        <label>Entity</label>
        <select :value="entityStore.activeEntity" @change="onEntityChange">
          <option v-for="e in entityStore.entities" :key="e" :value="e">{{ e }}</option>
        </select>
      </div>
      <nav>
        <router-link to="/"><i class="pi pi-home"></i> Dashboard</router-link>
        <router-link to="/query"><i class="pi pi-code"></i> FQL Query</router-link>
        <router-link to="/tour"><i class="pi pi-play"></i> Tours</router-link>
        <router-link to="/accounts"><i class="pi pi-book"></i> Accounts</router-link>
        <router-link to="/journals"><i class="pi pi-file-edit"></i> Journals</router-link>
        <router-link to="/rates"><i class="pi pi-percentage"></i> Rates</router-link>
      </nav>
    </aside>
    <main class="app-main">
      <router-view />
    </main>
  </div>
  <Toast />
</template>

<script setup lang="ts">
import { onMounted } from 'vue'
import Toast from 'primevue/toast'
import { useEntityStore } from './stores/entity'

const entityStore = useEntityStore()

onMounted(() => entityStore.fetchEntities())

function onEntityChange(e: Event) {
  const val = (e.target as HTMLSelectElement).value
  entityStore.setActiveEntity(val)
}
</script>

<style scoped>
.entity-selector {
  padding: 0 1rem;
  margin-bottom: 0.75rem;
}
.entity-selector label {
  display: block;
  font-size: 0.7rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--text-muted);
  margin-bottom: 0.25rem;
}
.entity-selector select {
  width: 100%;
  padding: 0.35rem 0.5rem;
  background: var(--bg-input);
  color: var(--text);
  border: 1px solid var(--border);
  border-radius: 6px;
  font-size: 0.85rem;
  cursor: pointer;
}
.entity-selector select:focus {
  outline: none;
  border-color: var(--accent);
}
</style>
