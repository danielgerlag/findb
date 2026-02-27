import { defineStore } from 'pinia'
import { ref } from 'vue'

export const useEntityStore = defineStore('entity', () => {
  const activeEntity = ref('default')
  const entities = ref<string[]>(['default'])
  const loading = ref(false)

  async function fetchEntities() {
    loading.value = true
    try {
      const res = await fetch('/api/entities')
      const list: string[] = await res.json()
      entities.value = list
      // If the active entity was deleted, fall back to default
      if (!list.includes(activeEntity.value)) {
        activeEntity.value = list.includes('default') ? 'default' : list[0] || 'default'
      }
    } catch {
      // keep current state on error
    } finally {
      loading.value = false
    }
  }

  async function createEntity(name: string): Promise<boolean> {
    try {
      const res = await fetch('/api/entities', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      })
      const data = await res.json()
      if (data.success) {
        await fetchEntities()
        activeEntity.value = name
        return true
      }
      return false
    } catch {
      return false
    }
  }

  function setActiveEntity(name: string) {
    activeEntity.value = name
  }

  return { activeEntity, entities, loading, fetchEntities, createEntity, setActiveEntity }
})
