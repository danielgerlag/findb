<template>
  <div class="entity-picker-overlay" @click.self="$emit('cancel')">
    <div class="entity-picker">
      <h3>Choose an Entity for This Tour</h3>
      <p class="picker-description">
        Each tour runs inside an isolated entity — a separate set of books.
        Pick an existing entity or create a new one.
      </p>

      <div v-if="loading" class="picker-loading">
        <i class="pi pi-spin pi-spinner"></i> Loading entities…
      </div>

      <div v-else>
        <!-- Create new (shown first, recommended) -->
        <div class="entity-option new-entity" :class="{ selected: mode === 'new' }" @click="mode = 'new'">
          <div class="option-radio"><span v-if="mode === 'new'" class="dot" /></div>
          <div class="option-body">
            <strong>Create new entity</strong>
            <div class="new-entity-input" v-if="mode === 'new'">
              <input
                ref="nameInput"
                v-model="newName"
                placeholder="Entity name"
                class="entity-name-input"
                @keydown.enter="confirm"
                autofocus
              />
            </div>
          </div>
        </div>

        <!-- Existing entities -->
        <div
          v-for="entity in entities"
          :key="entity"
          class="entity-option"
          :class="{ selected: mode === 'existing' && selectedEntity === entity }"
          @click="selectExisting(entity)"
        >
          <div class="option-radio"><span v-if="mode === 'existing' && selectedEntity === entity" class="dot" /></div>
          <div class="option-body">
            <strong>{{ entity }}</strong>
            <span v-if="entity === 'default'" class="default-tag">default</span>
          </div>
        </div>

        <div v-if="errorMsg" class="picker-error">{{ errorMsg }}</div>

        <div class="picker-actions">
          <Button label="Cancel" severity="secondary" text @click="$emit('cancel')" />
          <Button
            :label="mode === 'new' ? 'Create & Start' : 'Start Tour'"
            icon="pi pi-play"
            :disabled="!canConfirm"
            :loading="creating"
            @click="confirm"
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick } from 'vue'
import Button from 'primevue/button'

const props = defineProps<{
  suggestedName: string
}>()

const emit = defineEmits<{
  (e: 'select', entityId: string): void
  (e: 'cancel'): void
}>()

const entities = ref<string[]>([])
const loading = ref(true)
const creating = ref(false)
const errorMsg = ref<string | null>(null)

const mode = ref<'new' | 'existing'>('new')
const newName = ref(props.suggestedName)
const selectedEntity = ref('')
const nameInput = ref<HTMLInputElement | null>(null)

const canConfirm = computed(() => {
  if (mode.value === 'new') return newName.value.trim().length > 0
  return selectedEntity.value.length > 0
})

onMounted(async () => {
  try {
    const res = await fetch('/api/entities')
    if (res.ok) {
      entities.value = await res.json()
    }
  } catch {
    // ignore, just show create option
  } finally {
    loading.value = false
  }
  await nextTick()
  nameInput.value?.focus()
})

function selectExisting(entity: string) {
  mode.value = 'existing'
  selectedEntity.value = entity
}

async function confirm() {
  if (!canConfirm.value) return
  errorMsg.value = null

  if (mode.value === 'new') {
    const name = newName.value.trim()
    creating.value = true
    try {
      const res = await fetch('/api/entities', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name }),
      })
      const data = await res.json()
      if (!data.success) {
        errorMsg.value = data.error || 'Failed to create entity'
        return
      }
      emit('select', name)
    } catch (e: any) {
      errorMsg.value = e.message || 'Connection error'
    } finally {
      creating.value = false
    }
  } else {
    emit('select', selectedEntity.value)
  }
}
</script>

<style scoped>
.entity-picker-overlay {
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.6);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
}
.entity-picker {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  border-radius: 12px;
  padding: 1.5rem;
  width: 420px;
  max-width: 90vw;
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.4);
}
.entity-picker h3 {
  margin: 0 0 0.25rem;
  color: var(--text-heading);
  font-size: 1.15rem;
}
.picker-description {
  color: var(--text-secondary);
  font-size: 0.85rem;
  margin: 0 0 1rem;
  line-height: 1.4;
}
.picker-loading {
  text-align: center;
  padding: 1.5rem;
  color: var(--text-muted);
}
.entity-option {
  display: flex;
  align-items: flex-start;
  gap: 0.75rem;
  padding: 0.75rem;
  border: 1px solid var(--border);
  border-radius: 8px;
  margin-bottom: 0.5rem;
  cursor: pointer;
  transition: border-color 0.15s, background 0.15s;
}
.entity-option:hover {
  border-color: #3b82f6;
}
.entity-option.selected {
  border-color: #3b82f6;
  background: rgba(59, 130, 246, 0.08);
}
.option-radio {
  width: 18px;
  height: 18px;
  border-radius: 50%;
  border: 2px solid var(--text-muted);
  display: flex;
  align-items: center;
  justify-content: center;
  flex-shrink: 0;
  margin-top: 2px;
}
.entity-option.selected .option-radio {
  border-color: #3b82f6;
}
.dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background: #3b82f6;
}
.option-body {
  flex: 1;
}
.option-body strong {
  color: var(--text-heading);
  font-size: 0.95rem;
}
.default-tag {
  display: inline-block;
  background: var(--tag-bg);
  color: var(--tag-text);
  padding: 0.1em 0.5em;
  border-radius: 4px;
  font-size: 0.7rem;
  text-transform: uppercase;
  margin-left: 0.5rem;
  vertical-align: middle;
}
.new-entity-input {
  margin-top: 0.5rem;
}
.entity-name-input {
  width: 100%;
  padding: 0.5rem 0.75rem;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--bg-page);
  color: var(--text-primary);
  font-size: 0.9rem;
  outline: none;
}
.entity-name-input:focus {
  border-color: #3b82f6;
  box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.2);
}
.picker-error {
  color: var(--error);
  font-size: 0.85rem;
  margin: 0.5rem 0;
}
.picker-actions {
  display: flex;
  justify-content: flex-end;
  gap: 0.5rem;
  margin-top: 1rem;
  padding-top: 0.75rem;
  border-top: 1px solid var(--border);
}
</style>
