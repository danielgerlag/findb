<template>
  <div class="tour-narrative">
    <div v-if="text" class="tour-text" v-html="renderedText"></div>
    <div v-if="note" class="tour-note">
      <i class="pi pi-info-circle"></i>
      <div v-html="renderedNote"></div>
    </div>
    <div v-if="caption" class="tour-caption">{{ caption }}</div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import snarkdown from 'snarkdown'

const props = defineProps<{
  text?: string
  note?: string
  caption?: string
}>()

const renderedText = computed(() => props.text ? snarkdown(props.text) : '')
const renderedNote = computed(() => props.note ? snarkdown(props.note) : '')
</script>

<style scoped>
.tour-text {
  font-size: 1rem;
  line-height: 1.7;
  color: #e2e8f0;
  margin-bottom: 1rem;
}
.tour-text :deep(strong) { color: #f8fafc; }
.tour-text :deep(code) {
  background: #334155;
  padding: 0.15em 0.4em;
  border-radius: 4px;
  font-size: 0.9em;
}
.tour-text :deep(ul), .tour-text :deep(ol) {
  padding-left: 1.5rem;
  margin: 0.5rem 0;
}
.tour-note {
  display: flex;
  gap: 0.75rem;
  background: #1e3a5f;
  border-left: 3px solid #3b82f6;
  padding: 0.75rem 1rem;
  border-radius: 6px;
  margin-bottom: 1rem;
  font-size: 0.9rem;
  color: #93c5fd;
}
.tour-note .pi { margin-top: 0.15rem; flex-shrink: 0; }
.tour-caption {
  font-size: 0.8rem;
  color: #94a3b8;
  font-style: italic;
  margin-bottom: 0.5rem;
}
</style>
