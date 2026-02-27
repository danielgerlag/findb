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
import { marked } from 'marked'

const props = defineProps<{
  text?: string
  note?: string
  caption?: string
}>()

// Configure marked for inline rendering (no wrapping <p> tags)
const renderedText = computed(() => props.text ? marked.parse(props.text) as string : '')
const renderedNote = computed(() => props.note ? marked.parse(props.note) as string : '')
</script>

<style scoped>
.tour-text {
  font-size: 1rem;
  line-height: 1.7;
  color: var(--text-primary);
  margin-bottom: 1rem;
}
.tour-text :deep(strong) { color: var(--text-strong); font-weight: 600; }
.tour-text :deep(code) {
  background: var(--bg-surface);
  padding: 0.15em 0.4em;
  border-radius: 4px;
  font-size: 0.9em;
  color: var(--accent);
}
.tour-text :deep(ul), .tour-text :deep(ol) {
  padding-left: 1.5rem;
  margin: 0.5rem 0;
}
.tour-note {
  display: flex;
  gap: 0.75rem;
  background: var(--info-bg);
  border-left: 3px solid var(--info-border);
  padding: 0.75rem 1rem;
  border-radius: 6px;
  margin-bottom: 1rem;
  font-size: 0.9rem;
  color: var(--info-text);
}
.tour-note .pi { margin-top: 0.15rem; flex-shrink: 0; }
.tour-caption {
  font-size: 0.8rem;
  color: var(--text-muted);
  font-style: italic;
  margin-bottom: 0.5rem;
}
</style>
