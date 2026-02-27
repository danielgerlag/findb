<template>
  <div class="tour-progress">
    <div class="progress-bar">
      <div class="progress-fill" :style="{ width: progressPercent + '%' }"></div>
    </div>
    <div class="progress-controls">
      <Button icon="pi pi-refresh" severity="secondary" text size="small" @click="$emit('restart')" title="Restart" :disabled="currentStep === 0" />
      <Button icon="pi pi-arrow-left" severity="secondary" text size="small" @click="$emit('back')" :disabled="currentStep === 0" title="Previous step" />
      <span class="step-label">
        Step {{ currentStep + 1 }} of {{ totalSteps }}
        <span v-if="currentTitle" class="step-title"> — {{ currentTitle }}</span>
      </span>
      <Button icon="pi pi-arrow-right" severity="secondary" text size="small" @click="$emit('next')" :disabled="currentStep >= totalSteps - 1" title="Next step" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import Button from 'primevue/button'

const props = defineProps<{
  currentStep: number
  totalSteps: number
  stepTitles: string[]
}>()

defineEmits<{
  back: []
  next: []
  restart: []
}>()

const currentTitle = computed(() => props.stepTitles[props.currentStep] || '')
const progressPercent = computed(() =>
  props.totalSteps <= 1 ? 100 : ((props.currentStep + 1) / props.totalSteps) * 100
)
</script>

<style scoped>
.tour-progress { margin-bottom: 1.5rem; }
.progress-bar {
  height: 4px;
  background: var(--border);
  border-radius: 2px;
  overflow: hidden;
  margin-bottom: 0.75rem;
}
.progress-fill {
  height: 100%;
  background: var(--accent);
  transition: width 0.3s ease;
  border-radius: 2px;
}
.progress-controls {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}
.step-label {
  flex: 1;
  text-align: center;
  font-size: 0.85rem;
  color: var(--text-muted);
}
.step-title {
  color: var(--text-heading);
  font-weight: 500;
}
</style>
