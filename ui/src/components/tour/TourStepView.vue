<template>
  <div :class="['tour-step', layoutClass]">
    <div class="step-narrative">
      <TourNarrative :text="step.text" :note="step.note" :caption="step.caption" />
    </div>
    <div class="step-code-area">
      <TourCodeBlock
        v-if="step.code.trim()"
        :code="step.code"
        :highlight="step.highlight"
        :focus="step.focus"
        :reveal="step.reveal || 'instant'"
        :active="state !== 'narrative'"
        @revealed="$emit('codeRevealed')"
      />
      <div v-if="step.run === 'click' && state === 'code-reveal'" class="run-prompt">
        <Button label="Run" icon="pi pi-play" @click="$emit('runClicked')" />
      </div>
      <div v-if="state === 'executing'" class="executing">
        <i class="pi pi-spin pi-spinner"></i> Executing...
      </div>
      <div v-if="error" class="step-error">
        <i class="pi pi-exclamation-triangle"></i> {{ error }}
        <div class="error-actions">
          <Button label="Retry" icon="pi pi-refresh" size="small" severity="warning" @click="$emit('retry')" />
          <Button label="Skip" icon="pi pi-forward" size="small" severity="secondary" @click="$emit('skip')" />
        </div>
      </div>
      <div v-if="result && !step.hideOutput" class="step-results">
        <div v-for="(r, i) in resultLines" :key="i" class="result-block">
          <pre>{{ r }}</pre>
        </div>
      </div>
      <div v-if="step.expect && (state === 'result' || state === 'done') && !error" class="step-expect">
        <i class="pi pi-check-circle"></i> {{ step.expect }}
      </div>
      <div v-if="assertResults.length > 0" class="step-asserts">
        <div v-for="(a, i) in assertResults" :key="i" :class="['assert-item', a.passed ? 'passed' : 'failed']">
          <i :class="a.passed ? 'pi pi-check' : 'pi pi-times'"></i>
          {{ a.variable }} {{ a.operator }} {{ a.expected }}
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'
import type { TourStep } from '../../lib/fqlt/types'
import type { FqlResponse } from '../../api/client'
import TourNarrative from './TourNarrative.vue'
import TourCodeBlock from './TourCodeBlock.vue'
import Button from 'primevue/button'

const props = defineProps<{
  step: TourStep
  state: 'narrative' | 'code-reveal' | 'executing' | 'result' | 'done'
  result?: FqlResponse | null
  error?: string | null
}>()

defineEmits<{
  codeRevealed: []
  runClicked: []
  retry: []
  skip: []
}>()

const layoutClass = computed(() => `layout-${props.step.layout || 'stacked'}`)

const resultLines = computed(() => {
  if (!props.result?.results) return []
  return props.result.results.filter((r) => r.trim().length > 0)
})

const assertResults = computed(() => {
  if (!props.step.assert || !props.result) return []
  const output = props.result.results?.join('\n') || ''
  return props.step.assert.map((a) => {
    let passed = false
    if (a.operator === 'contains') {
      passed = output.includes(a.expected)
    } else {
      passed = output.includes(`${a.variable}: ${a.expected}`) ||
               output.includes(`${a.expected}`)
    }
    return { ...a, passed }
  })
})
</script>

<style scoped>
.tour-step { animation: fadeIn 0.3s ease; }
@keyframes fadeIn { from { opacity: 0; transform: translateY(8px); } to { opacity: 1; transform: translateY(0); } }

.layout-stacked .step-narrative { margin-bottom: 1rem; }
.layout-split {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
  align-items: start;
}
.layout-full-code .step-narrative {
  position: absolute;
  top: 1rem;
  right: 1rem;
  background: rgba(30, 41, 59, 0.95);
  padding: 1rem;
  border-radius: 8px;
  max-width: 320px;
  z-index: 1;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.3);
}
.layout-full-code { position: relative; }

.run-prompt { margin-bottom: 1rem; }
.executing {
  color: var(--text-muted);
  margin-bottom: 1rem;
  font-size: 0.9rem;
}
.step-error {
  background: var(--error-bg);
  border: 1px solid var(--error-border);
  color: var(--error-text);
  padding: 0.75rem 1rem;
  border-radius: 6px;
  margin-bottom: 1rem;
}
.error-actions { display: flex; gap: 0.5rem; margin-top: 0.5rem; }
.step-results { margin-bottom: 1rem; }
.result-block pre {
  background: var(--bg-surface);
  border: 1px solid var(--border);
  padding: 0.75rem;
  border-radius: 6px;
  overflow-x: auto;
  font-size: 0.85rem;
  color: var(--text-primary);
  margin-bottom: 0.5rem;
}
.step-expect {
  color: var(--success-text);
  font-size: 0.9rem;
  margin-bottom: 1rem;
}
.step-expect .pi { margin-right: 0.5rem; }
.step-asserts { margin-bottom: 1rem; }
.assert-item {
  font-size: 0.85rem;
  padding: 0.25rem 0;
}
.assert-item.passed { color: var(--success-text); }
.assert-item.failed { color: var(--error-text); }
.assert-item .pi { margin-right: 0.5rem; }
</style>
