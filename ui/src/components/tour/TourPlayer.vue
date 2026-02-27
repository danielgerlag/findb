<template>
  <div class="tour-player" @keydown="handleKeydown" tabindex="0" ref="playerEl">
    <TourProgress
      :current-step="currentStep"
      :total-steps="tour.steps.length"
      :step-titles="tour.steps.map((s) => s.title)"
      @back="goBack"
      @next="goNext"
      @restart="restart"
    />
    <TourStepView
      v-if="tour.steps.length > 0"
      :key="currentStep"
      :step="tour.steps[currentStep]!"
      :state="stepState"
      :result="results.get(currentStep) ?? null"
      :error="stepError"
      @code-revealed="onCodeRevealed"
      @run-clicked="executeCurrentStep"
      @retry="executeCurrentStep"
      @skip="advanceStep"
    />
    <div class="tour-nav-buttons">
      <Button
        v-if="canAdvance"
        :label="isLastStep ? 'Finish' : 'Next'"
        :icon="isLastStep ? 'pi pi-check' : 'pi pi-arrow-right'"
        icon-pos="right"
        @click="goNext"
      />
    </div>
    <div v-if="finished" class="tour-finished">
      <h2>🎉 Tour Complete!</h2>
      <p>You've completed "{{ tour.meta.title || 'this tour' }}".</p>
      <Button label="Restart Tour" icon="pi pi-refresh" severity="secondary" @click="restart" />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, nextTick } from 'vue'
import type { Tour } from '../../lib/fqlt/types'
import type { FqlResponse } from '../../api/client'
import { executeFql } from '../../api/client'
import TourProgress from './TourProgress.vue'
import TourStepView from './TourStepView.vue'
import Button from 'primevue/button'

const props = defineProps<{ tour: Tour }>()

const playerEl = ref<HTMLElement | null>(null)
const currentStep = ref(0)
const stepState = ref<'narrative' | 'code-reveal' | 'executing' | 'result' | 'done'>('narrative')
const stepError = ref<string | null>(null)
const results = ref<Map<number, FqlResponse>>(new Map())
const finished = ref(false)

const isLastStep = computed(() => currentStep.value >= props.tour.steps.length - 1)
const canAdvance = computed(() =>
  (stepState.value === 'result' || stepState.value === 'done') && !finished.value
)

onMounted(() => {
  nextTick(() => playerEl.value?.focus())
  startStep()
})

function startStep() {
  stepError.value = null
  finished.value = false
  const step = props.tour.steps[currentStep.value]
  if (!step) return

  // If there's narrative text, show it first
  if (step.text || step.note || step.caption) {
    stepState.value = 'narrative'
    // Auto-advance to code after a brief moment if no explicit wait
    if (!step.wait) {
      setTimeout(() => {
        if (stepState.value === 'narrative') {
          stepState.value = 'code-reveal'
          maybeAutoExecute()
        }
      }, 800)
    }
  } else {
    stepState.value = 'code-reveal'
    maybeAutoExecute()
  }
}

function onCodeRevealed() {
  // Reveal is done — execute directly (bypass the reveal guard in maybeAutoExecute)
  executeCurrentStep()
}

function maybeAutoExecute() {
  const step = props.tour.steps[currentStep.value]
  if (!step) return

  if (step.run === 'skip' || !step.code.trim()) {
    stepState.value = 'done'
    return
  }
  if (step.run === 'click') {
    // Wait for user to click Run
    return
  }
  // Default: auto
  if (step.reveal && step.reveal !== 'instant') {
    // Wait for reveal to finish (handled by codeRevealed event)
    if (stepState.value === 'code-reveal') return
  }
  executeCurrentStep()
}

async function executeCurrentStep() {
  const step = props.tour.steps[currentStep.value]
  if (!step || !step.code.trim()) {
    stepState.value = 'done'
    return
  }

  stepState.value = 'executing'
  stepError.value = null

  try {
    const resp = await executeFql(step.code)
    results.value.set(currentStep.value, resp)
    if (resp.success) {
      stepState.value = 'result'
    } else {
      stepError.value = resp.error || 'Execution failed'
      stepState.value = 'result'
    }
  } catch (e: any) {
    stepError.value = e.message || 'Connection error'
    stepState.value = 'result'
  }
}

function goNext() {
  if (stepState.value === 'narrative') {
    stepState.value = 'code-reveal'
    maybeAutoExecute()
    return
  }
  if (stepState.value === 'code-reveal') {
    // User clicked Next — skip remaining reveal and execute
    executeCurrentStep()
    return
  }
  advanceStep()
}

function advanceStep() {
  if (isLastStep.value) {
    finished.value = true
    stepState.value = 'done'
    return
  }
  currentStep.value++
  startStep()
}

function goBack() {
  if (currentStep.value > 0) {
    currentStep.value--
    // Show cached result if available, otherwise restart step
    if (results.value.has(currentStep.value)) {
      stepState.value = 'result'
      stepError.value = null
    } else {
      startStep()
    }
  }
}

function restart() {
  currentStep.value = 0
  results.value.clear()
  finished.value = false
  startStep()
}

function handleKeydown(e: KeyboardEvent) {
  if (e.key === 'ArrowRight' || e.key === ' ') {
    e.preventDefault()
    goNext()
  } else if (e.key === 'ArrowLeft') {
    e.preventDefault()
    goBack()
  } else if (e.key === 'r' || e.key === 'R') {
    e.preventDefault()
    restart()
  }
}
</script>

<style scoped>
.tour-player {
  outline: none;
  max-width: 900px;
}
.tour-nav-buttons {
  display: flex;
  justify-content: flex-end;
  margin-top: 0.5rem;
}
.tour-finished {
  text-align: center;
  padding: 2rem;
  animation: fadeIn 0.5s ease;
}
.tour-finished h2 { font-size: 1.5rem; margin-bottom: 0.5rem; color: var(--text-heading); }
.tour-finished p { color: var(--text-secondary); margin-bottom: 1rem; }
@keyframes fadeIn { from { opacity: 0; } to { opacity: 1; } }
</style>
