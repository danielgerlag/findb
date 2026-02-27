<template>
  <div class="tour-code-block">
    <pre ref="codeEl"><code><template v-for="(line, i) in displayLines" :key="i"><span
      :class="lineClasses(i)"
    ><span class="line-num">{{ i + 1 }}</span><span v-html="renderLine(line)"></span>
</span></template></code></pre>
  </div>
</template>

<script setup lang="ts">
import { computed, ref, watch, onUnmounted } from 'vue'
import { highlightFql } from '../../lib/fql-highlight'

const props = withDefaults(defineProps<{
  code: string
  highlight?: string[]
  focus?: string
  reveal?: 'instant' | 'typewriter' | 'line-by-line'
  active?: boolean
}>(), {
  reveal: 'instant',
  active: true,
})

const emit = defineEmits<{ revealed: [] }>()

const allLines = computed(() => props.code.split('\n'))
const revealedCount = ref(props.reveal === 'instant' || !props.active ? allLines.value.length : 0)
let timer: ReturnType<typeof setInterval> | null = null

const displayLines = computed(() => allLines.value.slice(0, revealedCount.value))

// Focus: parse lines:N-M
const focusRange = computed<[number, number] | null>(() => {
  if (!props.focus) return null
  const m = props.focus.match(/^lines:(\d+)-(\d+)$/)
  if (!m) return null
  return [parseInt(m[1]!, 10) - 1, parseInt(m[2]!, 10) - 1]
})

function lineClasses(lineIdx: number): Record<string, boolean> {
  const range = focusRange.value
  return {
    'code-line': true,
    'dimmed': range !== null && (lineIdx < range[0] || lineIdx > range[1]),
  }
}

function renderLine(line: string): string {
  // First apply syntax highlighting
  let result = highlightFql(line)
  // Then layer on tour highlights (mark tags) if any
  if (props.highlight && props.highlight.length > 0) {
    for (const token of props.highlight) {
      if (token.startsWith('lines:')) continue
      const escaped = token.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
      result = result.replace(
        new RegExp(`(${escaped})`, 'g'),
        '<mark class="fqlt-highlight">$1</mark>'
      )
    }
  }
  return result
}

watch(() => props.active, (active) => {
  if (active && props.reveal !== 'instant') {
    startReveal()
  }
}, { immediate: true })

function startReveal() {
  if (props.reveal === 'instant') {
    revealedCount.value = allLines.value.length
    emit('revealed')
    return
  }
  revealedCount.value = 0
  const delay = props.reveal === 'line-by-line' ? 400 : 150
  timer = setInterval(() => {
    revealedCount.value++
    if (revealedCount.value >= allLines.value.length) {
      if (timer) clearInterval(timer)
      timer = null
      emit('revealed')
    }
  }, delay)
}

onUnmounted(() => {
  if (timer) clearInterval(timer)
})

// When code changes (new step), reset reveal
watch(() => props.code, () => {
  if (timer) clearInterval(timer)
  timer = null
  if (props.reveal === 'instant' || !props.active) {
    revealedCount.value = allLines.value.length
  } else {
    revealedCount.value = 0
  }
})
</script>

<style scoped>
.tour-code-block {
  background: #0f172a;
  border: 1px solid var(--border);
  border-radius: 8px;
  overflow-x: auto;
  margin-bottom: 1rem;
}
pre {
  margin: 0;
  padding: 1rem;
  font-family: 'Fira Code', 'Cascadia Code', 'Consolas', monospace;
  font-size: 0.875rem;
  line-height: 1.6;
}
code { color: #e2e8f0; }
.code-line { display: block; }
.code-line.dimmed { opacity: 0.3; }
.line-num {
  display: inline-block;
  width: 2.5em;
  text-align: right;
  margin-right: 1em;
  color: #64748b;
  user-select: none;
}
/* FQL syntax colors */
:deep(.fql-keyword) { color: #c084fc; font-weight: 500; }
:deep(.fql-type) { color: #67e8f9; font-weight: 500; }
:deep(.fql-account) { color: #34d399; }
:deep(.fql-string) { color: #fbbf24; }
:deep(.fql-number) { color: #fb923c; }
:deep(.fql-date) { color: #60a5fa; }
:deep(.fql-operator) { color: #94a3b8; }
:deep(.fql-comment) { color: #64748b; font-style: italic; }
:deep(.fql-function) { color: #38bdf8; }
:deep(.fql-param) { color: #f472b6; }
:deep(.fql-punctuation) { color: #94a3b8; }
:deep(.fql-bool) { color: #fb923c; font-weight: 500; }
:deep(.fqlt-highlight) {
  background: rgba(250, 204, 21, 0.3);
  color: #fbbf24;
  padding: 0.1em 0.2em;
  border-radius: 3px;
}
</style>
