<template>
  <!-- outer div inherits any attrs (like class="p-2") passed from App.vue -->
  <div v-bind="$attrs">
    <div
      v-if="climateDataLoaded"
      class="container-fluid"
    >
      <div class="row align-items-center">
        <!-- ───────── Left Column ───────── -->
        <div class="col-md-8">

          <!-- variable selector buttons -->
          <div class="mb-2">
            <div class="btn-group" role="group">
              <button
                v-for="v in variablesList"
                :key="v"
                :class="[
                  'btn',
                  v === selectedVariable ? 'btn-primary' : 'btn-outline-primary'
                ]"
                @click="setSelectedVariable(v)"
              >
                {{ variableNameMapping[v] ?? v }}
              </button>
            </div>
          </div>

          <!-- timeline slider -->
          <div class="mb-2" style="max-width: 300px;">
            <input
              type="range"
              class="form-range w-100"
              :min="0"
              :max="times.length - 1"
              :value="currentTimeIndex"
              @input="onSlider"
            />
            <div>{{ times[currentTimeIndex] }}</div>
          </div>

          <!-- trend toggle -->
          <div class="mb-2">
            <button
              class="btn btn-info"
              @click="toggleTrend()"
            >
              {{ showTrendAnalysis ? 'Raw Climate Data' : 'Climate Trend Analysis' }}
            </button>
          </div>

          <!-- prev / next -->
          <div class="d-flex">
            <button class="btn btn-outline-secondary me-2" @click="decTime()">Previous</button>
            <button class="btn btn-outline-secondary"      @click="incTime()">Next</button>
          </div>
        </div>

        <!-- ───────── Right Column ───────── -->
        <div class="col-md-4 text-end">
          <button
            :class="['btn', showClimateGrid ? 'btn-secondary' : 'btn-outline-secondary']"
            class="me-2"
            @click="toggleGrid()"
          >
            {{ showClimateGrid ? 'Hide' : 'Show' }} Grid
          </button>

          <button
            :class="['btn', showInfrastructureMarkers ? 'btn-secondary' : 'btn-outline-secondary']"
            class="me-2"
            @click="toggleInfra()"
          >
            {{ showInfrastructureMarkers ? 'Hide' : 'Show' }} Infrastructure
          </button>

          
        </div>
      </div>
    </div>
  </div>
</template>

<script setup>
/* ------------------------------------------------------------
 * 1. imports & Pinia store
 * ---------------------------------------------------------- */
import { watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useDataStore } from '@/stores/data'
const store = useDataStore()

/* ---------- reactive refs we read ---------- */
const {
  climateDataLoaded,
  variablesList,
  variableNameMapping,
  times,
  currentTimeIndex,
  selectedVariable,
  /* UI flags */
  showTrendAnalysis,
  showClimateGrid,
  showInfrastructureMarkers,
  // fragilityView,
} = storeToRefs(store)

/* ---------- actions we call ---------- */
const {
  setSelectedVariable,
  setCurrentTime,
  toggleTrendAnalysis,
  toggleClimateGrid,
  toggleInfrastructureMarkers,
  // toggleFragilityView,
  increaseTime,
  decreaseTime,
} = store

/* ------------------------------------------------------------
 * 2. local helpers (just wrap store actions)
 * ---------------------------------------------------------- */
function onSlider (e) {
  setCurrentTime(+e.target.value)
}

function incTime () { increaseTime() }
function decTime () { decreaseTime() }


/* buttons & toggles call these directly */
function toggleTrend () { toggleTrendAnalysis() }
function toggleGrid () { toggleClimateGrid() }
function toggleInfra () { toggleInfrastructureMarkers() }
// function toggleFragility () { toggleFragilityView() }

// robust date coercion (handles ISO strings, Date, seconds/ms epoch)
function toDate(v) {
  if (v instanceof Date) return v
  if (typeof v === 'number') {
    // guess seconds vs ms
    return new Date(v > 1e12 ? v : v * 1000)
  }
  // fall back to Date parser for strings
  return new Date(v)
}

// pick the first index >= now; if none, choose the closest overall
function pickInitialTimeIndex(arr) {
  if (!Array.isArray(arr) || !arr.length) return 0
  const now = new Date()

  // try: first future-or-today
  for (let i = 0; i < arr.length; i++) {
    const d = toDate(arr[i])
    if (!isNaN(d) && d >= now) return i
  }

  // else: closest in absolute time
  let bestIdx = 0, bestDiff = Infinity
  for (let i = 0; i < arr.length; i++) {
    const d = toDate(arr[i])
    if (isNaN(d)) continue
    const diff = Math.abs(d - now)
    if (diff < bestDiff) { bestDiff = diff; bestIdx = i }
  }
  return bestIdx
}

// when data loads (or times changes), set the initial slider position once
let _initializedTimeIndex = false
watch([climateDataLoaded, times], ([loaded, t]) => {
  if (!loaded || !t || !t.length) return
  if (_initializedTimeIndex) return
  const idx = pickInitialTimeIndex(t)
  if (typeof idx === 'number' && idx !== currentTimeIndex.value) {
    setCurrentTime(idx)
  }
  _initializedTimeIndex = true
}, { immediate: true })
</script>