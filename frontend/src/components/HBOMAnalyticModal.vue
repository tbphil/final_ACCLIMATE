<template>
  <teleport to="body">
    <!-- Backdrop -->
    <div class="modal-backdrop fade show" style="z-index: 1055"></div>

    <div
      class="modal fade show"
      tabindex="-1"
      style="display: block; z-index: 1060"
      aria-hidden="false"
    >
      <!-- Wider dialog so the sunburst can be larger -->
      <div class="modal-dialog modal-xxl" style="max-width: 75vw">
        <div class="modal-content">
          <div class="modal-header">
            <h5 class="modal-title">{{ infraHeaderTitle }}</h5>
            <button type="button" class="btn-close" @click="emit('close')" />
          </div>

          <div class="modal-body">
            <!-- Time control (shared with GraphsContainer via store) -->
            <div class="d-flex align-items-center gap-3 mb-3">
              <button
                type="button"
                class="btn btn-sm btn-outline-secondary"
                @click="stepTime(-1)"
                :disabled="!(times?.length)"
              >
                ‹
              </button>

              <input
                type="range"
                class="form-range flex-grow-1"
                :min="0"
                :max="Math.max(0, (times?.length ?? 0) - 1)"
                v-model.number="currentTimeIndexLocal"
                :disabled="!(times?.length)"
              />

              <button
                type="button"
                class="btn btn-sm btn-outline-secondary"
                @click="stepTime(1)"
                :disabled="!(times?.length)"
              >
                ›
              </button>

              <div class="ms-2 small text-muted" style="white-space: nowrap;">
                {{ times[currentTimeIndex] ?? '' }}
              </div>
            </div>

            <!-- Charts -->
            <div v-if="fragilityView === 'sunburst'" class="mb-3">
              <!-- Make the sunburst taller; Plotly will fill the div -->
              <div id="sunburst-plot" style="min-height: 650px;"></div>
            </div>
            <div v-else-if="fragilityView === 'timeseries'" class="mb-3">
              <div id="fragility-timeseries-plot" style="min-height: 420px;"></div>
            </div>

            <!-- Inline Fragility panel (opens only when a clicked node has a curve) -->
            <div v-show="showFragilityInline" class="mt-3">
              <div class="card">
                <div class="card-body">
                  <div class="d-flex justify-content-between align-items-center mb-2">
                    <h6 class="mb-0">
                      {{ fragilityModalData.componentName }} — {{ fullVariableName }}
                    </h6>
                    <button
                      class="btn btn-sm btn-outline-secondary"
                      @click="showFragilityInline = false"
                    >
                      Close
                    </button>
                  </div>

                  <!-- (1) Inline PoF time series (already wired) -->
                  <div id="fragility-inline-plot" style="min-height: 320px;"></div>

                  <!-- (2) Static fragility curve + parameters -->
                  <div class="row g-3 mt-3">
                    <div class="col-lg-7">
                      <h6 class="mb-2">Fragility Curve</h6>
                      <div id="fragility-static-curve" style="min-height: 320px;"></div>
                      <div v-if="!hasStaticCurve" class="text-muted small mt-1">
                        (No curve data available for this component.)
                      </div>
                    </div>

                    <div class="col-lg-5">
                      <h6 class="mb-2">Model &amp; Parameters</h6>
                      <table class="table table-sm align-middle">
                        <tbody>
                          <tr>
                            <th class="w-25">Model</th>
                            <td>{{ fragilityModalData.model || '—' }}</td>
                          </tr>

                          <template v-if="modelParamsEntries.length">
                            <tr v-for="([key, val]) in modelParamsEntries" :key="key">
                              <th>{{ key }}</th>
                              <td>{{ formatParam(val) }}</td>
                            </tr>
                          </template>

                          <tr v-else>
                            <td colspan="2" class="text-muted small">No parameters provided.</td>
                          </tr>
                        </tbody>
                      </table>
                    </div>
                  </div>
                  <!-- /Static fragility curve + parameters -->
                </div>
              </div>
            </div>
          </div> <!-- /modal-body -->
        </div>
      </div>
    </div>
  </teleport>
</template>

<script setup>
import { computed, reactive, ref, watch, nextTick, onBeforeUnmount } from 'vue'
import { storeToRefs } from 'pinia'
import { useDataStore } from '@/stores/data'
import { createSunburstChart, createFragilityTimeSeriesChart, baseLayout } from '@/charting'
import Plotly from 'plotly.js-dist'

const emit = defineEmits(['close'])

// optional prop from App.vue: :asset-type="lastClickedType"
const props = defineProps({
  assetType: { type: String, default: '' }
})

// store state
const { fragilityCurves, fragilityView, hbomDefinitions, selectedAsset, selectedVariable, currentTimeIndex, Hazard, times, variableNameMapping } =
  storeToRefs(useDataStore())

// Header title: "<Type> Risk Details"
const infraName = computed(() =>
  props.assetType || selectedAsset.value?.facilityTypeName || 'Infrastructure'
)
const infraHeaderTitle = computed(() => `${infraName.value} Risk Details`)

// Fragility data populated by sunburst click
const fragilityModalData = reactive({
  componentName: '',
  curveData: [],           // time-series PoF for the clicked node (already in place)
  hazardName: '',
  // static curve + metadata (when available from HBOM backend)
  xValues: [],             // hazard intensity values (backend x_values)
  staticCurve: [],         // matching fc_values (backend fc_values)
  model: 'weibull',
  modelParams: {}
})

const fullVariableName = computed(() => 
  variableNameMapping.value[selectedVariable.value] || selectedVariable.value
)

// Inline fragility panel (proxy for the old floating modal)
const showFragilityInline = ref(false)

/**
 * Try to enrich fragilityModalData with static-curve info from hbomDefinitions.
 * We look up the clicked component inside the selected facility tree and pull:
 *   hazards[Hazard].fragility_curves[variable]['0'] => { x_values, fc_values }
 *   plus fragility_model / fragility_params at that node.
 * This matches the backend shape produced in fragilityCurve.py. :contentReference[oaicite:3]{index=3}
 */
function hydrateStaticCurveIfMissing() {
  if (!hbomDefinitions.value) return
  if (!fragilityModalData.componentName) return
  if (Array.isArray(fragilityModalData.xValues) && fragilityModalData.xValues.length > 1) return

  const facilityRoot = (hbomDefinitions.value.components || []).find(
    c => c.label === (selectedAsset.value?.facilityTypeName || props.assetType)
  )
  if (!facilityRoot) return

  let found = null
  ;(function walk(node) {
    if (found) return
    if (node.label === fragilityModalData.componentName) {
      found = node
      return
    }
    (node.subcomponents || []).forEach(walk)
  })(facilityRoot)

  if (!found) return

  const hazObj = (found.hazards || {})[Hazard.value]
  if (!hazObj) return

  // prefer the selected variable, grid "0"
  const varKey = selectedVariable.value
  const grids = (hazObj.fragility_curves || {})[varKey]
  const g0 = grids ? grids['0'] : null

  if (g0 && Array.isArray(g0.x_values) && Array.isArray(g0.fc_values) && g0.x_values.length === g0.fc_values.length) {
    fragilityModalData.xValues = g0.x_values
    fragilityModalData.staticCurve = g0.fc_values
  }
  if (hazObj.fragility_model) fragilityModalData.model = hazObj.fragility_model
  if (hazObj.fragility_params) fragilityModalData.modelParams = hazObj.fragility_params
}

// helper: format model parameter values like your old modal did. :contentReference[oaicite:4]{index=4}
const modelParamsEntries = computed(() =>
  fragilityModalData?.modelParams ? Object.entries(fragilityModalData.modelParams) : []
)
const formatParam = (v) => {
  if (v == null) return '—'
  if (typeof v === 'number') return Number.isFinite(v) ? v.toPrecision(6) : String(v)
  if (Array.isArray(v)) return `[${v.map(n => (Number.isFinite(n) ? n.toPrecision(4) : n)).join(', ')}]`
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

const hasStaticCurve = computed(() =>
  Array.isArray(fragilityModalData.xValues) &&
  Array.isArray(fragilityModalData.staticCurve) &&
  fragilityModalData.xValues.length === fragilityModalData.staticCurve.length &&
  fragilityModalData.xValues.length > 1
)

// Proxy object that satisfies createSunburstChart's expectations.
// Instead of opening a floating modal, we show an inline panel and plot there.
const fragModal = ref({
  show () {
    showFragilityInline.value = true
  },
  hide () {
    showFragilityInline.value = false
  },
  plotFragilityCurve () {
    // Make sure static curve metadata is populated when we click a node
    hydrateStaticCurveIfMissing()

    // (1) Inline time series (PoF over time)
    const tsEl = document.getElementById('fragility-inline-plot')
    if (tsEl) {
      const xT = times.value || []
      const yT = fragilityModalData.curveData || []
      const currentTime = times.value[currentTimeIndex.value]
      
      Plotly.react(
        tsEl,
        [{ x: xT, y: yT, mode: 'lines', name: fullVariableName.value }],
        {
          ...(baseLayout || {}),
          title: `${fragilityModalData.componentName} — Probability of Failure (Time)`,
          xaxis: { title: 'Time' },
          yaxis: { title: 'P(failure)', rangemode: 'tozero' },
          shapes: [{
            type: 'line',
            x0: currentTime,
            x1: currentTime,
            yref: 'paper',
            y0: 0,
            y1: 1,
            line: { width: 2, dash: 'dash', color: '#8dc340' }
          }],
          height: 340
        }
      )
    }

    // (2) Static fragility curve (hazard intensity → PoF), only if data present
    const fcEl = document.getElementById('fragility-static-curve')
    if (fcEl && hasStaticCurve.value) {
      Plotly.react(
        fcEl,
        [{
          x: fragilityModalData.xValues,
          y: fragilityModalData.staticCurve,
          mode: 'lines',
          name: fragilityModalData.model || 'fragility'
        }],
        {
          ...(baseLayout || {}),
          title: `${fragilityModalData.componentName} — Fragility Curve`,
          xaxis: { title: `${fullVariableName.value} Intensity` },
          yaxis: { title: 'P(failure)', rangemode: 'tozero' },
          height: 340
        }
      )
    }
  }
})

// Use chart helpers with cleanup
let destroySunburst = null
let destroyFragTs = null

const renderSunburst = async () => {
  await nextTick()
  if (destroySunburst) { destroySunburst(); destroySunburst = null }
  destroySunburst = createSunburstChart(
    { fragilityCurves, fragilityView, hbomDefinitions, selectedAsset, selectedVariable, currentTimeIndex, Hazard },
    { fragilityModalData, fragModal }
  )
}

const renderFragilityTs = async () => {
  await nextTick()
  if (destroyFragTs) { destroyFragTs(); destroyFragTs = null }
  destroyFragTs = createFragilityTimeSeriesChart({
    fragilityCurves, times, selectedAsset, selectedVariable, fragilityView
  })
}

// Switch between views
watch(
  fragilityView,
  async (view) => {
    if (view === 'sunburst') await renderSunburst()
    else if (view === 'timeseries') await renderFragilityTs()
  },
  { immediate: true }
)

// Time slider sync and re-render
const currentTimeIndexLocal = computed({
  get: () => currentTimeIndex.value,
  set: (v) => {
    const maxIndex = (times.value?.length ?? 1) - 1
    currentTimeIndex.value = Math.max(0, Math.min(v, maxIndex))
  }
})

const stepTime = (delta) => {
  const len = times.value?.length ?? 0
  if (len <= 0) return
  currentTimeIndexLocal.value = (currentTimeIndexLocal.value + delta + len) % len
}

watch(currentTimeIndex, async () => {
  if (fragilityView.value === 'sunburst') await renderSunburst()
  else if (fragilityView.value === 'timeseries') await renderFragilityTs()

  if (showFragilityInline.value) {
    fragModal.value.plotFragilityCurve()
  }
})

onBeforeUnmount(() => {
  if (destroySunburst) destroySunburst()
  if (destroyFragTs) destroyFragTs()
})
</script>