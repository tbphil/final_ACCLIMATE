<template>
  <div class="container-fluid">
    <div class="row">
      <!-- Left Column: raw vs trend -->
      <div v-if="!showTrendAnalysis" class="col-12 col-md-6 mb-3">
        <div class="graph">
          <div :id="`graph-${selectedVariable}`"></div>
        </div>
      </div>
      <div v-if="showTrendAnalysis" class="col-12 col-md-6 mb-3">
        <div class="graph">
          <div :id="`analysis-plot-${selectedVariable}`"></div>
          <div :id="`histogram-plot-${selectedVariable}`"></div>
        </div>
      </div>

      <!-- Right Column: sunburst vs fragility timeseries -->
      <div class="col-12 col-md-6 mb-3">
        <span>Coming Soon</span>
      </div> 
    </div>

    <!-- Fragility Curve Modal -->
    <FragilityCurveModal
      ref="fragModal"
      :modal-title="modalTitle"
      :full-variable-name="fullVariableName"
      :component-name="fragilityModalData.componentName"
      :hazard-name="fragilityModalData.hazardName"
      :model="fragilityModalData.model"
      :model-params="fragilityModalData.modelParams"
      :x-values="fragilityModalData.xValues"
      :curve-data="fragilityModalData.curveData"
    />
  </div>
</template>

<script setup>
import { onMounted, watch, nextTick, computed, reactive, ref } from 'vue'
import { storeToRefs } from 'pinia'
import { useDataStore } from '@/stores/data'
import {
  createSunburstChart,
  createLineChart,
  createTrendChart,
  createFragilityTimeSeriesChart
} from '@/charting'
import FragilityCurveModal from './FragilityCurveModal.vue'

// Pull Pinia refs
const {
  climateDataLoaded,
  gridData,
  variableNameMapping,
  times,
  memberIds,
  currentTimeIndex,
  selectedVariable,
  selectedGridIndex,
  selectedAsset,
  showTrendAnalysis,
  trendAnalysisResults,
  fragilityCurves,
  fragilityView,
  hbomDefinitions,
  Hazard
} = storeToRefs(useDataStore())

// Modal state + helpers
const fragilityModalData = reactive({
  componentName: '',
  xValues:       [],
  curveData:     [],
  hazardName:    '',
  model:         'weibull',
  modelParams:   {}
})
const modalTitle       = ref('Fragility Curve')
const fragModal        = ref(null)
const fullVariableName = computed(() =>
  variableNameMapping.value[selectedVariable.value] ?? selectedVariable.value
)

// 1) On mount, initialize the always-present charts (raw line + sunburst placeholder)
onMounted(async () => {
  // raw time-series line
  createLineChart(
    { gridData, times, memberIds,
      selectedVariable, selectedGridIndex,
      currentTimeIndex, climateDataLoaded },
    v => variableNameMapping.value[v] || v
  )

  // We leave sunburst/trend/fragility for the watches below,
  // since their containers depend on v-if or v-else-if
})

// 2) When showTrendAnalysis flips true, wait for nextTick then init trend chart
watch(showTrendAnalysis, async isTrend => {
  if (isTrend) {
    await nextTick()
    createTrendChart({
      trendAnalysisResults,
      showTrendAnalysis,
      selectedVariable,
      selectedGridIndex
    })
  }
})

// 3) When fragilityView changes, wait for nextTick then init the appropriate chart
watch(fragilityView, async view => {
  await nextTick()
  if (view === 'sunburst') {
    createSunburstChart(
      { fragilityCurves, fragilityView, hbomDefinitions,
        selectedAsset, selectedVariable, currentTimeIndex, Hazard },
      { fragilityModalData, fragModal }
    )
  } else if (view === 'timeseries') {
    createFragilityTimeSeriesChart({
      fragilityCurves,
      selectedVariable,
      selectedAsset,
      times,
      fragilityView
    })
  }
}, { immediate: true })
</script>