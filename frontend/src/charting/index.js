// Central charting module for GraphsContainer.vue
// src/charting/index.js

import Plotly from 'plotly.js-dist'
import { computed, watch, nextTick } from 'vue'

// --- Shared layout and utilities ---
export const baseLayout = {
  margin: { l: 40, r: 20, t: 30, b: 40 },
  responsive: true,
  // add default font sizes, colors, etc. here
}

/**
 * Returns a vertical line shape at x-coordinate `pos`
 * @param {number} pos - position along the x-axis
 * @param {string} [color='black']
 */
export function verticalLineShape(pos, color = 'black') {
  return {
    type: 'line',
    x0: pos,
    x1: pos,
    yref: 'paper',
    y0: 0,
    y1: 1,
    line: { width: 2, dash: 'dash', color },
  }
}


// --- Sunburst Chart Composable ---
/**
 * @param {object} storeRefs - destructured Pinia refs for sunburst
 * @param {Ref<Record>} storeRefs.fragilityCurves
 * @param {Ref<string>}  storeRefs.fragilityView
 * @param {Ref<object>}  storeRefs.hbomDefinitions
 * @param {Ref<object>}  storeRefs.selectedAsset
 * @param {Ref<string>}  storeRefs.selectedVariable
 * @param {Ref<number>}  storeRefs.currentTimeIndex
 * @param {Ref<string>}  storeRefs.Hazard
 * @param {object} modalRefs
 * @param {Ref<object>} modalRefs.fragilityModalData
 * @param {Ref<any>}    modalRefs.fragModal
 */
export function createSunburstChart(
  {
    fragilityCurves,
    fragilityView,
    hbomDefinitions,
    selectedAsset,
    selectedVariable,
    currentTimeIndex,
    Hazard,
  },
  { fragilityModalData, fragModal }
) {
  // accumulator-based traverse
  function traverse(node, parentId, facilityType, climateVar, out) {
  const label    = node.label
  const compType = node.component_type
  const id       = parentId ? `${parentId}/${label}` : label

  // compute P(fail)
  let p_fail = 0
  const uuid = node.uuid
  const series = fragilityCurves.value[uuid]?.[climateVar]
  if (Array.isArray(series) && series.length > 0) {
    const idx = Math.min(currentTimeIndex.value, series.length - 1)
    p_fail = series[idx]
  }

  // accumulate data
  out.ids.push(id)
  out.labels.push(label)
  out.parents.push(parentId || '')
  out.custom.push({ uuid: node.uuid, compType, p_fail })

  //DEBUG: log the root/facility node values
  if (label === facilityType) {
    console.log('[Sunburst root]', {
      label,
      uuid,
      varKey: climateVar,
      ownSeriesLen: Array.isArray(fragilityCurves.value[uuid]?.[climateVar])
        ? fragilityCurves.value[uuid][climateVar].length
        : 0,
      p_fail_own: p_fail
    })
  }

  (node.subcomponents || []).forEach(child =>
    traverse(child, id, facilityType, climateVar, out)
  )
}
  // computed sunburst data
  const sunburstData = computed(() => {
    if (fragilityView.value !== 'sunburst') return null
    if (!selectedAsset.value) return null
    const facilityType = selectedAsset.value.facilityTypeName
    if (!facilityType) return null
    const hbomNode = hbomDefinitions.value?.components?.find(c => 
    c.label === facilityType || 
    c.canonical_component_type === facilityType ||
    c.aliases?.includes(facilityType)
  )
    if (!hbomNode) return null

    const out = { ids: [], labels: [], parents: [], custom: [] }
    traverse(hbomNode, null, facilityType, selectedVariable.value, out)
    return out
  })

  // click handler for fragility modal
  function handleClick(eventData) {
    const pt = eventData.points?.[0]
    if (!pt) return

    const { uuid } = pt.customdata
    const varKey = selectedVariable.value
    
    console.log('Click:', pt.label, 'UUID:', uuid, 'Looking for var:', varKey)
    console.log('Available curves for this UUID:', Object.keys(fragilityCurves.value[uuid] || {}))
    
    const series = fragilityCurves.value[uuid]?.[varKey]
    
    if (!Array.isArray(series) || series.length === 0) {
      console.warn('No fragility data for', pt.label, 'variable:', varKey)
      return
    }

    Object.assign(fragilityModalData, {
      componentName: pt.label,
      curveData:     series,
      hazardName:    Hazard.value,
    })

    // inline proxy will open the panel; floating modal will open overlay
    fragModal.value?.show()
    nextTick(() => fragModal.value?.plotFragilityCurve())
  }

  // plotting function
  function plot(data) {
    const div = document.getElementById('sunburst-plot')
    if (!div) return
    const cfg = {
      low: 0.25,
      high: 0.51,
      colors: { low: 'green', medium: 'yellow', high: 'red' },
      ...(Hazard.value === 'Heat Stress' ? {} : {}) //placeholder for hazard specific configurations
    }
    const colours = data.custom.map(v => {
  if (v.p_fail === 0) return '#CCCCCC'  // BSU orange - no curve
  return v.p_fail < cfg.low ? cfg.colors.low
       : v.p_fail < cfg.high ? cfg.colors.medium
       : cfg.colors.high
})
    const trace = {
      type: 'sunburst',
      ids: data.ids,
      labels: data.labels,
      parents: data.parents,
      customdata: data.custom,
      marker: { colors: colours },
      hovertemplate: '<b>%{label}</b><br>P(fail): %{customdata.p_fail:.2%}<extra></extra>',
    }
    // avoid stacking handlers
    div.removeAllListeners?.('plotly_click')
    Plotly.react(div, [trace], baseLayout)
    div.on('plotly_click', handleClick)
  }

  // watch for data *and* view toggles; draw only when fragilityView === 'sunburst'
  const stop = watch(
    [() => sunburstData.value, () => fragilityView.value],
    ([data, view]) => {
      nextTick(() => {
        const div = document.getElementById('sunburst-plot')
        if (view !== 'sunburst' || !data) {
          if (div) {
            div.removeAllListeners?.('plotly_click')
            Plotly.purge(div)
          }
          return
        }
        plot(data)
      })
    },
    { immediate: true, flush: 'post' }
  )

  // return cleanup to caller (no lifecycle calls here)
  return () => {
    if (typeof stop === 'function') stop()
    const div = document.getElementById('sunburst-plot')
    if (div) {
      div.removeAllListeners?.('plotly_click')
      Plotly.purge(div)
    }
  }
}


// --- Line Chart Composable ---
/**
  * @param {object} storeRefs
 * @param {Ref<Array>}  storeRefs.gridData
 * @param {Ref<Array>}  storeRefs.times
 * @param {Ref<Array>}  storeRefs.memberIds
 * @param {Ref<string>} storeRefs.selectedVariable
 * @param {Ref<number>} storeRefs.selectedGridIndex
 * @param {Ref<number>} storeRefs.currentTimeIndex
 * @param {Ref<boolean>} storeRefs.climateDataLoaded
 * @param {Function}    getYAxisTitle    – fn(v) ⇒ axis label
 */
export function createLineChart(
  {
    gridData,
    times,
    memberIds,
    selectedVariable,
    selectedGridIndex,
    currentTimeIndex,
    climateDataLoaded,
  },
  getYAxisTitle
) {
  // 1) Build trace data
  const lineData = computed(() => {
    if (!climateDataLoaded.value) return null
    const v     = selectedVariable.value
    const asset = gridData.value[selectedGridIndex.value]
    if (!asset) return null
    const variableData = asset.climate?.[v]
    if (!variableData?.length) return null

    const traces = Array.isArray(variableData[0])
      ? memberIds.value.map((id, i) => ({
          x: times.value,
          y: variableData[i],
          mode: 'lines',
          name: id,
        }))
      : [{
          x: times.value,
          y: variableData,
          mode: 'lines',
          name: getYAxisTitle(v),
        }]

    return { v, traces }
  })

  // 2) Plot function, with a debug log
  function plotLine({ v, traces }) {
    console.log(`createLineChart ▶ plotting ${v}`)
    const div = document.getElementById(`graph-${v}`)
    if (!div) {
      console.warn('createLineChart: no div for', v)
      return
    }
    const t      = times.value[currentTimeIndex.value]
    Plotly.react(div, traces, {
      ...baseLayout,
      title: `${getYAxisTitle(v)} Over Time`,
      xaxis: { title: 'Time' },
      yaxis: { title: getYAxisTitle(v) },
      shapes: [ verticalLineShape(t, '#8dc340') ],
    })
  }

  // 3) Watch for data or variable changes → initial draw (after DOM is ready)
  const stopLine = watch(
    () => lineData.value,
    (data) => {
      if (!data) return
      nextTick(() => plotLine(data))
    },
    { immediate: true }
  )

  // 4) Watch the slider → update only the vertical line (after DOM is ready)
  const stopMarker = watch(
    () => currentTimeIndex.value,
    (idx) => {
      const data = lineData.value
      if (!data) return
      nextTick(() => {
        const div = document.getElementById(`graph-${data.v}`)
        if (!div?._fullLayout) return
        const t = times.value[idx]
        Plotly.relayout(div, {
          shapes: [ verticalLineShape(t, '#8dc340') ]
        })
      })
    },
    { immediate: false }
  )

  // 5) Return cleanup function
  return () => {
    stopLine()
    stopMarker()
  }
  }


// --- Trend Analysis Chart Composable ---
/**
 * Trend analysis chart (line + histogram)
 * @param {object} storeRefs
 * @param {Ref<object>} storeRefs.trendAnalysisResults - map from variable to trend data or array of trend data
 * @param {Ref<boolean>} storeRefs.showTrendAnalysis   - toggle between climate and trend view
 * @param {Ref<string>}  storeRefs.selectedVariable    - current variable
 * @param {Ref<number>}  storeRefs.selectedGridIndex   - current grid index
 */
export function createTrendChart(
  {
    trendAnalysisResults,
    showTrendAnalysis,
    selectedVariable,
    selectedGridIndex,
  }
) {
  // computed data object
  const trendData = computed(() => {
    if (!showTrendAnalysis.value) return null
    const v = selectedVariable.value
    const arr = trendAnalysisResults.value[v]
    if (!arr) return null
    // if array per grid, pick index
    return Array.isArray(arr) ? arr[selectedGridIndex.value] : arr
  })

  // plotting function
  function plotTrend(data) {
    const v = selectedVariable.value
    console.log(`createTrendChart: plotting variable ${v}`);
    const trendDiv = document.getElementById(`analysis-plot-${v}`)
    const histDiv  = document.getElementById(`histogram-plot-${v}`)
    if (!trendDiv || !histDiv) return

    const {
      composite_metric,
      dates,
      trend_line,
      slope,
      mean_value,
      median_value,
      std_dev,
      histogram_bins,
      histogram_counts,
    } = data
    // line traces
    const lineTrace = { x: dates, y: composite_metric, mode: 'lines', name: 'Composite', line: {} }
    const trendTrace= { x: dates, y: trend_line,      mode: 'lines', name: 'Trend',    line: {} }
    Plotly.react(trendDiv, [lineTrace, trendTrace], {
      ...baseLayout,
      title: `Trend Analysis (${v})`,
      xaxis: { title: 'Time' },
      yaxis: { title: 'Composite Metric' },
      annotations: [
        { x: dates[0], y: Math.max(...composite_metric), text: `Slope: ${slope.toFixed(4)}`, showarrow: false }
      ],
    })

    // histogram
    const histX = []
    const histY = []
    const colors = []
    const lower = mean_value - 2*std_dev
    const upper = mean_value + 2*std_dev
    for (let i=0; i<histogram_counts.length; i++) {
      const center = (histogram_bins[i] + histogram_bins[i+1]) / 2
      histX.push(center)
      histY.push(histogram_counts[i])
      colors.push(center < lower || center > upper ? 'red' : 'blue')
    }
    const histTrace = { x: histX, y: histY, type: 'bar', marker: { color: colors } }
    Plotly.react(histDiv, [histTrace], {
      ...baseLayout,
      title: `Frequency (${v})`,
      xaxis: { title: 'Composite Metric' },
      yaxis: { title: 'Count' },
      shapes: [
        { type:'line', x0: mean_value, x1: mean_value, y0:0, y1:1, yref:'paper', line:{ dash:'dash', color:'black' } },
        { type:'line', x0: median_value, x1: median_value, y0:0, y1:1, yref:'paper', line:{ dash:'dash', color:'gray' } }
      ],
      annotations: [
        { x: mean_value, text: `Mean`, showarrow:false },
        { x: median_value, text: `Median`, showarrow:false }
      ],
    })
  }

  // watch and cleanup
  const stop = watch(
    () => trendData.value,
    (data) => {
      if (data) plotTrend(data)
      else {
        // purge if toggled off or no data
        const v = selectedVariable.value
        const tDiv = document.getElementById(`analysis-plot-${v}`)
        const hDiv = document.getElementById(`histogram-plot-${v}`)
        if (tDiv) Plotly.purge(tDiv)
        if (hDiv) Plotly.purge(hDiv)
      }
    },
    { immediate: true }
  )

  // Return cleanup to caller
  return () => stop()
}


/**
 * Composable for plotting aggregated fragility time series of the selected asset
 * @param {object} storeRefs
 * @param {Ref<object>} storeRefs.fragilityCurves     - map of fragility curves keyed by "facility|component|variable"
 * @param {Ref<string>} storeRefs.selectedVariable     - current climate variable key
 * @param {Ref<object>} storeRefs.selectedAsset        - selected infrastructure asset
 * @param {Ref<Array>}  storeRefs.times                - time axis values matching fragility curves
 * @param {Ref<string>} storeRefs.fragilityView        - toggle between sunburst and timeseries
 */
export function createFragilityTimeSeriesChart(
  { fragilityCurves, times, selectedAsset, selectedVariable, fragilityView }
) {
  // 1) POF time-series for the *selected* asset + variable
  const seriesData = computed(() => {
    const asset  = selectedAsset.value
    const varKey = selectedVariable.value
    if (!asset || !varKey) return null

    // uuid-based lookup (existing structure)
    const uuid   = asset.uuid
    const series = fragilityCurves.value?.[uuid]?.[varKey]
    return Array.isArray(series) ? series : null
  })

  // 2) the actual Plotly plot routine
  function plot(data) {
    const div = document.getElementById('fragility-timeseries-plot')
    if (!div || !data) return
    const trace = {
      x: times.value,
      y: data,
      type: 'scatter',
      mode: 'lines+markers',
      hovertemplate: '%{x|%b %d, %Y}: %{y:.1%}<extra></extra>',
    }
    Plotly.react(div, [trace], { margin: { t: 20, b: 40 } })
  }

  // 3) watch for series changes (data) and view toggles
  const stopSeries = watch(seriesData, (val) => {
    nextTick(() => plot(val))
  }, { immediate: true })

  const stopView = watch(
    [() => seriesData.value, () => fragilityView.value],
    ([data, view]) => {
      const div = document.getElementById('fragility-timeseries-plot')
      if (view !== 'timeseries') {
        if (div) Plotly.purge(div)
        return
      }
      if (data) {
        nextTick(() => plot(data))
      }
    },
    { immediate: true, flush: 'post' }
  )

  // return cleanup to caller (no lifecycle calls here)
  return () => {
    if (typeof stopSeries === 'function') stopSeries()
    if (typeof stopView === 'function') stopView()
    const div = document.getElementById('fragility-timeseries-plot')
    if (div) Plotly.purge(div)
  }
}