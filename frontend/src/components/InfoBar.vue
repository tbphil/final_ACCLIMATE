<template>
  <div class="bg-light p-3 h-100 overflow-auto">

    <!-- 1) MENU INFO -->
    <div v-if="menuInfo && menuInfo.lat !== null" class="card mb-4">
      <div class="card-body">
        <h6 class="card-title mb-3">Central Grid Square Contains</h6>
        <p class="card-text mb-1"><strong>Lat:</strong> {{ props.menuInfo.lat.toFixed(4) }}</p>
        <p class="card-text mb-3"><strong>Lon:</strong> {{ props.menuInfo.lon.toFixed(4) }}</p>
        <p class="card-text mb-1"><strong>Hazard:</strong> {{ props.menuInfo.hazard }}</p>
        <p class="card-text mb-1"><strong>Scenario:</strong> {{ props.menuInfo.scenario }}</p>

        <template v-if="menuInfo.aggregate_over_member_id">
          <p class="card-text mb-0">
            <strong>Aggregated</strong> ({{ props.menuInfo.aggregation_method }}
            <span v-if="menuInfo.aggregation_method === 'percentile' && menuInfo.aggregation_q !== null">
              , q={{ menuInfo.aggregation_q }}
            </span>)
          </p>
        </template>
        <template v-else>
          <p class="card-text mb-0"><strong>Model:</strong> {{ props.menuInfo.climate_model }}</p>
        </template>
      </div>
    </div>

    <!-- 1.5) AOI DEMOGRAPHICS -->
     <div v-if="climateDataLoaded && !hasDemo" class="text-muted text-center py-3">
      Loading census data...
    </div>
    <div v-if="hasDemo" class="card mb-4">
      <div class="card-header">
        <strong>AOI Demographics</strong>
      </div>
      <div class="card-body">
        <small v-if="aoiModel">
          <span v-if="Number(currentYear) > aoiModel.last_hist_year">
            Projection: {{ aoiModel.series.population.method.toUpperCase() }}
            from ACS {{ aoiModel.last_hist_year }}
          </span>
          <span v-else>Observed (ACS {{ aoiModel.last_hist_year }})</span>
        </small>
        <p class="mb-1"><strong>Year:</strong> {{ currentYear }}</p> 
        <p class="mb-1"><strong>Population at Risk:</strong> {{ fmtInt(popNow) }}</p>
        <p class="mb-1"><strong>Households at Risk:</strong> {{ fmtInt(hhNow)  }}</p>
        <p class="mb-1"><strong>Median Household Income:</strong> {{ fmtCurrency(hhiNow) }}</p>
        <p class="mb-0"><strong>Per-Capita Income:</strong> {{ fmtCurrency(pciNow) }}</p>
      </div>
    </div>

    <!-- 3) INFRASTRUCTURE LEGEND -->
    <div v-if="infrastructureDataLoaded && infrastructureData.length" class="card mb-4">
      <div class="card-header"><strong>Infrastructure Data</strong></div>
      <div class="card-body p-0">
        <ul class="list-group list-group-flush">
          <li
            v-for="type in infrastructureTypes"
            :key="type"
            class="list-group-item d-flex justify-content-between align-items-center py-2"
            @click = "$emit('asset-type-click', type)"
            role = "button"
            tabindex = "0"
            @keydown.enter = "$emit('asset-type-click', type)"
            @keydown.space.prevent = "$emit('asset-type-click',type)"
            style = "cursor:pointer"
          >
            <div class="d-flex align-items-center">
              <span
                class="d-inline-block border rounded me-2"
                :style="{ width: '16px', height: '16px', backgroundColor: colorMapping[type] || '#000' }"
              ></span>
              <span>{{ type }}</span>
            </div>
            <span class="badge bg-secondary rounded-pill">
              {{ infrastructureCounts[type] }}
            </span>
          </li>
        </ul>
      </div>
    </div>

    <!-- SELECTED ASSET DETAILS -->
    <div v-if="selectedAsset" class="card mb-4">
      <div class="card-header"><strong>{{ selectedAsset.name || 'Selected Asset Details' }}</strong></div>
      <div class="card-body">
        <div v-for="(entry, idx) in filteredAssetEntries" :key="idx" class="mb-1">
          <strong>{{ entry.key }}:</strong> {{ entry.value }}
        </div>
      </div>
    </div>

    <!-- 4) ECONOMIC METRICS -->
    <div v-if="!economicLoading && hasEconomicData" class="card mb-4">
      <div class="card-header"><strong>Economic Metrics</strong></div>
      <div class="card-body">
        <p class="mb-1"><strong>Total CAPEX:</strong> ${{ totalCapex.toLocaleString() }}</p>
        <p class="mb-1">
          <strong>Total EAL:</strong>
          <span v-if="totalEal != null">{{ Number(totalEal).toLocaleString() }}</span>
          <span v-else>—</span>
        </p>
        <p class="mb-3">
          <strong>Assets at Risk:</strong>
          <span v-if="percentAtRisk != null">{{ (percentAtRisk * 100).toFixed(1) }}%</span>
          <span v-else>—</span>
        </p>

        <template v-if="topAssets && topAssets.length">
          <h6 class="mb-2">Top 5 Assets by EAL</h6>
          <ul class="list-unstyled ps-3 mb-0">
            <li v-for="asset in topAssets" :key="asset.label">
              • {{ asset.label }}: ${{ Number(asset.eal).toLocaleString() }}
            </li>
          </ul>
        </template>
      </div>
    </div>

    <div v-else-if="economicLoading" class="text-center text-muted mb-4 py-3">
      Economic Analysis Loading…
    </div>
  </div>
</template>

<script setup>
import { computed } from 'vue'
import { useDataStore } from '@/stores/data'
import { storeToRefs } from 'pinia'

const props = defineProps({ menuInfo: { type: Object, required: false } })

/* Pinia */
const store = useDataStore()
const {
  infrastructureData,
  infrastructureDataLoaded,
  climateDataLoaded,
  colorMapping,
  economicLoading,
  selectedAsset,
  hbomDefinitions,
  ealNow,
  percentAtRiskNow,
  topAssetsNow,

  // AOI series + time axis
  populationTimeseries,
  householdsTimeseries,
  medianHHITimeseries,
  pciTimeseries,
  times,
  currentTimeIndex,
} = storeToRefs(store)

/* legend counts */
const infrastructureCounts = computed(() => {
  const counts = {}
  infrastructureData.value.forEach(it => {
    const type = it?.facilityTypeName || 'Unknown'
    counts[type] = (counts[type] || 0) + 1
  })
  return counts
})
const infrastructureTypes = computed(() => Object.keys(infrastructureCounts.value))

/* economic numbers */
const totalCapex    = computed(() => hbomDefinitions.value?.total_replacement_cost ?? 0)
const totalEal      = ealNow
const percentAtRisk = percentAtRiskNow
const topAssets     = topAssetsNow

const hasEconomicData = computed(() => {
  const ts = hbomDefinitions.value?.system_eal_timeseries
  return Array.isArray(ts) && ts.length > 0
})

/* AOI demo helpers */
function fmtCurrency (v) {
  const n = Number(v)
  if (!isFinite(n)) return '—'
  return new Intl.NumberFormat(undefined, { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(n)
}
function toYear (t) {
  try {
    if (t instanceof Date) return t.getUTCFullYear()
    if (typeof t === 'number') {
      const d = new Date(t > 1e12 ? t : t * 1000) // ms vs sec epoch
      return d.getUTCFullYear()
    }
    return new Date(t).getUTCFullYear()
  } catch { return '' }
}

const idx = computed(() => {
  const i   = Number(currentTimeIndex.value ?? 0)
  const len = (times.value?.length ?? 1)
  return Math.max(0, Math.min(i, len - 1))
})



// choose the matching (or nearest) year to the UI’s selected year
function nearestYearIndex(years, targetYear) {
  if (!Array.isArray(years) || !years.length || !Number.isFinite(targetYear)) return 0
  const exact = years.indexOf(targetYear)
  if (exact !== -1) return exact
  let bestI = 0, best = Infinity
  for (let i = 0; i < years.length; i++) {
    const d = Math.abs(years[i] - targetYear)
    if (d < best) { best = d; bestI = i }
  }
  return bestI
}

const hasDemo = computed(() =>
  Array.isArray(populationTimeseries.value) && populationTimeseries.value.length > 0
)

const currentYear = computed(() => {
  const t = times.value?.[idx.value]
  // toYear() is defined above
  return t != null ? toYear(t) : ''
})
// --- AOI demographics: index by AOI years ---
const aoiDemo   = computed(() => 
  store.aoiDemographics ??
  store.preparedData?.aoi_demographics ??
  store.preparedData?.aoiDemographics ??
  null
  )
const demoYears = computed(() => {
  const ys = aoiDemo.value?.years ?? []
  return Array.isArray(ys) ? ys.map(y => Number(y)).filter(Number.isFinite) : []
})
// model metadata (projection info) for the header badge
const aoiModel = computed(() =>
  aoiDemo.value?.model ?? null
)
// index into AOI series using AOI years
const demoIdx = computed(() => nearestYearIndex(demoYears.value, Number(currentYear.value)))

// pick values from AOI series
const popNow = computed(() => populationTimeseries.value?.[demoIdx.value]  ?? null)
const hhNow  = computed(() => householdsTimeseries.value?.[demoIdx.value]  ?? null)
const hhiNow = computed(() => medianHHITimeseries.value?.[demoIdx.value]   ?? null)
const pciNow = computed(() => pciTimeseries.value?.[demoIdx.value]         ?? null)

function fmtInt(n) {
  const v = Number(n)
  return Number.isFinite(v) ? Math.round(v).toLocaleString(): '-'
}

/* selected asset details */
const filteredAssetEntries = computed(() => {
  if (!selectedAsset.value) return []
  return Object.entries(selectedAsset.value)
    .filter(([k, v]) =>
      v !== null &&
      !['id','uuid','name','facilityTypeName','latitude','longitude','sector','source_sheet','source_workbook'].includes(k)
    )
    .map(([key, value]) => ({ key, value }))
})
</script>