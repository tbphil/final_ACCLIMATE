<template>
  <div id="app">
    <!-- The Header -->
    <INLHeader
      :climate-loaded="climateDataLoaded"
      @open-climate="toggleClimateModal"
      @open-infrastructure="toggleInfrastructureModal"
      @open-hbom="showHbomModal = true"
    />

    <!-- The Extreme Weather Modal -->
    <transition name="fade">
      <div
        v-if="showClimateModal"
        class="modal fade show"
        tabindex="-1"
        style="display: block;"
        aria-hidden="false"
      >
        <div class="modal-dialog modal-lg">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title">
                Extreme Weather Parameters
              </h5>
              <button
                type="button"
                class="btn-close"
                aria-label="Close"
                @click="toggleClimateModal"
              />
            </div>
            <div class="modal-body">
              <div v-if="loading" class="spinner" />
              <ClimateNavMenu
                :loading="loading"
                :error-message="errorMessage"
                :success-message="successMessage"
                @submit-climate="submitClimate"
              />
            </div>
          </div>
        </div>
      </div>
    </transition>

    <!-- HBOM Modal -->
    <AddHBOM v-model="showHbomModal" />

    <!-- Infrastructure Modal -->
    <transition name="fade">
      <div
        v-if="showInfrastructureModal"
        class="modal fade show"
        tabindex="-1"
        style="display: block;"
        aria-hidden="false"
      >
        <div class="modal-dialog">
          <div class="modal-content">
            <div class="modal-header">
              <h5 class="modal-title">
                Critical Infrastructure
              </h5>
              <button
                type="button"
                class="btn-close"
                aria-label="Close"
                @click="toggleInfrastructureModal"
              />
            </div>
            <div class="modal-body">
              <div v-if="loading" class="spinner" />
              <div v-if="errorMessage" class="alert alert-danger">
                {{ errorMessage }}
              </div>
              <div v-if="successMessage" class="alert alert-success">
                {{ successMessage }}
              </div>
              <InfrastructureNavMenu
                @submit-infrastructure="submitInfrastructure"
                @file-upload="handleInfrastructureFile"
              />
            </div>
          </div>
        </div>
      </div>
    </transition>

    <!-- Main Flex Row -->
    <div
      class="d-flex"
      :style="{ marginRight: showInfoBar ? '350px' : '0' }"
      style="margin-top: 5vh; height: calc(100vh - 5vh);"
    >
      <div class="flex-grow-1 d-flex flex-column">
        <div
          class="map-wrapper"
          :class="climateDataLoaded ? 'map-partial' : 'map-full'"
        >
          <MapSection @asset-selected="onAssetSelected" />
        </div>

        <div v-if="climateDataLoaded" class="p-2" style="overflow:auto; flex:1;">
          <DataControls class="p-2" />
          <GraphsContainer class="mt-3" />
        </div>
      </div>
    </div>

    <div v-if="showInfoBar" class="info-bar">
      <InfoBar 
        :menu-info="lastRequest" 
        @asset-type-click="onAssetTypeClick" 
      />
    </div>

    <HBOMAnalyticModal 
      v-if="showHbomAnalyticModal" 
      :asset-type="lastClickedType"
      @close="showHbomAnalyticModal = false" 
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import INLHeader from '@/components/INLHeader.vue'
import ClimateNavMenu from '@/components/ClimateNavMenu.vue'
import AddHBOM from '@/components/addHBOM.vue'
import InfrastructureNavMenu from '@/components/InfrastructureNavMenu.vue'
import MapSection from '@/components/MapSection.vue'
import DataControls from '@/components/DataControls.vue'
import GraphsContainer from '@/components/GraphsContainer.vue'
import InfoBar from '@/components/InfoBar.vue'
import HBOMAnalyticModal from '@/components/HBOMAnalyticModal.vue'

import { useDataStore } from '@/stores/data'
import { storeToRefs } from 'pinia'

const store = useDataStore()
const {
  Hazard,
  sector,
  gridData,
  selectedVariable, 
  climateDataLoaded,  
  hbomDefinitions,
  showInfoBar, 
  lastRequest,
} = storeToRefs(store)

const {
  setHazard, 
  setSector, 
  setGridData, 
  setVariables, 
  setTimes,
  setMemberIds, 
  setSelectedVariable, 
  setSelectedGridIndex,
  setPreparedData,
  markDataLoaded, 
  markInfrastructureLoaded,
  setShowInfoBar,
  setInfrastructure, 
  setSelectedAsset, 
  generateColorMapping,
  setFragilityCurves, 
  setHbomDefinitions,
  setTrendAnalysisResults,
} = store

const showClimateModal = ref(false)
const showInfrastructureModal = ref(false)
const showHbomModal = ref(false)
const showHbomAnalyticModal = ref(false)

const lastClickedType = ref(null)
const apiBaseUrl = 'http://127.0.0.1:8000'
const climateBbox = ref(null)

const loading = ref(false)
const successMessage = ref('')
const errorMessage = ref('')

const hbomByUuid = computed(() => {
  const map = {}
  if (hbomDefinitions.value && hbomDefinitions.value.components) {  // Fixed: components not children
    (function walk(node){
      map[node.uuid] = node
      if (node.subcomponents) node.subcomponents.forEach(walk)  // Fixed: subcomponents not children
    })(hbomDefinitions.value)
  }
  return map
})

onMounted(() => {
  fetch(`${apiBaseUrl}/api/clear-cache`, { method: 'POST' })
    .then((res) => {
      if (res.ok) {
        console.log('Cache cleared request succeeded.')
        return res.json()
      } else {
        console.error('Cache clear request failed with status', res.status)
      }
    })
    .then((json) => {
      if (json) console.log('Server response:', json.message)
    })
    .catch((err) => {
      console.error('Error sending clear-cache request:', err)
    })
})

const submitClimate = async (formData) => {
  setSector(formData.sector || 'Energy Grid')
  setHazard(formData.hazard || "Heat Stress")

  const hasPoint = formData.lat !== undefined && formData.lon !== undefined
  const hasBbox = formData.min_lat !== undefined
  
  let displayLat, displayLon
  if (hasBbox) {
    displayLat = (formData.min_lat + formData.max_lat) / 2
    displayLon = (formData.min_lon + formData.max_lon) / 2
  } else {
    displayLat = formData.lat
    displayLon = formData.lon
  }
  
  store.setLastRequest({
    ...formData,
    lat: displayLat,
    lon: displayLon
  })
  
  if (!hasPoint && !hasBbox) {
    errorMessage.value = 'Must select a location on the map or enter coordinates.'
    console.error('No spatial selection provided')
    return
  }

  loading.value = true
  successMessage.value = ''
  errorMessage.value = ''

  try {
    const climateRequest = {
      hazard: formData.hazard,
      scenario: formData.scenario,
      domain: formData.domain,
      prior_years: formData.prior_years,
      future_years: formData.future_years,
      climate_model: formData.climate_model,
      aggregate_over_member_id: formData.aggregate_over_member_id,
      aggregation_method: formData.aggregation_method,
      aggregation_q: formData.aggregation_q,
      sector: sector.value,
    }

    if (hasBbox) {
      climateRequest.min_lat = formData.min_lat
      climateRequest.max_lat = formData.max_lat
      climateRequest.min_lon = formData.min_lon
      climateRequest.max_lon = formData.max_lon
    } else {
      climateRequest.lat = formData.lat
      climateRequest.lon = formData.lon
      climateRequest.num_cells = formData.num_cells
    }

    const climateResp = await fetch(`${apiBaseUrl}/api/get-climate`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(climateRequest),
    })
    if (!climateResp.ok) {
      throw new Error(`Climate fetch error: ${climateResp.status}`)
    }

    const climateData = await climateResp.json()
    setPreparedData(climateData)
    setTrendAnalysisResults(climateData?.climate_analysis?.analysis_results || {})
    setGridData(climateData.data || [])
    climateBbox.value = climateData.bounding_box || null

    let centerLat, centerLon
    if (hasBbox) {
      centerLat = (formData.min_lat + formData.max_lat) / 2
      centerLon = (formData.min_lon + formData.max_lon) / 2
    } else {
      centerLat = formData.lat
      centerLon = formData.lon
    }
    const found = findGridIndex(centerLat, centerLon)
    if (found !== -1) setSelectedGridIndex(found)

    if (Array.isArray(climateData.times) && climateData.times.length) {
      const iso = climateData.times.map(t => new Date(t).toISOString().split('T')[0])
      setTimes(iso)
    }

    const derivedMemberIds = Array.isArray(climateData.members)
      ? climateData.members.map(m => m?.member_id).filter(Boolean)
      : null
    setMemberIds(derivedMemberIds)

    if (Array.isArray(climateData.variables) && climateData.variables.length) {
      const mapping = {}
      climateData.variables.forEach((code, i) => {
        mapping[code] = climateData.variable_long_names[i] ?? code
      })
      setVariables(climateData.variables, mapping)

      if (!selectedVariable.value) {
        setSelectedVariable(climateData.variables[0])
      }
    }
    
    markDataLoaded('climate')
    setShowInfoBar(true)
    showClimateModal.value = false

    //Load HBOM and Fragility
    await loadHBOMWithFragility()
    
    // Lazy load census data (non-blocking)
    successMessage.value = 'Climate data loaded! Census data loading...'
    fetchPopulationAtRisk(climateData.bounding_box)
      .then(() => console.log('✓ Census data loaded'))
      .catch(err => console.error('Census data fetch failed:', err))

  } catch (err) {
    errorMessage.value = 'Error loading climate data.'
    console.error('Climate error:', err)
  } finally {
    loading.value = false
  }
}

function buildYearsForAOI(times) {
  if (Array.isArray(times) && times.length) {
    const years = times.map(t => {
      try {
        if (t instanceof Date) return t.getUTCFullYear()
        if (typeof t === 'number') {
          const d = new Date(t > 1e12 ? t : t * 1000)
          return d.getUTCFullYear()
        }
        return new Date(t).getUTCFullYear()
      } catch { return null }
    }).filter(y => Number.isInteger(y))
    return [...new Set(years)].sort((a, b) => a - b)
  }
  return [new Date().getUTCFullYear()]
}

const fetchPopulationAtRisk = async (bbox) => {
  try {
    const years = buildYearsForAOI(store.times)

    const resp = await fetch(`${apiBaseUrl}/api/get-census-population`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        bbox: {
          min_lat: bbox.min_lat,
          max_lat: bbox.max_lat,
          min_lon: bbox.min_lon,
          max_lon: bbox.max_lon,
        },
        years
      }),
    })
    if (!resp.ok) throw new Error(`Census fetch error: ${resp.status}`)

    const data = await resp.json()
    store.setPreparedData({
      ...(store.preparedData || {}),
      aoi_demographics: data.aoi_demographics
    })
  } catch (err) {
    console.error('fetchPopulationAtRisk failed:', err)
    store.setPreparedData({
      aoi_demographics: {
        years: [],
        population: [],
        households: [],
        median_hhi: [],
        per_capita_income: [],
      },
    })
  }
}

const submitInfrastructure = async () => {
  if (!climateBbox.value) {
    console.warn('No bounding box from climate. Load climate first.')
    return
  }

  loading.value = true
  errorMessage.value = ''
  successMessage.value = ''

  try {
    const infraResp = await fetch(`${apiBaseUrl}/api/get-infrastructure`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        sector: sector.value,
        hazard: Hazard.value,
        min_lat: climateBbox.value.min_lat,
        max_lat: climateBbox.value.max_lat,
        min_lon: climateBbox.value.min_lon,
        max_lon: climateBbox.value.max_lon,
      }),
    })
    if (!infraResp.ok) throw new Error(`Infrastructure fetch error ${infraResp.status}`)

    const infraJson = await infraResp.json()
    setInfrastructure(infraJson.infrastructure)
    generateColorMapping()
    markInfrastructureLoaded()
    toggleInfrastructureModal()

    successMessage.value = 'Infrastructure loaded!'
  } catch (err) {
    console.error('Infrastructure error:', err)
    errorMessage.value = 'Error loading infrastructure.'
  } finally {
    loading.value = false
  }
}

// NEW: Single call that gets HBOM tree + computed fragility curves
const loadHBOMWithFragility = async () => {
  try {
    const sec = sector.value || 'Energy Grid'
    const haz = Hazard.value || 'Heat Stress'
    
    const url = `${apiBaseUrl}/api/fragility/compute/${encodeURIComponent(sec)}/${encodeURIComponent(haz)}`
    
    const resp = await fetch(url)
    if (!resp.ok) throw new Error(`Fragility compute error: ${resp.status}`)
    
    const data = await resp.json()
    console.log('Loaded HBOM with fragility curves:', data)
    
    // Store complete tree
    setHbomDefinitions(data)
    
    // Extract time series for charting (uuid -> var -> [pof_timeseries])
    const curves = extractFragilityTimeSeries(data)
    setFragilityCurves(curves)
    
  } catch (err) {
    console.error('Failed to load HBOM with fragility:', err)
  }
}

// Helper: extract {uuid: {var: [series]}} from tree with embedded fc_values
const extractFragilityTimeSeries = (hbom) => {
  const result = {}
  
  const traverse = (node) => {
    const hazData = node?.hazards?.[Hazard.value]
    if (hazData?.fragility_curves) {
      result[node.uuid] = {}
      
      Object.entries(hazData.fragility_curves).forEach(([varName, gridDict]) => {
        // Take max across grids or just use grid 0
        const grid0 = gridDict['0'] || gridDict[0]
        if (grid0?.fc_values) {
          result[node.uuid][varName] = grid0.fc_values
        }
      })
    }
    (node.subcomponents || []).forEach(child => traverse(child))
  }
  
  (hbom.components || []).forEach(comp => traverse(comp))
  return result
}

const toggleClimateModal = () => {
  showClimateModal.value = !showClimateModal.value
}

const toggleInfrastructureModal = () => {
  if (!climateDataLoaded.value && !showInfrastructureModal.value) return
  showInfrastructureModal.value = !showInfrastructureModal.value
}

const findGridIndex = (lat, lon) => {
  let foundIndex = -1
  let minDistance = Infinity

  gridData.value.forEach((grid, idx) => {
    const { min_lat, min_lon, max_lat, max_lon } = grid.bounds
    const centerLat = (min_lat + max_lat) / 2
    const centerLon = (min_lon + max_lon) / 2
    const dist = Math.hypot(centerLat - lat, centerLon - lon)
    if (dist < minDistance) {
      minDistance = dist
      foundIndex = idx
    }
  })
  return foundIndex
}

const onAssetSelected = (asset) => {
  const sel = hbomByUuid.value[asset.id] ?? hbomByUuid.value[asset.uuid] ?? asset
  setSelectedAsset(sel)

  const idx = findGridIndex(asset.latitude, asset.longitude)
  setSelectedGridIndex(idx)
}

const handleInfrastructureFile = async ({ file, sector }) => {
  if (!climateBbox.value) {
    errorMessage.value = 'Please load climate data first to establish area of interest.'
    console.warn('No bounding box; load climate data first.')
    return
  }

  loading.value = true
  errorMessage.value = ''
  successMessage.value = ''

  const formData = new FormData()
  formData.append('file', file)
  formData.append('sector', sector)
  formData.append('save_to_database', 'false')  // Preview only, don't persist
  
  // Add bounding box for filtering
  formData.append('min_lat', String(climateBbox.value.min_lat))
  formData.append('max_lat', String(climateBbox.value.max_lat))
  formData.append('min_lon', String(climateBbox.value.min_lon))
  formData.append('max_lon', String(climateBbox.value.max_lon))

  try {
    const resp = await fetch(`${apiBaseUrl}/upload`, {
      method: 'POST',
      body: formData,
    })
    if (!resp.ok) throw new Error(`Upload failed: ${resp.status}`)

    const result = await resp.json()
    
    // Check for errors
    if (!result.success) {
      errorMessage.value = result.error || 'Upload failed'
      return
    }
    
    // Handle response format (backward compatible)
    const infrastructureData = result.data || result
    
    // Log mapping metadata for debugging
    if (result.metadata) {
      console.log('Field mappings:', result.metadata.field_mappings)
      console.log('Component mappings:', result.metadata.component_mappings)
      console.log(`Loaded ${infrastructureData.length} assets in bounding box`)
    }
    
    // DEBUG: Log first asset structure
    if (infrastructureData.length > 0) {
      console.log('First asset structure:', JSON.stringify(infrastructureData[0], null, 2))
      console.log('First asset lat:', infrastructureData[0].latitude)
      console.log('First asset lon:', infrastructureData[0].longitude)
      console.log('First asset facilityTypeName:', infrastructureData[0].facilityTypeName)
    }
    
    setInfrastructure(infrastructureData)
    generateColorMapping()
    markInfrastructureLoaded()
    toggleInfrastructureModal()
    
    successMessage.value = `File loaded successfully! ${infrastructureData.length} assets in area of interest.`
  } catch (err) {
    console.error('Error uploading file:', err)
    errorMessage.value = `Error uploading: ${err.message || err}`
  } finally {
    loading.value = false
  }
}

const onAssetTypeClick = (type) => {
  lastClickedType.value = type
  store.setSelectedAsset({ facilityTypeName: type })
  
  // Check if HBOM exists for this type
  const hasHbom = hbomDefinitions.value?.components?.find(c => 
    c.label === type || 
    c.canonical_component_type === type ||
    c.aliases?.includes(type)
  )
  
  if (!hasHbom) {
    console.log(`No HBOM decomposition for ${type}`)
    return
  }
  
  if (store.fragilityView !== 'sunburst') {
    store.toggleFragilityView()
  }
  showHbomAnalyticModal.value = true
}
</script>
