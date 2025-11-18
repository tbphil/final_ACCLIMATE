<template>
  <div class="container-fluid p-0 h-100">
    <l-map
      ref="mapRef"
      :zoom="zoom"
      :center="center"
      class="leaflet-map w-100 h-100"
      @ready="onMapReady"
    >
      <l-tile-layer
        :url="url"
        :attribution="attribution"
      />

      <!-- Climate Grid Layer Group -->
      <l-layer-group v-if="showClimateGrid">
        <l-rectangle
          v-for="(grid, index) in gridData"
          :key="'grid-' + index"
          :bounds="convertBounds(grid)"
          color="black"
          :fillColor="getFillColor(grid)"
          :fillOpacity="0.7"
          pane="gridPane"
        ></l-rectangle>
      </l-layer-group>

      <!-- Infrastructure Markers Layer Group -->
      <l-layer-group v-if="showInfrastructureMarkers">
        <l-marker
          v-for="(item, idx) in infrastructureData"
          :key="item.id ?? idx"
          :lat-lng="[item.latitude, item.longitude]"
          :icon="getMarkerIcon(item)"
          pane="markerPane"
          @click="onInfrastructureClick(item)"
        />
      </l-layer-group>

      <!-- Clicked Point Marker -->
      <l-marker
        v-if="clickedLat !== null && clickedLon !== null"
        :lat-lng="[clickedLat, clickedLon]"
        :icon="clickedLocationIcon"
        pane="markerPane"
      >
        <l-tooltip :options="{ permanent: false, direction: 'top' }">
          Selected: {{ clickedLat.toFixed(4) }}, {{ clickedLon.toFixed(4) }}
        </l-tooltip>
      </l-marker>

      <!-- Drawn Bounding Box -->
      <l-rectangle
        v-if="drawnBbox"
        :bounds="[[drawnBbox.min_lat, drawnBbox.min_lon], [drawnBbox.max_lat, drawnBbox.max_lon]]"
        :color="'#8dc340'"
        :weight="3"
        :fill-opacity="0.1"
        pane="markerPane"
      />

      <!-- Drawing Mode Toggle Button -->
      <l-control position="topright">
        <button 
          class="btn btn-sm"
          :class="drawingEnabled ? 'btn-success' : 'btn-outline-secondary'"
          @click="toggleDrawing"
          style="box-shadow: 0 2px 4px rgba(0,0,0,0.2);"
        >
          {{ drawingEnabled ? '✓ Drawing Mode ON' : 'Enable Drawing' }}
        </button>
        
        <!-- Add this button -->
        <button
          v-if="drawnBbox"
          class="btn btn-sm btn-outline-secondary ms-1"
          @click="dataStore.clearCoordinateSelection()"
          style="box-shadow: 0 2px 4px rgba(0,0,0,0.2);"
        >
          Clear Selection
        </button>
      </l-control>

      <!-- Legend Control -->
      <l-control v-if="climateDataLoaded" position="bottomleft">
        <div id="legend" class="legend"></div>
      </l-control>
    </l-map>
  </div>
</template>

<script setup>
import { ref, watch, nextTick, computed } from 'vue'
import { storeToRefs } from 'pinia'
import { useDataStore } from '@/stores/data'

import {
  LMap,
  LTileLayer,
  LLayerGroup,
  LRectangle,
  LMarker,
  LControl,
  LTooltip,
} from '@vue-leaflet/vue-leaflet'
import 'leaflet/dist/leaflet.css'
import L from 'leaflet'

const dataStore = useDataStore()
const {
  zoom,
  center,
  url,
  attribution,
  showClimateGrid,
  gridData,
  showInfrastructureMarkers,
  infrastructureData,
  climateDataLoaded,
  selectedVariable,
  currentTimeIndex,
  colorMapping,
  variableNameMapping,
  clickedLat,
  clickedLon,
  drawnBbox,
} = storeToRefs(dataStore)

const { variableRange, colourInterpolator } = storeToRefs(dataStore)
watch(gridData, (gd) => {
  if (gd?.[0]) console.log('First cell bounds:', gd[0].bounds)
  if (gd?.[639]) console.log('Last cell bounds:', gd[639].bounds)
}, { immediate: true })
const min = computed(() => variableRange.value.min)
const max = computed(() => variableRange.value.max)
const mapRef = ref(null)
const mapObject = ref(null)

const isDrawing = ref(false)
const startLatLng = ref(null)
const tempRectangle = ref(null)
const drawingEnabled = ref(false)

const clickedLocationIcon = L.divIcon({
  className: 'clicked-marker-icon',
  html: `<div style="
    background-color: #8dc340;
    width: 20px;
    height: 20px;
    border-radius: 50%;
    border: 3px solid white;
    box-shadow: 0 2px 4px rgba(0,0,0,0.3);
  "></div>`,
  iconSize: [20, 20],
  iconAnchor: [10, 10],
})

function onMapReady() {
  const m = mapRef.value?.leafletObject
  if (!m) return
  mapObject.value = m

  createCustomPanes()
  setupMapInteraction(m)

  if (climateDataLoaded.value && gridData.value.length) {
    recenterMap()
    nextTick(updateLegend)
  }
}

function toggleDrawing() {
  drawingEnabled.value = !drawingEnabled.value
  if (drawingEnabled.value) {
    console.log('Drawing mode enabled - click or drag to select area')
  } else {
    console.log('Drawing mode disabled - normal map navigation')
  }
}

function setupMapInteraction(map) {
  map.on('mousedown', (e) => {
    if (!drawingEnabled.value) return
    if (e.originalEvent.target.classList.contains('leaflet-marker-icon')) return
    
    isDrawing.value = true
    startLatLng.value = e.latlng
    
    tempRectangle.value = L.rectangle(
      [e.latlng, e.latlng],
      {
        color: '#8dc340',
        weight: 3,
        fillOpacity: 0.1,
      }
    ).addTo(map)
    
    map.dragging.disable()
  })

  map.on('mousemove', (e) => {
    if (!isDrawing.value || !tempRectangle.value) return
    const bounds = L.latLngBounds(startLatLng.value, e.latlng)
    tempRectangle.value.setBounds(bounds)
  })

  map.on('mouseup', (e) => {
    if (!isDrawing.value) return
    
    isDrawing.value = false
    map.dragging.enable()
    
    if (!startLatLng.value) return
    
    const endLatLng = e.latlng
    const latDiff = Math.abs(endLatLng.lat - startLatLng.value.lat)
    const lonDiff = Math.abs(endLatLng.lng - startLatLng.value.lng)
    
    if (tempRectangle.value) {
      tempRectangle.value.remove()
      tempRectangle.value = null
    }
    
    if (latDiff < 0.01 && lonDiff < 0.01) {
      const lat = startLatLng.value.lat
      const lng = startLatLng.value.lng
      console.log(`Point clicked at: ${lat.toFixed(4)}, ${lng.toFixed(4)}`)
      dataStore.setClickedCoordinates(lat, lng)
      drawingEnabled.value = false
    } else {
      const bbox = {
        min_lat: Math.min(startLatLng.value.lat, endLatLng.lat),
        max_lat: Math.max(startLatLng.value.lat, endLatLng.lat),
        min_lon: Math.min(startLatLng.value.lng, endLatLng.lng),
        max_lon: Math.max(startLatLng.value.lng, endLatLng.lng),
      }
      console.log('Bounding box drawn:', bbox)
      dataStore.setDrawnBbox(bbox)
      drawingEnabled.value = false
    }
    
    startLatLng.value = null
  })
}

function createCustomPanes() {
  const m = mapObject.value
  if (!m) return
  if (!m.getPane('gridPane')) {
    m.createPane('gridPane')
    m.getPane('gridPane').style.zIndex = 400
  }
  if (!m.getPane('markerPane')) {
    m.createPane('markerPane')
    m.getPane('markerPane').style.zIndex = 600
  }
}

function convertBounds(grid) {
  if (!grid?.bounds) return null
  const { min_lat, min_lon, max_lat, max_lon } = grid.bounds
  return [
    [min_lat, min_lon],
    [max_lat, max_lon],
  ]
}

function getColorForValue(val) {
  const t = max.value > min.value ? (val - min.value) / (max.value - min.value) : 0
  return colourInterpolator.value(Math.max(0, Math.min(1, t)))
}

function getFillColor(grid) {
  const arr = grid.climate[selectedVariable.value] || []
  if (!arr.length) return '#ffffff'
  const value = Array.isArray(arr[0])
    ? arr.map(m => m[currentTimeIndex.value] || 0).reduce((a, b) => a + b, 0) / arr.length
    : (arr[currentTimeIndex.value] || 0)
  return getColorForValue(value)
}

function getMarkerIcon(item) {
  const color = colorMapping.value[item.facilityTypeName] || 'black'
  return L.divIcon({
    className: 'custom-marker-icon',
    html: `<div style="
      background-color:${color};
      width:30px;height:30px;
      border-radius:50%;
      border:2px solid white;
    "></div>`,
    iconSize: [30, 30],
    iconAnchor: [15, 15],
  })
}

function onInfrastructureClick(item) {
  dataStore.selectedAsset = item
  dataStore.fragilityView = 'sunburst'
}

function updateLegend() {
  if (!showClimateGrid.value || !climateDataLoaded.value) return
  const legend = document.getElementById('legend')
  if (!legend) return

  legend.innerHTML = ''

  const title = document.createElement('div')
  title.className = 'legend-title'
  title.innerText = `${variableNameMapping.value[selectedVariable.value] ?? selectedVariable.value}`
  legend.appendChild(title)

  const scale = document.createElement('div')
  scale.className = 'legend-scale'
  legend.appendChild(scale)

  const labels = document.createElement('div')
  labels.className = 'legend-labels'
  legend.appendChild(labels)

  const steps = 5
  const minV = min.value
  const maxV = max.value
  const step = (maxV - minV) / steps

  for (let i = 0; i <= steps; i++) {
    const v = minV + step * i
    const box = document.createElement('div')
    box.style.backgroundColor = getColorForValue(v)
    scale.appendChild(box)
    if (i === 0 || i === steps) {
      const lbl = document.createElement('span')
      lbl.innerText = v.toFixed(2)
      labels.appendChild(lbl)
    }
  }
}

function recenterMap() {
  if (!mapObject.value || !gridData.value.length) return
  const b = gridData.value[0].bounds
  mapObject.value.fitBounds(
    [[b.min_lat, b.min_lon], [b.max_lat, b.max_lon]],
    { padding: [100, 100] }
  )
}

watch(
  () => climateDataLoaded.value,
  (loaded) => {
    if (!loaded || !mapObject.value || !gridData.value.length) return
    recenterMap()
    nextTick(updateLegend)
  },
  { immediate: true }
)

watch(
  () => selectedVariable.value,
  () => {
    if (!gridData.value.length) return
    nextTick(updateLegend)
  },
  { immediate: true }
)

watch(
  [() => currentTimeIndex.value, () => showClimateGrid.value],
  ([, show]) => {
    if (show) nextTick(updateLegend)
  }
)

</script>

<style scoped>
.custom-marker-icon {
  display: flex;
  align-items: center;
  justify-content: center;
}

.clicked-marker-icon {
  display: flex;
  align-items: center;
  justify-content: center;
}
</style>