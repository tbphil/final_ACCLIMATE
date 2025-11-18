// src/stores/data.js
import { defineStore } from 'pinia'
import {
  interpolateViridis,
  interpolateInferno,
  interpolateYlOrRd,
  interpolateRdBu,
} from 'd3-scale-chromatic'

export const useDataStore = defineStore('data', {
  // ---------------- state ----------------
  state: () => ({
    /* climate data */
    gridData: [],
    variablesList: [],
    variableNameMapping: {},
    times: [],
    memberIds: null,
    selectedVariable: null,
    currentTimeIndex: 0,
    climateDataLoaded: false,
    Hazard: null,
    rangesByVar: {},
    paletteByHazard: {
      'Heat Stress': 'inferno',
      Drought: 'ylOrRd',
      'Extreme Cold': 'rdBu',
      Default: 'viridis',
    },

    /* infrastructure */
    sector: null,
    infrastructureData: [],
    infrastructureDataLoaded: false,
    fragilityCurves: {},
    hbomDefinitions: { components: [] },
    preparedData: null,

    /* Map */
    selectedGridIndex: null,
    zoom: 5,
    center: [39.8283, -98.5795],
    
    // Coordinate selection (for climate modal pre-fill)
    clickedLat: null,
    clickedLon: null,
    drawnBbox: null,  // { min_lat, max_lat, min_lon, max_lon }

    url: 'https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png',
    attribution:
      'Map data © <a href="https://openstreetmap.org">OpenStreetMap</a> contributors',
    colorMapping: {},

    /* Analysis */
    trendAnalysisResults: {},
    aoiDemographics: null,
    populationTimeseries: [],
    householdsTimeseries: [],
    medianHHITimeseries: [],
    pciTimeseries: [],

    /* UI */
    selectedAsset: null,

    /* Charts / views */
    fragilityView: 'sunburst', // 'sunburst' | 'timeseries'

    /* flags */
    showInfoBar: false,
    showClimateGrid: true,
    showInfrastructureMarkers: true,
    showTrendAnalysis: false,
    economicLoading: false,
    lastRequest: null,
  }),

  // ---------------- getters ----------------
  getters: {
    // interpolator function for map colors
    colourInterpolator(state) {
      const key =
        state.paletteByHazard[state.Hazard] ?? state.paletteByHazard.Default
      const table = {
        viridis: interpolateViridis,
        inferno: interpolateInferno,
        ylOrRd: interpolateYlOrRd,
        rdBu: (t) => interpolateRdBu(1 - t), // invert so red = hot
      }
      return table[key]
    },

    // {min,max} for the currently-selected variable (cached)
    variableRange(state) {
      const v = state.selectedVariable
      if (!v) return { min: 0, max: 1 }
      if (state.rangesByVar[v]) return state.rangesByVar[v]

      let min = Infinity
      let max = -Infinity
      state.gridData.forEach((cell) => {
        const series = cell?.climate?.[v] || []
        const values = Array.isArray(series[0]) ? series.flat() : series
        values.forEach((val) => {
          if (typeof val !== 'number') return
          if (val < min) min = val
          if (val > max) max = val
        })
      })
      if (!Number.isFinite(min) || !Number.isFinite(max)) {
        min = 0
        max = 1
      }
      state.rangesByVar[v] = { min, max }
      return state.rangesByVar[v]
    },

    // live economic values at current slider step
    ealNow(state) {
      const i = state.currentTimeIndex ?? 0
      return state.hbomDefinitions?.system_eal_timeseries?.[i] ?? null
    },
    percentAtRiskNow(state) {
      const i = state.currentTimeIndex ?? 0
      return state.hbomDefinitions?.percent_at_risk_timeseries?.[i] ?? null
    },
    topAssetsNow(state) {
      const i = state.currentTimeIndex ?? 0
      return state.hbomDefinitions?.top_assets_timeseries?.[i] ?? []
    },

    // AOI demographics "now"
    populationNow(state) {
      const i = state.currentTimeIndex ?? 0
      return state.populationTimeseries?.[i] ?? null
    },
    householdsNow(state) {
      const i = state.currentTimeIndex ?? 0
      return state.householdsTimeseries?.[i] ?? null
    },
    medianHHINow(state) {
      const i = state.currentTimeIndex ?? 0
      return state.medianHHITimeseries?.[i] ?? null
    },
    pciNow(state) {
      const i = state.currentTimeIndex ?? 0
      return state.pciTimeseries?.[i] ?? null
    },

    isPreparedLoaded: (state) => !!state.preparedData,
  },

  // ---------------- actions ----------------
  actions: {
    // cache the blob returned by the backend loader
    setPreparedData(obj) {
      // existing climate/infrastructure payloads
      if (obj) {
        // keep storing whatever you already store, then also handle AOI
        this.preparedData = obj
      }

      // New: nested AOI block (preferred)
      const aoi = obj?.aoi_demographics || obj?.aoiDemographics
      if (aoi) {
        this.aoiDemographics      = aoi
        this.populationTimeseries = Array.isArray(aoi.population) ? aoi.population : []
        this.householdsTimeseries = Array.isArray(aoi.households) ? aoi.households : []
        this.medianHHITimeseries  = Array.isArray(aoi.median_hhi) ? aoi.median_hhi : []
        this.pciTimeseries        = Array.isArray(aoi.per_capita_income) ? aoi.per_capita_income : []
        return
      }

      // Legacy/fallback: flat keys (kept for safety)
      if ('population_timeseries' in (obj || {}) ||
          'households_timeseries' in (obj || {}) ||
          'median_hhi_timeseries' in (obj || {}) ||
          'pci_timeseries' in (obj || {})) {
        this.populationTimeseries = obj.population_timeseries ?? obj.populationTimeseries ?? []
        this.householdsTimeseries = obj.households_timeseries ?? obj.householdsTimeseries ?? []
        this.medianHHITimeseries  = obj.median_hhi_timeseries ?? obj.medianHHITimeseries ?? []
        this.pciTimeseries        = obj.pci_timeseries ?? obj.pciTimeseries ?? []
      }
    },

    // Map / UI flags
    setCenter(c) {
      this.center = c
    },
    toggleClimateGrid() {
      this.showClimateGrid = !this.showClimateGrid
    },
    toggleInfrastructureMarkers() {
      this.showInfrastructureMarkers = !this.showInfrastructureMarkers
    },
    setShowInfoBar(flag) {
      this.showInfoBar = flag
    },
    setEconomicLoading(flag) {
      this.economicLoading = flag
    },
    toggleTrendAnalysis() {
      this.showTrendAnalysis = !this.showTrendAnalysis
    },
    toggleFragilityView() {
      this.fragilityView =
        this.fragilityView === 'sunburst' ? 'timeseries' : 'sunburst'
    },
    setUrl(u) {
      this.url = u
    },
    setAttribution(a) {
      this.attribution = a
    },

    // Coordinate selection
    setClickedCoordinates(lat, lon) {
      this.clickedLat = lat
      this.clickedLon = lon
      this.drawnBbox = null  // Clear bbox when clicking point
    },
    
    setDrawnBbox(bbox) {
      this.drawnBbox = bbox  // { min_lat, max_lat, min_lon, max_lon }
      this.clickedLat = null  // Clear point when drawing bbox
      this.clickedLon = null
    },
    
    clearCoordinateSelection() {
      this.clickedLat = null
      this.clickedLon = null
      this.drawnBbox = null
    },

    // Core Climate
    setHazard(h) {
      this.Hazard = h
    },
    setTimes(arr) {
      this.times = arr
    },
    setGridData(v) {
      this.gridData = v
    },
    setVariables(list, mapping) {
      this.variablesList = list
      this.variableNameMapping = mapping || {}
    },
    setMemberIds(arr) {
      this.memberIds = arr
    },
    setSelectedVariable(v) {
      this.selectedVariable = v
    },
    setSelectedGridIndex(idx) {
      this.selectedGridIndex = idx
    },
    setCurrentTime(idx) {
      this.currentTimeIndex = idx
    },
    increaseTime() {
      if (this.currentTimeIndex < this.times.length - 1) {
        this.currentTimeIndex++
      }
    },
    decreaseTime() {
      if (this.currentTimeIndex > 0) {
        this.currentTimeIndex--
      }
    },
    markDataLoaded(kind) {
      // kind: 'climate' | 'infrastructure'
      this[`${kind}DataLoaded`] = true
    },
    setTrendAnalysisResults(obj) {
      this.trendAnalysisResults = obj
    },

    // Infrastructure
    setSector(s) {
      this.sector = s
    },
    setInfrastructure(arr) {
      this.infrastructureData = arr || []
    },
    markInfrastructureLoaded() {
      this.infrastructureDataLoaded = true
    },
    setSelectedAsset(asset) {
      this.selectedAsset = asset
    },

    // colour palette – SINGLE source of truth
    generateColorMapping() {
      if (!Array.isArray(this.infrastructureData) || !this.infrastructureData.length)
        return
      const types = [
        ...new Set(
          this.infrastructureData.map((i) => i?.facilityTypeName || 'Unknown')
        ),
      ]
      const map = {}
      types.forEach((t, idx) => {
        const tNorm = types.length > 1 ? idx / (types.length - 1) : 0
        map[t] = interpolateViridis(tNorm)
      })
      this.colorMapping = map
    },

    // HBOM / fragility / census
    setFragilityCurves(obj) {
      this.fragilityCurves = obj || {}
    },
    setHbomDefinitions(obj) {
      this.hbomDefinitions = obj || {}
    },
    setPopulationAtRisk(n) {
      this.populationAtRisk = n
    },
    setLastRequest(payload) {
      this.lastRequest = payload
    },
  },
})