<template>
  <div class="p-3 border rounded mb-0">
    <form @submit.prevent="onSubmitForm">
      <!-- Saved Queries Section -->
      <div v-if="savedQueries.length > 0" class="mb-3">
        <label class="form-label fw-bold">Saved Queries</label>
        <div class="list-group">
          <div
            v-for="(query, idx) in savedQueries"
            :key="idx"
            class="list-group-item d-flex justify-content-between align-items-center"
          >
            <button
              type="button"
              class="btn btn-sm btn-link text-start flex-grow-1 p-0"
              @click="loadQuery(query)"
            >
              {{ query.name }}
            </button>
            <button
              type="button"
              class="btn btn-sm btn-outline-danger"
              @click="deleteQuery(idx)"
            >
              ×
            </button>
          </div>
        </div>
      </div>

      <!-- Save Current Query Button -->
      <div class="mb-3">
        <button
          type="button"
          class="btn btn-outline-success btn-sm w-100"
          @click="showSaveModal = true"
        >
          Save Current Query
        </button>
      </div>

      <!-- Save Modal -->
      <div v-if="showSaveModal" class="mb-3 p-3 border rounded bg-light">
        <label class="form-label">Query Name</label>
        <div class="input-group">
          <input
            v-model="queryName"
            type="text"
            class="form-control"
            placeholder="e.g., Idaho Heat Stress"
            @keyup.enter="saveQuery"
          />
          <button type="button" class="btn btn-success" @click="saveQuery">
            Save
          </button>
          <button
            type="button"
            class="btn btn-secondary"
            @click="showSaveModal = false"
          >
            Cancel
          </button>
        </div>
      </div>

      <hr v-if="savedQueries.length > 0 || showSaveModal" class="mb-3" />

      <!-- Area Selection Indicator (when bbox is drawn) -->
      <div v-if="drawnBbox" class="alert alert-success mb-2">
        <strong>Area selected on map</strong><br />
        <small>
          {{ drawnBbox.min_lat.toFixed(4) }},
          {{ drawnBbox.min_lon.toFixed(4) }} to
          {{ drawnBbox.max_lat.toFixed(4) }}, {{ drawnBbox.max_lon.toFixed(4) }}
        </small>
        <button
          type="button"
          class="btn btn-sm btn-outline-secondary ms-2"
          @click="clearSelection"
        >
          Clear
        </button>
      </div>

      <!-- Point Selection (lat/lon + num_cells) - only show if no bbox -->
      <div v-if="!drawnBbox">
        <!-- Latitude + Longitude (same row) -->
        <div class="row g-2 mb-2">
          <div class="col">
            <label for="lat" class="form-label">Latitude</label>
            <input
              id="lat"
              type="number"
              class="form-control"
              v-model.number="lat"
              step="any"
              required
            />
          </div>
          <div class="col">
            <label for="lon" class="form-label">Longitude</label>
            <input
              id="lon"
              type="number"
              class="form-control"
              v-model.number="lon"
              step="any"
              required
            />
          </div>
        </div>

        <!-- Grid Scope full width -->
        <div class="mb-2">
          <label for="num_cells" class="form-label">Grid Cell Buffer</label>
          <input
            id="num_cells"
            type="number"
            class="form-control"
            v-model.number="num_cells"
            min="0"
            max="10"
            required
          />
        </div>
      </div>

      <!-- Hazard + Scenario (same row) -->
      <div class="row g-2 mb-2">
        <div class="col">
          <label for="hazard" class="form-label">Hazard</label>
          <select id="hazard" class="form-select" v-model="Hazard" required>
            <option disabled value="">Select a hazard…</option>
            <option v-for="h in hazards" :key="h" :value="h">{{ h }}</option>
          </select>
        </div>
        <div class="col">
          <label for="scenario" class="form-label">Scenario</label>
          <select id="scenario" class="form-select" v-model="scenario" required>
            <option value="rcp85">RCP 8.5</option>
            <option value="rcp45">RCP 4.5</option>
          </select>
        </div>
      </div>

      <!-- Historical & Projected Years (same row) -->
      <div class="row g-2 mb-2">
        <div class="col">
          <label for="priorYears" class="form-label">Historical Years</label>
          <input
            id="priorYears"
            type="number"
            class="form-control"
            v-model.number="priorYears"
            min="0"
            placeholder="e.g. 5"
            required
          />
        </div>
        <div class="col">
          <label for="futureYears" class="form-label">Projected Years</label>
          <input
            id="futureYears"
            type="number"
            class="form-control"
            v-model.number="futureYears"
            min="0"
            placeholder="e.g. 3"
            required
          />
        </div>
      </div>

      <!-- "Aggregate Climate Models" checkbox -->
      <div class="form-check mb-2">
        <input
          id="aggregate"
          type="checkbox"
          class="form-check-input"
          v-model="aggregateOverMemberId"
          @change="onAggregateToggle"
        />
        <label for="aggregate" class="form-check-label">
          Aggregate Climate Models
        </label>
      </div>

      <!-- If aggregating: show "Aggregation Method" -->
      <div v-if="aggregateOverMemberId" class="mb-2">
        <label for="aggregationMethod" class="form-label"
          >Aggregation Method</label
        >
        <select
          id="aggregationMethod"
          class="form-select"
          v-model="aggregationMethod"
          @change="onCheckAggregationMethod"
          required
        >
          <option value="mean">Mean</option>
          <option value="max">Max</option>
          <option value="min">Min</option>
          <option value="percentile">Percentile</option>
        </select>
      </div>

      <!-- If percentile chosen: show "Quantile" input -->
      <div
        v-if="aggregateOverMemberId && aggregationMethod === 'percentile'"
        class="mb-2"
      >
        <label for="aggregationQ" class="form-label">Percentile (1–100)</label>
        <input
          id="aggregationQ"
          type="number"
          class="form-control"
          v-model.number="aggregationQ"
          min="1"
          max="100"
          required
        />
      </div>

      <!-- If NOT aggregating: show "Climate Model" dropdown -->
      <div v-if="!aggregateOverMemberId" class="mb-2">
        <label for="climateModel" class="form-label">Climate Model</label>
        <select
          id="climateModel"
          class="form-select"
          v-model="climateModel"
          :disabled="modelsLoading || aggregateOverMemberId"
          required
        >
          <option disabled value="">Select a model…</option>
          <option v-for="m in availableModels" :key="m" :value="m">
            {{ m }}
          </option>
        </select>
        <div v-if="modelsLoading" class="form-text">Loading models…</div>
        <div
          v-else-if="!modelsLoading && availableModels.length === 0"
          class="form-text text-danger"
        >
          No models found for this scenario/hazard.
        </div>
      </div>

      <!-- Submit Button -->
      <button
        type="submit"
        class="btn btn-primary w-100"
        :disabled="
          loading || (!aggregateOverMemberId && climateModel === 'all')
        "
      >
        <span
          v-if="loading"
          class="spinner-border spinner-border-sm"
          role="status"
        ></span>
        Load Extreme Weather Data
      </button>
    </form>
  </div>
</template>

<script setup>
import { ref, watch, onMounted, computed } from "vue";
import { storeToRefs } from "pinia";
import { useDataStore } from "@/stores/data";

const emit = defineEmits(["submit-climate"]);

const store = useDataStore();
const Hazard = computed({
  get: () => store.Hazard,
  set: (val) => store.setHazard(val),
});

const { clickedLat, clickedLon, drawnBbox } = storeToRefs(store);

const lat = ref(clickedLat.value);
const lon = ref(clickedLon.value);
const num_cells = ref(0);

const priorYears = ref(1);
const futureYears = ref(1);

const scenario = ref("rcp85");
const domain = ref("NAM-22i");

const availableModels = ref([]);
const climateModel = ref("all");
const modelsLoading = ref(false);

const aggregateOverMemberId = ref(true);
const aggregationMethod = ref("mean");
const aggregationQ = ref(50);

const loading = ref(false);
const hazards = ["Heat Stress", "Drought", "Wind"];

// Save/Load functionality
const savedQueries = ref([]);
const showSaveModal = ref(false);
const queryName = ref("");

watch([clickedLat, clickedLon], ([newLat, newLon]) => {
  if (newLat !== null && newLon !== null) {
    lat.value = newLat;
    lon.value = newLon;
    console.log("Pre-filled coordinates from map click:", newLat, newLon);
  }
});

function clearSelection() {
  store.clearCoordinateSelection();
}

function onCheckAggregationMethod() {
  if (aggregationMethod.value !== "percentile") {
    aggregationQ.value = null;
  }
}

function onAggregateToggle() {
  if (aggregateOverMemberId.value) {
    availableModels.value = [];
    climateModel.value = "all";
  } else {
    fetchModels();
  }
}

watch([scenario, domain, Hazard], async () => {
  if (!aggregateOverMemberId.value) {
    await fetchModels();
  }
});

async function fetchModels() {
  if (aggregateOverMemberId.value) return;

  modelsLoading.value = true;
  availableModels.value = [];
  climateModel.value = "all";

  try {
    const url =
      "http://127.0.0.1:8000/api/climate-models" +
      `?scenario=${encodeURIComponent(scenario.value)}` +
      `&domain=${encodeURIComponent(domain.value)}` +
      `&hazard=${encodeURIComponent(Hazard.value)}`;

    const res = await fetch(url);
    if (!res.ok) throw new Error("Network error");
    const models = await res.json();
    models.sort();
    availableModels.value = models;
  } catch (err) {
    console.error("Failed to load models:", err);
    availableModels.value = [];
  } finally {
    modelsLoading.value = false;
  }
}

function saveQuery() {
  if (!queryName.value.trim()) {
    alert("Please enter a name for this query");
    return;
  }

  const query = {
    name: queryName.value,
    hazard: Hazard.value,
    scenario: scenario.value,
    domain: domain.value,
    priorYears: priorYears.value,
    futureYears: futureYears.value,
    aggregateOverMemberId: aggregateOverMemberId.value,
    aggregationMethod: aggregationMethod.value,
    aggregationQ: aggregationQ.value,
    climateModel: climateModel.value,
    timestamp: new Date().toISOString(),
  };

  if (drawnBbox.value) {
    query.bbox = drawnBbox.value;
  } else {
    query.lat = lat.value;
    query.lon = lon.value;
    query.num_cells = num_cells.value;
  }

  savedQueries.value.push(query);
  localStorage.setItem(
    "savedClimateQueries",
    JSON.stringify(savedQueries.value)
  );

  queryName.value = "";
  showSaveModal.value = false;
}

function loadQuery(query) {
  Hazard.value = query.hazard;
  scenario.value = query.scenario;
  domain.value = query.domain;
  priorYears.value = query.priorYears;
  futureYears.value = query.futureYears;
  aggregateOverMemberId.value = query.aggregateOverMemberId;
  aggregationMethod.value = query.aggregationMethod;
  aggregationQ.value = query.aggregationQ;
  climateModel.value = query.climateModel;

  if (query.bbox) {
    store.setDrawnBbox(query.bbox);
  } else {
    lat.value = query.lat;
    lon.value = query.lon;
    num_cells.value = query.num_cells;
    store.clearCoordinateSelection();
  }
}

function deleteQuery(index) {
  if (confirm("Delete this saved query?")) {
    savedQueries.value.splice(index, 1);
    localStorage.setItem(
      "savedClimateQueries",
      JSON.stringify(savedQueries.value)
    );
  }
}

onMounted(() => {
  const saved = localStorage.getItem("savedClimateQueries");
  if (saved) {
    try {
      savedQueries.value = JSON.parse(saved);
    } catch (e) {
      console.error("Failed to load saved queries:", e);
    }
  }

  if (!aggregateOverMemberId.value) {
    fetchModels();
  }
});

function onSubmitForm() {
  loading.value = true;

  const payload = {
    hazard: Hazard.value,
    scenario: scenario.value,
    domain: domain.value,
    prior_years: priorYears.value,
    future_years: futureYears.value,
    climate_model: climateModel.value,
    aggregate_over_member_id: aggregateOverMemberId.value,
    aggregation_method: aggregationMethod.value,
    aggregation_q: aggregationQ.value,
  };

  if (drawnBbox.value) {
    payload.min_lat = drawnBbox.value.min_lat;
    payload.max_lat = drawnBbox.value.max_lat;
    payload.min_lon = drawnBbox.value.min_lon;
    payload.max_lon = drawnBbox.value.max_lon;
  } else {
    payload.lat = lat.value;
    payload.lon = lon.value;
    payload.num_cells = num_cells.value;
  }

  console.log("[get_climate] payload:", payload);

  emit("submit-climate", payload);
  loading.value = false;
}
</script>
