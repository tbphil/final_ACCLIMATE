<template>
  <div class="card mb-3">
    <div class="card-header">
      <div class="row g-2">
        <!-- Component Label -->
        <div class="col-md-4">
          <label class="form-label">Component Label:</label>
          <input
            type="text"
            v-model="localNode.label"
            placeholder="Enter component label"
            class="form-control"
          />
        </div>
        <!-- Auto-generated Component Type (read-only) -->
        <div class="col-md-4">
          <label class="form-label">Component Type:</label>
          <div class="form-control-plaintext">{{ autoComponentType }}</div>
        </div>
        <!-- Hazard Selection -->
        <div class="col-md-4">
          <label class="form-label">Hazard:</label>
          <select v-model="localNode.hazard" class="form-select">
            <option value="">Select hazard</option>
            <option value="Heat Stress">Heat Stress</option>
            <option value="Ice Storm">Ice Storm</option>
          </select>
        </div>
        <!-- Fragility Model Selection -->
        <div class="col-md-4">
          <label class="form-label">Fragility Model:</label>
          <select v-model="localNode.fragility_model" class="form-select">
            <option value="">Select model</option>
            <option value="weibull">Weibull</option>
            <option value="lognormal">Lognormal</option>
            <option value="logistic">Logistic</option>
            <option value="inherit">Inherit</option>
          </select>
        </div>
      </div>
      <!-- Fragility Parameters -->
      <div class="row g-2 mt-2" v-if="fragilityParamKeys.length">
        <div
          class="col-md-3"
          v-for="key in fragilityParamKeys"
          :key="key"
        >
          <label class="form-label">
            {{ key.charAt(0).toUpperCase() + key.slice(1) }}:
          </label>
          <input type="number" v-model.number="localNode.fragility_params[key]" class="form-control" />
        </div>
      </div>
      <!-- Action Buttons -->
      <div class="d-flex gap-2 mt-3">
        <button class="btn btn-primary" @click="addChild">Add Child</button>
        <button v-if="allowRemove" class="btn btn-danger" @click="removeNode">Delete</button>
      </div>
    </div>
    <!-- Child Nodes -->
    <div class="card-body">
      <div v-if="localNode.subcomponents && localNode.subcomponents.length" class="ms-3">
        <HBOMNode
          v-for="(child, index) in localNode.subcomponents"
          :key="index"
          :node="child"
          :allowRemove="true"
          @remove="removeChild(index)"
          @update:node="updateChild(index, $event)"
        />
      </div>
    </div>
  </div>
</template>

<script>
export default {
  name: "HBOMNode",
  props: {
    node: {
      type: Object,
      required: true
    },
    allowRemove: {
      type: Boolean,
      default: false
    }
  },
  data() {
    return {
      updating: false,
      // Create a deep copy to work on locally
      localNode: JSON.parse(JSON.stringify(this.node))
    };
  },
  computed: {
    autoComponentType() {
      if (!this.localNode.label) return "";
      return this.localNode.label
        .toLowerCase()
        .trim()
        .replace(/\s+/g, "_")
        .replace(/[^a-z0-9_]/g, "");
    },
    fragilityParamKeys() {
      const model = this.localNode.fragility_model;
      if (!model || model === "inherit") return [];
      if (model === "weibull") return ["scale", "shape"];
      if (model === "lognormal") return ["dispersion", "median"];
      if (model === "logistic") return ["mid_point", "slope"];
      return [];
    }
  },
  watch: {
    "localNode.label": {
      handler() {
        if (this.updating) return;
        const newType = this.autoComponentType;
        if (this.localNode.component_type !== newType) {
          this.updating = true;
          this.localNode.component_type = newType;
          this.$emit("update:node", this.localNode);
          this.updating = false;
        }
      }
    },
    "localNode.fragility_model": {
      handler() {
        if (this.updating) return;
        this.updating = true;
        if (
          this.localNode.fragility_model &&
          this.localNode.fragility_model !== "inherit" &&
          (!this.localNode.fragility_params || Object.keys(this.localNode.fragility_params).length === 0)
        ) {
          this.localNode.fragility_params = {};
          this.fragilityParamKeys.forEach(key => {
            this.localNode.fragility_params[key] = 0;
          });
        }
        this.$emit("update:node", this.localNode);
        this.updating = false;
      }
    },
    node: {
      deep: true,
      handler(newVal) {
        this.localNode = JSON.parse(JSON.stringify(newVal));
      }
    }
  },
  methods: {
    addChild() {
      const newChild = {
        label: "",
        component_type: "",
        hazard: "",
        fragility_model: "",
        fragility_params: {},
        subcomponents: []
      };
      if (!this.localNode.subcomponents) {
        this.localNode.subcomponents = [];
      }
      this.localNode.subcomponents.push(newChild);
      this.$emit("update:node", this.localNode);
    },
    removeNode() {
      this.$emit("remove");
    },
    removeChild(index) {
      this.localNode.subcomponents.splice(index, 1);
      this.$emit("update:node", this.localNode);
    },
    updateChild(index, updatedChild) {
      this.localNode.subcomponents.splice(index, 1, updatedChild);
      this.$emit("update:node", this.localNode);
    }
  }
};
</script>

<style scoped>
.card {
  border: 1px solid #ccc;
  border-radius: 4px;
}
.card-header {
  background-color: #f9f9f9;
  padding: 12px;
}
.card-body {
  padding: 12px;
}
.field-group {
  margin-bottom: 8px;
}
</style>