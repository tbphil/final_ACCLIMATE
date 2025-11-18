<template>
  <div class="modal fade" tabindex="-1" aria-labelledby="fragModalLabel" >
    <div class="modal-dialog modal-lg">
      <div class="modal-content">
        <div class="modal-header">
          <h5 class="modal-title" id="fragModalLabel">{{ modalTitle }}</h5>
          <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
        </div>
        <div class="modal-body">
          <div id="frag-curve-plot" style="height: 400px;"></div>
          <div class="fragility-info">
            <p><strong>Component:</strong> {{ componentName }}</p>
            <p><strong>Hazard:</strong> {{ hazardName }}</p>
            <p><strong>Model:</strong> {{ model }}</p>
            <p><strong>Parameters:</strong> {{ modelParamsText }}</p>
            <p v-if="xRangeText"><strong>Data Range:</strong> {{ xRangeText }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script>
import { Modal } from 'bootstrap';
import Plotly from 'plotly.js-dist';

function erf(x) {
  const sign = (x >= 0) ? 1 : -1;
  x = Math.abs(x);
  const a1 =  0.254829592, a2 = -0.284496736, a3 =  1.421413741;
  const a4 = -1.453152027, a5 =  1.061405429, p  =  0.3275911;
  const t = 1.0 / (1.0 + p * x);
  const y = 1.0 - ((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * Math.exp(-x * x);
  return sign * y;
}

function lognormalCDF(x, median, dispersion) {
  if (x <= 0) return 0;
  const z = (Math.log(x) - Math.log(median)) / (dispersion * Math.sqrt(2));
  return 0.5 * (1 + erf(z));
}

function weibullCDF(x, shape, scale) {
  if (x <= 0) return 0;
  return 1 - Math.exp(-Math.pow(x / scale, shape));
}

function logisticCDF(x, midPoint, slope) {
  return 1 / (1 + Math.exp(-slope * (x - midPoint)));
}

export default {
  name: 'FragilityCurveModal',
  props: {
    modalTitle: { type: String, default: 'Fragility Curve' },
    componentName: { type: String, default: '' },
    fullVariableName: { type: String, default: ''},
    hazardName: { type: String, default: 'Heat Stress' },
    model: { type: String, default: 'weibull' },
    modelParams: { type: Object, default: () => ({ scale: 100, shape: 2.0 }) },
    // xValues from the flattened data (e.g., daily climate values) used to compute the range.
    xValues: { type: Array, default: () => [] },
    // curveData is not used for the parametric computation.
    curveData: { type: Array, default: () => [] },
    climateVariable: { type: String, default: 'Hazard Intensity'}
  },
  computed: {
    modelParamsText() {
      return Object.entries(this.modelParams)
        .map(([key, value]) => `${key}: ${value}`)
        .join(', ');
    },
    xRangeText() {
      if (this.xValues.length > 0) {
        const minVal = Math.min(...this.xValues).toFixed(1);
        const maxVal = Math.max(...this.xValues).toFixed(1);
        return `${minVal} to ${maxVal}`;
      }
      return '';
    }
  },
  data() {
    return { bsModal: null };
  },
  methods: {
    show() {
      // Ensure the modal element is attached to document.body.
      if (!document.body.contains(this.$el)) {
        document.body.appendChild(this.$el);
      }
      this.bsModal = new Modal(this.$el, {});
      this.bsModal.show();
    },
    hide() {
      if (this.bsModal) {
        this.bsModal.hide();
      }
    },
    plotFragilityCurve() {
      const fragDiv = this.$el.querySelector('#frag-curve-plot');
      if (!fragDiv) return;
      
      // Derive the x-axis domain from xValues, with some padding.
      let minX, maxX;
      if (this.xValues.length > 0) {
        minX = Math.min(...this.xValues);
        maxX = Math.max(...this.xValues);
        const padding = (maxX - minX) * 0.2;
        minX -= padding;
        maxX += padding;
      } else {
        minX = 0;
        maxX = 100;
      }
      
      // Generate 100 equally spaced points over the domain.
      const numPoints = 250;
      const step = (maxX - minX) / (numPoints - 1);
      const xAxis = [];
      for (let i = 0; i < numPoints; i++) {
        xAxis.push(minX + i * step);
      }
      
      let yAxis = [];
      const modelType = this.model.toLowerCase();
      if (modelType === 'lognormal') {
        const median = this.modelParams.median || 100;
        const dispersion = this.modelParams.dispersion || 0.3;
        yAxis = xAxis.map(x => lognormalCDF(x, median, dispersion));
      } else if (modelType === 'weibull') {
        const shape = this.modelParams.shape || 2.0;
        const scale = this.modelParams.scale || 100.0;
        yAxis = xAxis.map(x => weibullCDF(x, shape, scale));
      } else if (modelType === 'logistic') {
        const midPoint = this.modelParams.mid_point || 50;
        const slope = this.modelParams.slope || 0.5;
        yAxis = xAxis.map(x => logisticCDF(x, midPoint, slope));
      } else {
        yAxis = new Array(numPoints).fill(0);
      }
      
      const trace = {
        x: xAxis,
        y: yAxis,
        mode: 'lines+markers',
        name: 'Parametric Curve'
      };
      const layout = {
        //   title: this.modalTitle,fullVariableName
        xaxis: { title: `${this.fullVariableName} Intensity`  },
        yaxis: { title: 'Probability of Failure' }
      };
      Plotly.newPlot(fragDiv, [trace], layout);
    }
  }
};
</script>

<style scoped>
.fragility-info {
  margin-top: 1em;
}
.fragility-info p {
  margin: 0.2em 0;
}
</style>