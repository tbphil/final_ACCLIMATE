import {
  Component,
  inject,
  effect,
  computed,
  untracked,
  Injector,
  afterRenderEffect,
  signal,
  WritableSignal,
} from '@angular/core';
import { AnalysisService } from '../../services/analysis';
import { AssetAnalysis, ComponentAnalysis, HAZARD_OPTIONS } from '../../interfaces/analysis';
import * as Plotly from 'plotly.js-dist-min';
import { DataService } from '../../services/data.service';
import { MatButtonToggleModule } from '@angular/material/button-toggle';
import { MatSliderModule } from '@angular/material/slider';
import { MatSortModule } from '@angular/material/sort';
import { MatIconModule } from '@angular/material/icon';
import { MatButtonModule } from '@angular/material/button';
import { KeyValuePipe } from '@angular/common';
import { EsriMapService } from '../../services/esri-map.service';
import { MatTooltip } from '@angular/material/tooltip';
import { AnalysisResult, ClimateVariables } from '../../objects/models';

@Component({
  selector: 'app-analysis',
  imports: [
    MatButtonToggleModule,
    MatSliderModule,
    MatSortModule,
    KeyValuePipe,
    MatIconModule,
    MatButtonModule,
    MatTooltip,
  ],
  templateUrl: './analysis.html',
  styleUrl: './analysis.scss',
})
export class AnalysisComponent {
  private analysisService = inject(AnalysisService);
  private esriMapService = inject(EsriMapService);
  dataService = inject(DataService);
  private injector = inject(Injector);

  hazards = HAZARD_OPTIONS; // Use the HAZARD_OPTIONS array

  // Use the computed() to update the values when they are updated
  analysis = computed(() => this.analysisService.analysisInfo());

  selectedVariable = computed(() => this.analysis().assetAnalysis?.variables?.selectedVariable);
  selectedAsset = computed(() => this.analysis().componentAnalysis?.selectedAsset);
  climateDataLoaded = computed(() => this.analysis().assetAnalysis?.climateDataLoaded);
  analysisPaneSize = computed(() => this.analysis().analysisPaneSize);

  currentTime = computed(() => {
    const times = this.analysis().assetAnalysis?.times;
    const index = this.analysis().assetAnalysis?.currentTimeIndex || 0;
    return times[index];
  });
  currentTimeIndex = computed(() => {
    const index = this.analysis().assetAnalysis?.currentTimeIndex;
    if (index) {
      return index;
    } else {
      return 0;
    }
  });

  fragilityItemClicked = signal(false);
  fragilityData: any = {};

  // --- Shared layout and utilities ---
  baseLayout: any = {
    margin: { l: 40, r: 20, t: 30, b: 40 },
    responsive: true,
    // add default font sizes, colors, etc. here
  };

  constructor() {
    // Tracks when the climate data loads and creates the line chart when it does
    afterRenderEffect(() => {
      if (this.climateDataLoaded() && this.selectedVariable()) {
        // Untracked runs the method below without creating a dependency for any signals in the method
        untracked(() => {
          this.createLineChart();

          if (this.analysis().assetAnalysis?.trendAnalysisResults) {
            this.createTrendChart();
          }
        });
      }
    });

    // Tracks when an asset has been selected and creates the HBOM/fragility curve graphs if applicable
    afterRenderEffect(() => {
      if (this.selectedAsset()) {
        // Untracked runs the method below without creating a dependency for any signals in the method
        untracked(() => {
          this.fragilityItemClicked.set(false);

          this.setSunburstChart();
        });
      }
    });

    // Tracks when the right panel is resized to change the width of the plotly charts
    afterRenderEffect(() => {
      if (this.analysisPaneSize()) {
        const width = this.analysisPaneSize() as string;
        const newWidth = Math.round(Number(width.replace('px', ''))) - 20;

        const chartDiv = document.getElementById('plotlyChart') as Plotly.PlotlyHTMLElement;
        const trendDiv = document.getElementById('analysis-plot') as Plotly.PlotlyHTMLElement;
        const histDiv = document.getElementById('histogram-plot') as Plotly.PlotlyHTMLElement;
        const div = document.getElementById('sunburst-plot') as Plotly.PlotlyHTMLElement;
        const fcEl = document.getElementById('fragility-static-curve') as Plotly.PlotlyHTMLElement;
        const tsEl = document.getElementById('fragility-inline-plot') as Plotly.PlotlyHTMLElement;

        const graphElements: Plotly.PlotlyHTMLElement[] = [
          chartDiv,
          trendDiv,
          histDiv,
          div,
          fcEl,
          tsEl,
        ];
        graphElements.forEach((element) => {
          if (element) {
            Plotly.relayout(element, {
              width: newWidth,
            });
          }
        });
      }
    });
    // Tracks when an HBOM component is clicked in the sunburst graph and it has a fragility curve to load fragility plots
    afterRenderEffect(() => {
      if (this.fragilityItemClicked()) {
        this.plotFragilityCurve();
      }
    });
  }

  updateTimeIndex(event: any) {
    let target = event.target as HTMLInputElement;
    const currentValue = this.analysis().assetAnalysis;
    const updatedValue = {
      ...currentValue,
      currentTimeIndex: Number(target.value),
    };
    this.analysisService.updateAnalysis({ assetAnalysis: updatedValue });
  }

  updateSelectedVariable(variable: string) {
    const currentValue = this.analysis().assetAnalysis;
    const updatedValue = {
      ...currentValue,
      variables: {
        ...currentValue?.variables,
        selectedVariable: variable,
      },
    };
    this.analysisService.updateAnalysis({ assetAnalysis: updatedValue });
  }

  createLineChart() {
    const gridData = computed(() => this.analysis().assetAnalysis?.gridData);
    const times = computed(() => this.analysis().assetAnalysis?.times);
    const memberIds = computed(() => this.analysis().assetAnalysis?.memberIds);
    const selectedGridIndex = computed(() => this.analysis().assetAnalysis?.selectedGridIndex);

    // 1) Build trace data
    const lineData: any = computed(() => {
      const v = this.selectedVariable() as string;
      const asset = gridData()![selectedGridIndex()];
      if (!asset) return null;
      const variableData = asset.climate ? asset.climate[v as keyof ClimateVariables] : null;
      if (!variableData?.length) return null;
      const traces = Array.isArray(variableData[0])
        ? memberIds().map((id: any, i: any) => ({
            x: times(),
            y: variableData[i],
            mode: 'lines',
            name: id,
          }))
        : [
            {
              x: times(),
              y: variableData,
              mode: 'lines',
              name: v,
            },
          ];
      return { v, traces };
    });

    // 2) Plot function
    const plotLine = () => {
      const time = this.currentTime();
      Plotly.react('plotlyChart', lineData().traces, {
        ...this.baseLayout,
        autosize: true,
        title: `${lineData().v} Over Time`,
        xaxis: { title: 'Time' },
        yaxis: { title: lineData().v },
        shapes: [this.verticalLineShape(time, '#8dc340')],
      });
    };

    // Replots line if line data is changed
    afterRenderEffect(
      () => {
        if (lineData()) {
          plotLine();
        }
      },
      { injector: this.injector }
    );
    // Changes the graph vertical line location if the time index is changed
    afterRenderEffect(
      () => {
        const time = this.currentTime();
        const lineShape: any = this.verticalLineShape(time, '#8dc340');
        if (this.currentTimeIndex())
          this.esriMapService.updateFillColor(lineData().traces[0].y, this.currentTimeIndex());
        Plotly.relayout('plotlyChart', { shapes: [lineShape] });
      },
      { injector: this.injector }
    );
  }

  verticalLineShape(pos: any, color?: any) {
    if (!color) {
      color = 'black';
    }
    return {
      type: 'line',
      x0: pos,
      x1: pos,
      yref: 'paper',
      y0: 0,
      y1: 1,
      line: { width: 2, dash: 'dash', color },
    };
  }

  createTrendChart() {
    const trendAnalysisResults = computed(
      () => this.analysis().assetAnalysis?.trendAnalysisResults
    );
    const selectedGridIndex = computed(() => this.analysis().assetAnalysis?.selectedGridIndex);

    // computed data object
    const trendData = computed(() => {
      const v = this.selectedVariable();
      let arr;
      if (trendAnalysisResults()) {
        arr = trendAnalysisResults()![v as keyof Map<string, AnalysisResult[]>];
      }
      if (!arr) return null;
      // if array per grid, pick index
      return Array.isArray(arr) ? arr[selectedGridIndex()] : arr;
    });

    // plotting function
    const plotTrend = (data: any) => {
      const v = this.selectedVariable();
      console.log(`createTrendChart: plotting variable ${v}`);
      const trendDiv = document.getElementById('analysis-plot');
      const histDiv = document.getElementById('histogram-plot');
      if (!trendDiv || !histDiv) return;

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
      } = data;
      // line traces
      const lineTrace = {
        x: dates,
        y: composite_metric,
        mode: 'lines',
        name: 'Composite',
        line: {},
      };
      const trendTrace = { x: dates, y: trend_line, mode: 'lines', name: 'Trend', line: {} };
      Plotly.react(trendDiv, [lineTrace, trendTrace], {
        ...this.baseLayout,
        title: `Trend Analysis (${v})`,
        xaxis: { title: 'Time' },
        yaxis: { title: 'Composite Metric' },
        annotations: [
          {
            x: dates[0],
            y: Math.max(...composite_metric),
            text: `Slope: ${slope.toFixed(4)}`,
            showarrow: false,
          },
        ],
      });

      // histogram
      const histX = [];
      const histY = [];
      const colors = [];
      const lower = mean_value - 2 * std_dev;
      const upper = mean_value + 2 * std_dev;
      for (let i = 0; i < histogram_counts.length; i++) {
        const center = (histogram_bins[i] + histogram_bins[i + 1]) / 2;
        histX.push(center);
        histY.push(histogram_counts[i]);
        colors.push(center < lower || center > upper ? 'red' : 'blue');
      }
      const histTrace: any = { x: histX, y: histY, type: 'bar', marker: { color: colors } };
      Plotly.react(histDiv, [histTrace], {
        ...this.baseLayout,
        title: `Frequency (${v})`,
        xaxis: { title: 'Composite Metric' },
        yaxis: { title: 'Count' },
        shapes: [
          {
            type: 'line',
            x0: mean_value,
            x1: mean_value,
            y0: 0,
            y1: 1,
            yref: 'paper',
            line: { dash: 'dash', color: 'black' },
          },
          {
            type: 'line',
            x0: median_value,
            x1: median_value,
            y0: 0,
            y1: 1,
            yref: 'paper',
            line: { dash: 'dash', color: 'gray' },
          },
        ],
        annotations: [
          { x: mean_value, text: `Mean`, showarrow: false },
          { x: median_value, text: `Median`, showarrow: false },
        ],
      });
    };

    // Replots line if line data is changed
    afterRenderEffect(
      () => {
        const data = trendData();
        if (data) {
          plotTrend(data);
        } else {
          // purge if toggled off or no data
          const tDiv = document.getElementById('analysis-plot');
          const hDiv = document.getElementById('histogram-plot');
          if (tDiv) Plotly.purge(tDiv);
          if (hDiv) Plotly.purge(hDiv);
        }
      },
      { injector: this.injector }
    );
  }

  showTypeHBOM(assetType: string) {
    this.fragilityItemClicked.set(false);
    const currentValue = this.analysis().componentAnalysis;
    const updatedValue = {
      ...currentValue,
      hbomAssetType: assetType,
    };
    this.analysisService.updateAnalysis({ componentAnalysis: updatedValue });
    this.setSunburstChart();
  }

  setSunburstChart() {
    // Sunburst Chart  -----------------------------
    const analysis = this.analysis();
    // accumulator-based traverse
    const traverse = (
      node: { label: any; component_type: any; uuid: any; subcomponents: any },
      parentId: null,
      facilityType: any,
      climateVar: string | number,
      out: { ids: any; labels: any; parents: any; custom: any }
    ) => {
      const label = node.label;
      const compType = node.component_type;
      const id = parentId ? `${parentId}/${label}` : label;

      // compute P(fail)
      let p_fail = 0;
      const uuid = node.uuid;
      const series: any = analysis.componentAnalysis?.fragilityCurves?.[uuid]?.[climateVar];

      if (Array.isArray(series) && series.length > 0) {
        const idx = Math.min(this.currentTimeIndex(), series.length - 1);
        p_fail = series[idx];
      }

      // accumulate data
      out.ids.push(id);
      out.labels.push(label);
      out.parents.push(parentId || '');
      out.custom.push({ uuid: node.uuid, compType, p_fail });

      //DEBUG: log the root/facility node values
      if (label === facilityType) {
        console.log('[Sunburst root]', {
          label,
          uuid,
          varKey: climateVar,
          ownSeriesLen: Array.isArray(
            analysis.componentAnalysis?.fragilityCurves[uuid]?.[climateVar]
          )
            ? analysis.componentAnalysis?.fragilityCurves[uuid][climateVar].length
            : 0,
          p_fail_own: p_fail,
        });
      }

      (node.subcomponents || []).forEach((child: any) =>
        traverse(child, id, facilityType, climateVar, out)
      );
    };

    // computed sunburst data
    const sunburstData = computed(() => {
      let facilityType = '';
      if (analysis.componentAnalysis?.selectedAsset) {
        facilityType = analysis.componentAnalysis?.selectedAsset.facilityTypeName;
      } else {
        facilityType = analysis.componentAnalysis?.hbomAssetType;
      }
      if (!facilityType) return null;
      const selectedVariable = analysis.assetAnalysis?.variables?.selectedVariable;

      const hbomNode = analysis.componentAnalysis?.hbomDefinitions.components?.find(
        (c: { label: any; canonical_component_type: any; aliases: string | any[] }) =>
          c.label === facilityType ||
          c.canonical_component_type === facilityType ||
          c.aliases?.includes(facilityType)
      );
      if (!hbomNode) return null;

      const out = { ids: [], labels: [], parents: [], custom: [] };
      traverse(hbomNode, null, facilityType, selectedVariable, out);
      return out;
    });

    // click handler for fragility modal
    const handleClick = (eventData: any) => {
      const pt = eventData.points?.[0];
      if (!pt) return;

      const { uuid } = pt.customdata;
      const varKey = analysis.assetAnalysis?.variables?.selectedVariable;

      console.log('Click:', pt.label, 'UUID:', uuid, 'Looking for var:', varKey);
      console.log(
        'Available curves for this UUID:',
        Object.keys(analysis.componentAnalysis?.fragilityCurves[uuid] || {})
      );

      const series = analysis.componentAnalysis?.fragilityCurves[uuid]?.[varKey];

      if (!Array.isArray(series) || series.length === 0) {
        console.warn('No fragility data for', pt.label, 'variable:', varKey);
        return;
      }

      const fragData = {
        itemClicked: true,
        componentName: pt.label,
        curveData: series,
        hazardName: analysis.scenario?.hazard,
      };

      this.fragilityData = fragData;
      this.fragilityItemClicked.set(true);
    };

    // plotting function
    const plot = (data: { custom: any[]; ids: any; labels: any; parents: any }) => {
      const div = document.getElementById('sunburst-plot') as Plotly.PlotlyHTMLElement;
      if (!div) return;
      const cfg = {
        low: 0.25,
        high: 0.51,
        colors: { low: 'green', medium: 'yellow', high: 'red' },
        ...(analysis.scenario?.hazard === 'Heat Stress' ? {} : {}), //placeholder for hazard specific configurations
      };
      const colours = data.custom.map((v: { p_fail: number }) => {
        if (v.p_fail === 0) return '#CCCCCC'; // BSU orange - no curve
        return v.p_fail < cfg.low
          ? cfg.colors.low
          : v.p_fail < cfg.high
          ? cfg.colors.medium
          : cfg.colors.high;
      });

      const trace: Plotly.Data = {
        type: 'sunburst',
        ids: data.ids,
        labels: data.labels,
        parents: data.parents,
        customdata: data.custom,
        marker: { colors: colours },
        hovertemplate: '<b>%{label}</b><br>P(fail): %{customdata.p_fail:.2%}<extra></extra>',
      };
      // avoid stacking handlers
      div.removeAllListeners?.('plotly_click');
      Plotly.react(div, [trace], this.baseLayout);
      div.on('plotly_click', handleClick);
    };

    if (sunburstData() != null) {
      plot(sunburstData() as { custom: any[]; ids: any; labels: any; parents: any });
    }

    // Replots data if changed
    afterRenderEffect(
      () => {
        this.currentTimeIndex();
        const data = sunburstData();
        const div = document.getElementById('sunburst-plot') as Plotly.PlotlyHTMLElement;
        if (!data) {
          if (div) {
            div.removeAllListeners?.('plotly_click');
            Plotly.purge(div);
          }
          return;
        }

        plot(data);
      },
      { injector: this.injector }
    );
  }

  plotFragilityCurve() {
    // Make sure static curve metadata is populated when we click a node
    this.hydrateStaticCurveIfMissing();

    const fullVariableName = computed(
      () =>
        this.analysis().assetAnalysis?.variables?.variableNameMapping[
          this.analysis().assetAnalysis?.variables?.selectedVariable
        ] || this.analysis().assetAnalysis?.variables?.selectedVariable
    );

    const times = computed(() => this.analysis().assetAnalysis?.times);
    const currentTimeIndex = computed(() => this.analysis().assetAnalysis?.currentTimeIndex);

    // (1) Inline time series (PoF over time)
    const tsEl = document.getElementById('fragility-inline-plot') as Plotly.PlotlyHTMLElement;

    if (tsEl) {
      const xT = times() || [];
      const yT = this.fragilityData.curveData || [];
      const currentTime = times()[this.currentTimeIndex()];

      Plotly.react(tsEl, [{ x: xT, y: yT, mode: 'lines', name: fullVariableName() }], {
        ...(this.baseLayout || {}),
        title: `${this.fragilityData.componentName} — Probability of Failure (Time)`,
        xaxis: { title: 'Time' },
        yaxis: { title: 'P(failure)', rangemode: 'tozero' },
        shapes: [
          {
            type: 'line',
            x0: currentTime,
            x1: currentTime,
            yref: 'paper',
            y0: 0,
            y1: 1,
            line: { width: 2, dash: 'dash', color: '#8dc340' },
          },
        ],
        height: 340,
      });
    }

    const hasStaticCurve = computed(
      () =>
        Array.isArray(this.fragilityData.xValues) &&
        Array.isArray(this.fragilityData.staticCurve) &&
        this.fragilityData.xValues.length === this.fragilityData.staticCurve.length &&
        this.fragilityData.xValues.length > 1
    );

    // (2) Static fragility curve (hazard intensity → PoF), only if data present
    const fcEl = document.getElementById('fragility-static-curve');
    if (fcEl && hasStaticCurve()) {
      Plotly.react(
        fcEl,
        [
          {
            x: this.fragilityData.xValues,
            y: this.fragilityData.staticCurve,
            mode: 'lines',
            name: this.fragilityData.model || 'fragility',
          },
        ],
        {
          ...(this.baseLayout || {}),
          title: `${this.fragilityData.componentName} — Fragility Curve`,
          xaxis: { title: `${fullVariableName} Intensity` },
          yaxis: { title: 'P(failure)', rangemode: 'tozero' },
          height: 340,
        }
      );
    }
  }

  /**
   * Try to enrich fragilityData with static-curve info from hbomDefinitions.
   * We look up the clicked component inside the selected facility tree and pull:
   *   hazards[Hazard].fragility_curves[variable]['0'] => { x_values, fc_values }
   *   plus fragility_model / fragility_params at that node.
   * This matches the backend shape produced in fragilityCurve.py. :contentReference[oaicite:3]{index=3}
   */
  hydrateStaticCurveIfMissing() {
    if (!this.analysis().componentAnalysis?.hbomDefinitions) return;
    if (!this.fragilityData.componentName) return;
    if (Array.isArray(this.fragilityData.xValues) && this.fragilityData.xValues.length > 1) return;

    const facilityRoot = (this.analysis().componentAnalysis?.hbomDefinitions.components || []).find(
      (c: any) =>
        c.label ===
        (this.analysis().componentAnalysis?.selectedAsset?.facilityTypeName ||
          this.analysis().componentAnalysis?.hbomAssetType ||
          this.analysis().componentAnalysis?.props.assetType)
    );
    if (!facilityRoot) return;

    const compName = this.fragilityData.componentName;
    let found = null;
    (function walk(node) {
      if (found) return;
      if (node.label === compName) {
        found = node;
        return;
      }
      (node.subcomponents || []).forEach(walk);
    })(facilityRoot);

    if (!found) return;

    let hazObj;
    if (this.analysis().scenario?.hazard) {
      const hazardKey = Object.keys(found.hazards || {}).find(
        (key) => key === this.analysis().scenario?.hazard
      )!;
      hazObj = found.hazards ? found.hazards[hazardKey] : {};
    }
    if (!hazObj) return;

    // prefer the selected variable, grid "0"
    const varKey = this.analysis().assetAnalysis?.variables?.selectedVariable;
    const grids = (hazObj.fragility_curves || {})[varKey];
    const g0 = grids ? grids['0'] : null;

    if (
      g0 &&
      Array.isArray(g0.x_values) &&
      Array.isArray(g0.fc_values) &&
      g0.x_values.length === g0.fc_values.length
    ) {
      this.fragilityData.xValues = g0.x_values;
      this.fragilityData.staticCurve = g0.fc_values;
    }
    if (hazObj.fragility_model) this.fragilityData.model = hazObj.fragility_model;
    if (hazObj.fragility_params) this.fragilityData.modelParams = hazObj.fragility_params;
  }

  formatParam(v: any) {
    if (v == null) return '—';
    if (typeof v === 'number') return Number.isFinite(v) ? v.toPrecision(6) : String(v);
    if (Array.isArray(v))
      return `[${v.map((n) => (Number.isFinite(n) ? n.toPrecision(4) : n)).join(', ')}]`;
    if (typeof v === 'object') return JSON.stringify(v);
    return String(v);
  }

  /* Clears plotly graphs and fragility data when a new asset is selected */
  resetComponentGraphs() {
    this.fragilityData = {};
    const fcEl = document.getElementById('fragility-static-curve') as Plotly.PlotlyHTMLElement;
    const tsEl = document.getElementById('fragility-inline-plot') as Plotly.PlotlyHTMLElement;

    const graphElements = [fcEl, tsEl];
    graphElements.forEach((element) => {
      if (element) {
        element.removeAllListeners?.('plotly_click');
        Plotly.purge(element);
      }
    });
  }
}
