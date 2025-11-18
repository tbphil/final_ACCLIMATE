import { Injectable, signal, WritableSignal } from '@angular/core';
import {
  Analysis,
  Asset,
  AssetAnalysis,
  ComponentType,
  Infrastructure,
} from '../interfaces/analysis';

import {
  ClimateData,
  DataRequest,
  GridBounds,
  GridData,
  HBOMComponent,
  HBOMDefinition,
  InfrastructureResult,
} from '../objects/models';
import { DataService } from './data.service';
import { interpolateViridis } from 'd3-scale-chromatic';

@Injectable({
  providedIn: 'root',
})
export class AnalysisService {
  private _analysis: WritableSignal<Analysis> = signal({});

  get analysisInfo() {
    return this._analysis.asReadonly();
  }

  constructor(private dataService: DataService) {}

  /**
   * Updates the analysis information with the provided partial analysis data.
   * This method merges the existing analysis data with the new data, ensuring
   * that any properties in the new data overwrite the corresponding properties
   * in the existing data.
   *
   * @param updatedAnalysis - The partial analysis data to be merged with the existing analysis data.
   */
  updateAnalysis(updatedAnalysis: Partial<Analysis>) {
    this._analysis.update((currentAnalysis) => ({ ...currentAnalysis, ...updatedAnalysis }));
  }

  setClimateData(data: ClimateData) {
    const assetAnalysis: AssetAnalysis = {
      demographics: {
        years: data?.aoi_demographics?.years,
        population: data?.aoi_demographics?.population || [],
        households: data?.aoi_demographics?.households || [],
        medianHHI: data?.aoi_demographics?.median_hhi || [],
        perCapitaIncome: data?.aoi_demographics?.per_capita_income || [],
      },
      trendAnalysisResults: data?.climate_analysis?.analysis_results || {},
      gridData: data.data || [],
      selectedGridIndex: this.findGridIndex(data.bounding_box, data.data),
      times: this.mapTimes(data.times),
      currentTimeIndex: 0,
      memberIds: Array.isArray(data.members)
        ? data.members.map((m) => m?.member_id).filter(Boolean)
        : null,
      variables: {
        variablesList: data.variables,
        variableNameMapping: this.mapVariables(data.variables, data.variable_long_names),
        selectedVariable: data.variables[0], // May need to add contingency to only set if selected variable not set
      },
      climateDataLoaded: true,
    };

    this.updateAnalysis({ assetAnalysis: assetAnalysis });
  }

  findGridIndex(boundingBox: GridBounds, gridData: GridData[]) {
    let lat = (boundingBox.min_lat + boundingBox.max_lat) / 2;
    let lon = (boundingBox.min_lon + boundingBox.max_lon) / 2;

    let foundIndex = -1;
    let minDistance = Infinity;

    gridData.forEach((grid, idx) => {
      const { min_lat, min_lon, max_lat, max_lon } = grid.bounds;
      const centerLat = (min_lat + max_lat) / 2;
      const centerLon = (min_lon + max_lon) / 2;
      const dist = Math.hypot(centerLat - lat, centerLon - lon);
      if (dist < minDistance) {
        minDistance = dist;
        foundIndex = idx;
      }
    });
    return foundIndex;
  }

  mapTimes(times: string[]) {
    if (times && times.length) {
      const iso = times.map((t) => new Date(t).toISOString().split('T')[0]);
      return iso;
    } else {
      return [];
    }
  }

  mapVariables(variables: string[], variableLongNames: string[]) {
    if (variables.length) {
      const mapping: any = {};
      variables.forEach((code, i) => {
        mapping[code] = variableLongNames[i] ?? code;
      });
      return mapping;
    } else {
      return [];
    }
  }

  setInfrastructureData(infraResult: InfrastructureResult) {
    const infraState: Infrastructure = {};
    infraState.count = infraResult.count;
    infraState.infrastructure = [];
    if (infraResult?.infrastructure?.length) {
      infraResult.infrastructure.forEach((infra) => {
        const asset: Asset = {
          uuid: infra.uuid,
          name: infra.name,
          type: infra.facilityTypeName,
        };
        infraState.infrastructure?.push(asset);
      });
    }
    infraState.metadata = {
      componentTypes: [],
      states: [],
    };
    Object.entries(infraResult.metadata.component_types).forEach(([key, value]) => {
      const type: ComponentType = {
        assetName: key,
        count: value,
      };

      infraState.metadata?.componentTypes.push(type);
    });
    infraState.source = infraResult.source;

    this.generateColorMapping(infraState);

    this.updateAnalysis({ infrastructure: infraState });

    this.queryHBOMDefs();
  }

  generateColorMapping(facilities: any) {
    if (!Array.isArray(facilities) || !facilities.length) {
      return;
    }
    const types = [...new Set(facilities.map((i) => i?.facilityTypeName || 'Unknown'))];
    const map: { [key: string]: string } = {};
    types.forEach((t: string, idx) => {
      const tNorm = types.length > 1 ? idx / (types.length - 1) : 0;
      map[t] = interpolateViridis(tNorm);
    });

    const colorMapping = map;
    const currentInfrastructure = this.analysisInfo().infrastructure; // You need to implement this method to get the current infrastructure object
    const updatedInfrastructure = {
      ...currentInfrastructure,
      colorMapping: colorMapping,
    };
    this.updateAnalysis({ infrastructure: updatedInfrastructure });
  }

  queryHBOMDefs() {
    const sector = 'Energy Grid';
    const hazard = this.analysisInfo().scenario?.hazard;
    if (sector && hazard) {
      this.dataService.loadHBOMWithFragility(sector, hazard).subscribe({
        next: (data: HBOMDefinition) => {
          // Process and use the timeseriesData as needed

          // Store complete tree
          const currentValue = this.analysisInfo().componentAnalysis;
          const updatedValue = {
            ...currentValue,
            hbomDefinitions: data,
          };
          this.updateAnalysis({ componentAnalysis: updatedValue });

          // Extract time series for charting (uuid -> var -> [pof_timeseries])
          const curves = this.extractFragilityTimeSeries(data, hazard);
          const currentValue2 = this.analysisInfo().componentAnalysis;
          const updatedValue2 = {
            ...currentValue2,
            fragilityCurves: curves,
          };
          this.updateAnalysis({ componentAnalysis: updatedValue2 });

          // Add hbom flag to each queried component type if they exist
          this.analysisInfo().infrastructure?.metadata?.componentTypes.forEach((type) => {
            const hbomNode =
              this.analysisInfo().componentAnalysis?.hbomDefinitions?.components?.find(
                (c: { label: any; canonical_component_type: any; aliases: string | any[] }) =>
                  c.label === type.assetName ||
                  c.canonical_component_type === type.assetName ||
                  c.aliases?.includes(type.assetName)
              );
            type.hbom = hbomNode ? true : false;
          });
        },
        error: (err) => {
          console.error('Error fetching hbom:', err);
        },
      });
    } else {
    }
  }

  // Helper: extract {uuid: {var: [series]}} from tree with embedded fc_values
  extractFragilityTimeSeries(hbom: HBOMDefinition, hazard: string) {
    const result: Record<string, any> = {};

    const traverse = (node: any) => {
      const hazData = node?.hazards?.[hazard];
      if (hazData?.fragility_curves) {
        result[node.uuid] = {};

        Object.entries(hazData.fragility_curves).forEach(([varName, gridDict]: any) => {
          // Take max across grids or just use grid 0
          const grid0 = gridDict['0'] || gridDict[0];
          if (grid0?.fc_values) {
            result[node.uuid][varName] = grid0.fc_values;
          }
        });
      }
      (node.subcomponents || []).forEach((child: HBOMComponent) => traverse(child));
    };

    (hbom.components || []).forEach((comp) => traverse(comp));
    return result;
  }
}
