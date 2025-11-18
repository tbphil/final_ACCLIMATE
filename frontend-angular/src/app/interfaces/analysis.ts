import {
  AggregationEnum,
  AnalysisResult,
  GridData,
  HazardEnum,
  ScenarioEnum,
  SectorEnum,
} from '../objects/models';

export interface Analysis {
  scenario?: Scenario;
  infrastructure?: Infrastructure;
  assetAnalysis?: AssetAnalysis;
  componentAnalysis?: ComponentAnalysis;
  analysisPaneSize?: string;
}

export interface Scenario {
  hazard: HazardEnum;
  scenario: ScenarioEnum;
  historicalYears?: number;
  projectedYears?: number;
  aggregate: boolean;
  aggregationMethod: AggregationEnum;
  sector?: SectorEnum;
}

export interface Infrastructure {
  count?: number;
  infrastructure?: Asset[];
  metadata?: Metadata;
  source?: string | null;
  colorMapping?: any;
}

export interface Asset {
  uuid: string;
  name?: string;
  type?: string;
}

export interface Metadata {
  componentTypes: ComponentType[];
  states: StateType[];
}

export interface ComponentType {
  assetName: string;
  count: number;
  hbom?: boolean;
  color?: string;
}

export interface StateType {
  stateName: string;
  count: number;
}

export interface AssetAnalysis {
  demographics?: Demographics;
  variables?: Variable;
  economicMetrics?: unknown;
  trendAnalysisResults?: Map<string, Array<AnalysisResult>>;
  gridData?: GridData[];
  selectedGridIndex?: any;
  times?: any;
  currentTimeIndex?: number;
  memberIds?: any;
  climateDataLoaded?: boolean;
}

export interface Demographics {
  years?: Array<number>;
  population?: Array<number>;
  households?: Array<number>;
  medianHHI?: Array<number>;
  perCapitaIncome?: Array<number>;
}

export interface Variable {
  selectedVariable?: any;
  variablesList?: Array<any>;
  variableNameMapping?: any;
}

export interface ComponentAnalysis {
  selectedAsset?: any;
  components?: unknown;
  hbomDefinitions?: any;
  hbomAssetType?: any;
  fragilityCurves?: any;
  props?: any;
}

export interface HazardOption {
  value: HazardEnum;
  viewValue: string;
}

export interface ScenarioOption {
  value: ScenarioEnum;
  viewValue: string;
}

export interface AggregationOption {
  value: AggregationEnum;
  viewValue: string;
}

// Create a mapping for the enum values to view values
export const HAZARD_OPTIONS: HazardOption[] = [
  { value: HazardEnum.heat_stress, viewValue: 'Heat Stress' },
  { value: HazardEnum.drought, viewValue: 'Drought' },
  // Add other hazard types here
];

// export const SCENARIO_OPTIONS: ScenarioOption[] [
//   { value: ScenarioEnum.rcp85, viewValue: 'RCP 8.5' },
//   { value: ScenarioEnum.rcp45, viewValue: 'RCP 4.5' },
// ];

// Create a mapping for the enum values to view values
export const SCENARIO_OPTIONS: ScenarioOption[] = [
  { value: ScenarioEnum.rcp85, viewValue: 'RCP 8.5' },
  { value: ScenarioEnum.rcp45, viewValue: 'RCP 4.5' },
  // Add other scenario types here
];

export const AGGREGATION_METHOD_OPTIONS: AggregationOption[] = [
  { value: AggregationEnum.mean, viewValue: 'Mean' },
  // { value: AggregationEnum.median, viewValue: 'Median' },
  { value: AggregationEnum.max, viewValue: 'Max' },
  { value: AggregationEnum.min, viewValue: 'Min' },
  { value: AggregationEnum.percentile, viewValue: 'Percentile' },
];
