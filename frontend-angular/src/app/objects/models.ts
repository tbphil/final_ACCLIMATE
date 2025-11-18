import Graphic from '@arcgis/core/Graphic';
import { HazardOption } from '../interfaces/analysis';

// Enums
export enum ScenarioEnum {
  rcp85 = 'rcp85',
  rcp45 = 'rcp45',
}

export enum AggregationEnum {
  mean = 'mean',
  median = 'median',
  max = 'max',
  min = 'min',
  percentile = 'percentile',
}

export enum SectorEnum {
  energy_grid = 'Energy Grid',
  agriculture = 'Agriculture',
}

export enum HazardEnum {
  heat_stress = 'Heat Stress',
  drought = 'Drought',
}

// Request Models
export class DataRequest {
  hazard!: HazardEnum;
  scenario!: ScenarioEnum;
  domain!: string;
  lat!: number;
  lon!: number;
  num_cells!: number;
  min_lat!: number;
  max_lat!: number;
  min_lon!: number;
  max_lon!: number;
  prior_years!: number;
  future_years!: number;
  climate_model!: string;
  aggregate_over_member_id!: boolean;
  aggregation_method!: AggregationEnum;
  aggregation_q!: number;
  sector!: SectorEnum;
}
// hazard, scenario, bbox, histyears, projyears, aggclimmod, aggmethod

// Infrastructure Models
export class InfrastructureRequest {
  sector!: string;
  min_lat!: number;
  max_lat!: number;
  min_lon!: number;
  max_lon!: number; // why not grid bounds since it's same info?
  hazard!: string;
}

// config fromAttributes = True

export class InfrastructureBase {
  id!: string;
  sector!: string;
  name!: string;
  facilityTypeName!: string;
  county!: string;
  state!: string;
  latitude!: number;
  longitude!: number;
  source_sheet!: string;
  source_workbook!: string;
}

export class EnergyGrid {
  sector!: string; // Literal["Energy Grid"] = "Energy Grid"
  balanchingauthority!: string;
  eia_plant_id!: string;
  lines!: number;
  min_voltage!: number;
  max_voltage!: number;
}

// infrastructureunion = annotated union[energygrid]...

// Climate-Data Models
export class GridBounds {
  min_lat!: number;
  min_lon!: number;
  max_lat!: number;
  max_lon!: number;
}

export class ClimateVariables {
  // this is the same as member series except the id
  // modelConfig = ConfigDict
  tas!: Array<number>;
  hurs!: Array<number>;
}

export class AnalysisResult {
  compose_metric!: Array<number>;
  dates!: Array<string>;
  trend_line!: Array<number>;
  slope!: number;
  intercept!: number;
  histogram_counts!: Array<number>;
  histogram_bins!: Array<number>;
  mean_value!: number;
  median_value!: number;
  std_dev!: number;
}

export class ClimateAnalysis {
  analysis_results!: Map<string, Array<AnalysisResult>>;
}

export class GridData {
  grid_index!: number;
  bounds!: GridBounds;
  climate!: ClimateVariables;
}

export class MemberSeries {
  // modelConfig = ConfigDict
  member_id!: string;
  tas!: Array<number>;
  hurs!: Array<number>;
}

export class AOIDemographics {
  years!: Array<number>;
  population!: Array<number>;
  households!: Array<number>;
  median_hhi!: Array<number>;
  per_capita_income!: Array<number>;
}

export class ClimateData {
  variables: Array<string> = [];
  variable_long_names: Array<string> = [];
  times: Array<string> = [];
  bounding_box: GridBounds = new GridBounds();
  climate_analysis: ClimateAnalysis = new ClimateAnalysis();
  data: Array<GridData> = [];
  members: Array<MemberSeries> = [];
  aoi_demographics: AOIDemographics = new AOIDemographics();
}

// Fragility and HBOM Models
export class FragilityDetails {
  fragility_model!: string;
  fragility_params!: Map<string, number>;
  // modelConfig = ConfigDict
}

export class HBOMComponent {
  uuid!: string;
  label!: string;
  component_type!: string;
  hazards!: Map<string, FragilityDetails>;
  // subcomponents!: Array<HBOMComponent>; // in quotes in python?
  pof!: number;
  replacement_cost!: number;
  expected_annual_loss!: number;

  // config?
}

export class HBOMDefinition {
  sector!: string;
  components!: Array<HBOMComponent>;
}

// Cost Data
export class CostCategory {
  replacement = 'replacement';
  repair = 'repair';
  o_and_m = 'o&m';
  downtime = 'downtime';
}

export class CostSelector {
  field!: string; // literal maxVoltage, minVoltage, lines, capacityMva
  min_value!: number;
  max_value!: number;
}

export class CostItem {
  uuid!: string;
  component_type!: string;
  cost_category!: CostCategory;
  base_year!: number;
  capex_usd!: number;
  repair_ssd!: number;
  downtime_usd_per_hr!: number;
  opex_usd_per_year!: number;
  selector!: CostSelector;
  scaling_formula!: Map<string, number>;
  region!: string;
  source!: string;
  // updatedAt datetime
}

// Infrastructure-Level Risk Summary
export class InfrastructureRiskSummary {
  sector!: string;
  hazard!: string;
  total_expected_annual_loss!: number;
  components_total_count!: number;
  components_at_risk_count!: number;
  percent_at_risk!: number;
}

export class CensusRequest {
  // bbox!: GridBounds;
  years!: Array<number>;
  project!: boolean;
  // method!:
  window!: number;
  // fill!:
}

export class BackgroundTasks {}

export class FacilityMapItem {
  graphic!: Graphic;
  facilityData!: any;
}

export class InfrastructureResult {
  bounding_box!: GridBounds;
  count!: number;
  infrastructure!: any[];
  metadata!: InfrastructureMetaData;
  source!: string;
}

export class InfrastructureMetaData {
  component_types!: Map<string, number>;
  states!: string[];
}
