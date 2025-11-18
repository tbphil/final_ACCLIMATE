import { Component, CUSTOM_ELEMENTS_SCHEMA, inject, ViewChild } from '@angular/core';
import { FormBuilder, Validators, FormsModule, ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatStepper, MatStepperModule } from '@angular/material/stepper';
import { MatButtonModule } from '@angular/material/button';
import { MatSelectModule } from '@angular/material/select';
import { MatIcon } from '@angular/material/icon';
import { AnalysisService } from '../../services/analysis';
import {
  AGGREGATION_METHOD_OPTIONS,
  Analysis,
  HAZARD_OPTIONS,
  Scenario,
  SCENARIO_OPTIONS,
} from '../../interfaces/analysis';
import { EsriMapService } from '../../services/esri-map.service';
import { CommonModule } from '@angular/common';
import { MatTooltipModule } from '@angular/material/tooltip';
import { DataService } from '../../services/data.service';
import {
  AggregationEnum,
  ClimateData,
  DataRequest,
  GridBounds,
  HazardEnum,
  InfrastructureRequest,
  InfrastructureResult,
  ScenarioEnum,
  SectorEnum,
} from '../../objects/models';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { Subscription, firstValueFrom } from 'rxjs';
import { SnackBarService } from '../../services/snack-bar.service';
import { SpinnerService } from '../../services/spinner.service';

@Component({
  selector: 'app-stepper',
  imports: [
    MatButtonModule,
    MatStepperModule,
    FormsModule,
    ReactiveFormsModule,
    MatFormFieldModule,
    MatInputModule,
    MatSelectModule,
    MatIcon,
    MatTooltipModule,
    CommonModule,
    MatCheckboxModule,
  ],
  templateUrl: './stepper.html',
  styleUrl: './stepper.scss',
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class Stepper {
  private _formBuilder = inject(FormBuilder).nonNullable; // prevents null but not undefined
  private analysisService = inject(AnalysisService);

  @ViewChild('stepper') stepper!: MatStepper;

  hazards = HAZARD_OPTIONS;
  scenarios = SCENARIO_OPTIONS;
  aggregationMethods = AGGREGATION_METHOD_OPTIONS;

  scenarioForm = this._formBuilder.group({
    hazard: [HazardEnum.heat_stress, Validators.required],
    scenario: [ScenarioEnum.rcp85, Validators.required],
    historicalYears: [1, Validators.required],
    projectedYears: [5, Validators.required],
    aggregate: [true, Validators.required],
    aggregationMethod: [AggregationEnum.mean, Validators.required],
    climateModel: [''],
  });

  squareEnabled = false;

  climateModels: Array<string> = []; // the climate models require scenario, domain, and hazard before they can be loaded

  showAggregation = true;
  dataLoaded = false;
  queryName = '';
  savedQueries: Array<any> = [];

  bbox!: GridBounds;

  constructor(
    private esriMapService: EsriMapService,
    private dataService: DataService,
    private snackBarService: SnackBarService,
    private spinnerService: SpinnerService
  ) {
    this.esriMapService.bbox$.subscribe((bbox) => {
      if (bbox && bbox.max_lat && bbox.max_lon && bbox.min_lat && bbox.min_lon) {
        this.getClimate(bbox);
      }
    });
    const storage = localStorage.getItem('savedClimateQueries') ?? '';
    if (storage && storage !== '') {
      this.savedQueries = JSON.parse(storage) ?? [];
    }
  }

  ngOnInit() {
    this.scenarioUpdated();
    this.scenarioForm.valueChanges.subscribe(() => {
      this.scenarioUpdated();
    });
  }

  getClimateModels() {
    let hazard = this.scenarioForm.get('hazard')?.value;
    let scenario = this.scenarioForm.get('scenario')?.value;
    let agg = this.scenarioForm.get('aggregate')?.value;
    if (agg === false && scenario && hazard) {
      this.showAggregation = false;
      this.spinnerService.showSpinner();
      this.dataService.getClimateModels(scenario, hazard).subscribe(
        (res) => {
          this.climateModels = res;
          this.spinnerService.hideSpinner();
        },
        () => {
          this.spinnerService.hideSpinner();
          this.snackBarService.showErrorMessage('Failed to load climate models');
        }
      );
    } else if (agg === false) {
      this.snackBarService.showErrorMessage(
        'Hazard and Scenario must be set to load climate models'
      );
      this.showAggregation = false;
    } else if (agg === true) {
      this.showAggregation = true;
    }
  }

  getClimate(bbox: GridBounds) {
    console.log('loading climate');
    const dataRequest = new DataRequest();
    dataRequest.hazard = this.scenarioForm.get('hazard')?.value ?? HazardEnum.heat_stress; // default to heat stress if undefined
    dataRequest.scenario = this.scenarioForm.get('scenario')?.value ?? ScenarioEnum.rcp85;
    dataRequest.domain = 'NAM-22i'; // hard coded for now but could change in the future
    dataRequest.min_lat = bbox.min_lat;
    dataRequest.min_lon = bbox.min_lon;
    dataRequest.max_lat = bbox.max_lat;
    dataRequest.max_lon = bbox.max_lon;

    dataRequest.prior_years = this.scenarioForm.get('historicalYears')?.value ?? 5;
    dataRequest.future_years = this.scenarioForm.get('projectedYears')?.value ?? 10;

    dataRequest.climate_model = this.scenarioForm.get('climateModel')?.value ?? '';
    dataRequest.aggregate_over_member_id = this.scenarioForm.get('aggregate')?.value ?? true;
    dataRequest.aggregation_method =
      this.scenarioForm.get('aggregationMethod')?.value ?? AggregationEnum.mean;
    dataRequest.aggregation_q = 50; // had 0, vue has 50
    // dataRequest.sector = SectorEnum.energy_grid;
    // dataRequest.climate_model = "all";

    this.spinnerService.showSpinner();

    this.dataService.getClimate(dataRequest).subscribe(
      (res: ClimateData) => {
        this.spinnerService.hideSpinner();
        this.esriMapService.updateBBox(res.bounding_box);
        this.bbox = res.bounding_box;
        this.getInfrastructure(res);

        this.analysisService.setClimateData(res);

        // ? loadHBOMWithFragility() ?
        // ? load census data ?
      },
      () => {
        this.dataLoaded = false;
        this.spinnerService.hideSpinner();
        this.snackBarService.showErrorMessage('Failed to load climate data');
      }
    );
  }

  getInfrastructure(climateData: ClimateData) {
    let infraReq: InfrastructureRequest = {
      sector: 'Energy Grid',
      min_lat: climateData.bounding_box.min_lat,
      max_lat: climateData.bounding_box.max_lat,
      min_lon: climateData.bounding_box.min_lon,
      max_lon: climateData.bounding_box.max_lon,
      hazard: this.scenarioForm.get('hazard')?.value ?? HazardEnum.heat_stress,
    };
    this.spinnerService.showSpinner();
    this.dataService.getInfrastructure(infraReq).subscribe(
      (res: InfrastructureResult) => {
        this.dataLoaded = true;
        this.spinnerService.hideSpinner();
        this.analysisService.setInfrastructureData(res);
        this.esriMapService.drawFacilities(res.infrastructure, infraReq.hazard);
        this.stepper.next();
      },
      () => {
        this.dataLoaded = false;
        this.spinnerService.hideSpinner();
        this.snackBarService.showErrorMessage('Failed to load infrastructure');
      }
    );
  }

  scenarioUpdated() {
    // Get the raw form values
    const updatedScenario = this.scenarioForm.value as Scenario;

    // Update the analysis with the transformed scenario
    this.analysisService.updateAnalysis({ scenario: updatedScenario });
  }

  squareMode() {
    if (!this.squareEnabled) {
      this.squareEnabled = true;
      this.esriMapService.drawRectangleButton('rectangle');
    } else {
      this.squareEnabled = false;
      this.esriMapService.reset();
    }
  }

  saveQuery() {
    if (!this.queryName || this.queryName.length < 1) {
      this.snackBarService.showErrorMessage('Please enter a query name');
      return;
    }

    const query = {
      name: this.queryName,
      hazard: this.scenarioForm.get('hazard')?.value ?? HazardEnum.heat_stress,
      scenario: this.scenarioForm.get('scenario')?.value ?? ScenarioEnum.rcp85,
      domain: 'NAM-22i',
      priorYears: this.scenarioForm.get('historicalYears')?.value ?? 5,
      futureYears: this.scenarioForm.get('projectedYears')?.value ?? 10,
      aggregateOverMemberId: this.scenarioForm.get('aggregate')?.value ?? '',
      aggregationMethod: this.scenarioForm.get('aggregationMethod')?.value ?? AggregationEnum.mean,
      aggregationQ: 50,
      climateModel: this.scenarioForm.get('climateModel')?.value ?? '',
      bbox: this.bbox,
    };

    if (this.savedQueries.find((x) => x.name === query.name)) {
      this.snackBarService.showErrorMessage(
        'Query name already in use, please select a different name.'
      );
      return;
    }

    this.savedQueries.push(query);
    localStorage.setItem('savedClimateQueries', JSON.stringify(this.savedQueries));

    if (this.savedQueries.find((x) => x.name === query.name)) {
      this.snackBarService.showSuccessMessage('Query saved');
    }
  }

  loadQuery(query: any) {
    const dataRequest = new DataRequest();
    if (query.hazard === 'Drought') {
      dataRequest.hazard = HazardEnum.drought;
    } else {
      dataRequest.hazard = HazardEnum.heat_stress;
    }
    if ((query.scenario = 'rcp45')) {
      dataRequest.scenario = ScenarioEnum.rcp45;
    } else {
      dataRequest.scenario = ScenarioEnum.rcp85;
    }
    dataRequest.domain = query.domain;
    dataRequest.min_lat = query.bbox.min_lat;
    dataRequest.min_lon = query.bbox.min_lon;
    dataRequest.max_lat = query.bbox.max_lat;
    dataRequest.max_lon = query.bbox.max_lon;
    dataRequest.prior_years = query.priorYears;
    dataRequest.future_years = query.futureYears;
    dataRequest.climate_model = query.climateModel;
    dataRequest.aggregate_over_member_id = query.aggregateOverMemberId;
    dataRequest.aggregation_method = query.aggregationMethod;
    dataRequest.aggregation_q = query.aggregationQ;

    this.spinnerService.showSpinner();
    this.dataService.getClimate(dataRequest).subscribe(
      (res: ClimateData) => {
        this.spinnerService.hideSpinner();
        this.esriMapService.updateBBox(res.bounding_box);
        this.bbox = res.bounding_box;
        this.getInfrastructure(res);
        this.analysisService.setClimateData(res);
      },
      () => {
        this.spinnerService.hideSpinner();
        this.snackBarService.showErrorMessage('Failed to load climate data');
      }
    );
  }

  deleteQuery(query: any) {
    const index = this.savedQueries.findIndex((x) => x.name === query.name);
    if (index < 0) {
      this.snackBarService.showErrorMessage('Failed to delete query');
      return;
    }
    this.savedQueries.splice(index, 1);
    localStorage.setItem('savedClimateQueries', JSON.stringify(this.savedQueries));
  }
}
