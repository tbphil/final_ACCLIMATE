import { HttpClient, HttpParams } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { share } from 'rxjs';
import {
  DataRequest,
  ClimateData,
  CensusRequest,
  HBOMDefinition,
  AOIDemographics,
  InfrastructureRequest,
  InfrastructureResult,
} from '../objects/models';

@Injectable({ providedIn: 'root' })
export class DataService {
  constructor(private http: HttpClient) {}

  // testHealth(test: TestModel) {
  //   console.log('/api/test/health');
  //   return this.http.post<any>('/api/test/health', test).pipe(share());
  // }

  // clearCache() { // post - not used in vue?
  //   return this.http.post("/api/cache/clear").pipe(share());
  // }

  getClimateModels(scenario: string, hazard: string) {
    // on aggregate toggle
    let params = new HttpParams();
    params = params.append('scenario', scenario);
    params = params.append('domain', 'NAM-22i');
    params = params.append('hazard', hazard);

    return this.http.get<Array<string>>('/api/climate-models/', { params: params }).pipe(share());
  }

  getClimate(dataRequest: DataRequest) {
    return this.http.post<ClimateData>('/api/get-climate/', dataRequest).pipe(share());
  }

  getCensusPopulation(req: CensusRequest) {
    return this.http.post<AOIDemographics>('/api/get-census-population/', req).pipe(share());
  }

  // getHbomFragility(sector: string, hazard: string) {
  //   return this.http
  //     .get<HBOMDefinition>('/api/hbom-fragility/' + sector + '/' + hazard)
  //     .pipe(share());
  // }

  getHbom(sector: string, hazard: string) {
    return this.http.get<HBOMDefinition>('/api/get-hbom/' + sector + '/' + hazard).pipe(share());
  }

  loadHBOMWithFragility(sector: string, hazard: string) {
    const encodedSector = encodeURIComponent(sector);
    const encodedHazard = encodeURIComponent(hazard);
    return this.http
      .get<HBOMDefinition>('/api/fragility/compute/' + encodedSector + '/' + encodedHazard)
      .pipe(share());
  }

  // prewarm(sector: string, hazard: string, backgroundTasks: BackgroundTasks) { // post
  //   return this.http.post("/api/prewarm/" + sector + "/" + hazard).pipe(share()); // returns status
  // } // not in vue

  // getInfrastructureRisk(sector: string, hazard: string) {
  //   return this.http.get('/api/infrastructure-risk/' + sector + '/' + hazard).pipe(share());
  // }

  getFragilityTimeseries(sector: string, hazard: string) {
    return this.http
      .get<Map<string, Map<string, Array<number>>>>(
        '/api/fragility-timeseries/' + sector + '/' + hazard
      )
      .pipe(share());
  }

  // getEconomicAnalysis(sector: string, hazard: string) {
  //   return this.http.get('/api/economic-analysis/' + sector + '/' + hazard).pipe(share());
  // }

  getEconomicTimeseries(sector: string, hazard: string) {
    return this.http.get('/api/economic-timeseries/' + sector + '/' + hazard).pipe(share());
  }

  // getHbomTree(sector: string, hazard: string) {
  //   return this.http.get('/api/hbom/tree/' + sector + '/' + hazard).pipe(share());
  // }

  // getHbom2(sector: string, hazard: string) {
  //   return this.http.get('/api/hbom/get-hbom/' + sector + '/' + hazard).pipe(share());
  // }

  previewHbom(file: any, sector: any, formData: any) {
    return this.http.post('/api/hbom/preview/', formData).pipe(share());
  }

  commitHbom(formData: any) {
    return this.http.post('/api/hbom/commit/', formData).pipe(share());
  }

  getInfrastructure(infraReq: InfrastructureRequest) {
    return this.http.post<InfrastructureResult>('/api/get-infrastructure/', infraReq).pipe(share());
  } // return json of dict[str, any] with bounding_box and infrastructure
}
