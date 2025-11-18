import { Component, inject } from '@angular/core';
import { KENDO_LAYOUT } from '@progress/kendo-angular-layout';
import { Stepper } from '../stepper/stepper';
import { EsriMap } from '../map/map';
import { AnalysisComponent } from '../analysis/analysis';
import { AnalysisService } from '../../services/analysis';

@Component({
  selector: 'app-home',
  imports: [KENDO_LAYOUT, Stepper, EsriMap, AnalysisComponent],
  templateUrl: './home.html',
  styleUrl: './home.scss',
})
export class Home {
  private analysisService = inject(AnalysisService);

  paneResized(event: string) {
    setTimeout(() => {
      // this.analysisService.updateStateData({ analysisPaneSize: event });
      this.analysisService.updateAnalysis({ analysisPaneSize: event });
    }, 2000);
  }
}
