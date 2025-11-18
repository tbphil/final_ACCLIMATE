// Copyright 2019, Battelle Energy Alliance, LLC    All Rights Reserved.
import { Injectable } from '@angular/core';
import { NgxSpinnerService } from 'ngx-spinner';

@Injectable({
  providedIn: 'root'
})
export class SpinnerService {
  constructor(private spinner: NgxSpinnerService) {}

  showSpinner() {
    this.spinner.show(undefined, {
      bdColor: 'rgba(255,255,255,0.7)',
      color: '#0091ff',
      type: 'ball-clip-rotate',
      size: 'medium',
      fullScreen: true
    });
  }

  hideSpinner() {
    this.spinner.hide();
  }
}
