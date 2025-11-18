import { Injectable } from "@angular/core";
import { MatSnackBar } from "@angular/material/snack-bar";

@Injectable({
  providedIn: 'root'
})
export class SnackBarService {

  constructor(private _snackBar: MatSnackBar) {

  }

  showErrorMessage(message: string, label: string = '', duration: number = 10000) {
    this._snackBar.open(message, label, {
      panelClass: 'snackbar-error-message',
      duration: duration,
    });
  }

  showSuccessMessage(message: string, label: string = '', duration: number = 10000) {
    this._snackBar.open(message, label, {
      panelClass: 'snackbar-success-message',
      duration: duration,
    });
  }

  showWarningMessage(message: string, label: string = '', duration: number = 10000) {
    this._snackBar.open(message, label, {
      panelClass: 'snackbar-warning-message',
      duration: duration,
    });
  }
}
