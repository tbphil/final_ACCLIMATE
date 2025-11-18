import { Component, CUSTOM_ELEMENTS_SCHEMA, ElementRef, OnInit, ViewChild } from '@angular/core';
import Map from '@arcgis/core/Map';
import MapView from '@arcgis/core/views/MapView';
import { EsriMapService } from '../../services/esri-map.service';

@Component({
  selector: 'app-map',
  imports: [],
  templateUrl: './map.html',
  styleUrl: './map.scss',
  schemas: [CUSTOM_ELEMENTS_SCHEMA],
})
export class EsriMap implements OnInit {
  public mapObject!: Map;
  public mapView!: MapView;

  hoverLocation!: string;

  @ViewChild('mapViewNode', { static: true })
  private mapViewEl!: ElementRef;

  constructor(private esriMapService: EsriMapService) {}

  ngOnInit(): void {
    this.esriMapService.constructMap(this.mapObject, this.mapView, this.mapViewEl);
  }
}
