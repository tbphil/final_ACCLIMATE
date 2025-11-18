import { computed, ElementRef, Injectable } from '@angular/core';
import Map from '@arcgis/core/Map';
import MapView from '@arcgis/core/views/MapView';
import Zoom from '@arcgis/core/widgets/Zoom';
import SketchViewModel from '@arcgis/core/widgets/Sketch/SketchViewModel';
import GraphicsLayer from '@arcgis/core/layers/GraphicsLayer';
import Sketch from '@arcgis/core/widgets/Sketch';
import SimpleMarkerSymbol from '@arcgis/core/symbols/SimpleMarkerSymbol';
import Color from '@arcgis/core/Color';
import SimpleLineSymbol from '@arcgis/core/symbols/SimpleLineSymbol';
import SimpleFillSymbol from '@arcgis/core/symbols/SimpleFillSymbol';
import Graphic from '@arcgis/core/Graphic';
import { FacilityMapItem, GridBounds } from '../objects/models';
import * as webMercatorUtils from '@arcgis/core/geometry/support/webMercatorUtils';
import { BehaviorSubject } from 'rxjs';
import Point from '@arcgis/core/geometry/Point';
import Extent from '@arcgis/core/geometry/Extent';
import Polygon from '@arcgis/core/geometry/Polygon';
import * as geometryEngine from '@arcgis/core/geometry/geometryEngine';
import { AnalysisService } from './analysis';
import { MapPopupTemplate } from '../objects/popup-template';
import {
  interpolateViridis,
  interpolateInferno,
  interpolateYlOrRd,
  interpolateRdBu,
} from 'd3-scale-chromatic';
import { ComponentAnalysis } from '../interfaces/analysis';
@Injectable({ providedIn: 'root' })
export class EsriMapService {
  location = [-95.249, 38.954];
  zoomLevel = 5;

  public map!: Map;
  public mapView!: MapView;
  public mapViewEl!: ElementRef;

  sketchViewModel!: SketchViewModel;
  sketchViewGraphicsLayer!: GraphicsLayer;

  private bbox: BehaviorSubject<GridBounds> = new BehaviorSubject<GridBounds>(new GridBounds());
  bbox$ = this.bbox.asObservable();

  infrastructureData: FacilityMapItem[] = [];
  facilityLayer!: GraphicsLayer;
  selectedFacility: FacilityMapItem | null = null;

  colorMapping = computed(() => this.analysisService.analysisInfo().infrastructure?.colorMapping);

  selectedHazard = '';

  constructor(private analysisService: AnalysisService) {}

  constructMap(map: Map, mapView: MapView, mapViewEl: ElementRef) {
    this.map = map;
    this.mapView = mapView;
    this.mapViewEl = mapViewEl;

    this.map = new Map({ basemap: 'hybrid' });

    this.mapView = new MapView({
      container: this.mapViewEl.nativeElement,
      center: this.location,
      zoom: this.zoomLevel,
      map: this.map,
    });

    const zoom = new Zoom({
      view: this.mapView,
    });
    this.mapView.ui.remove('zoom');
    this.mapView.popupEnabled = true;
    this.loadedMap();
  }

  loadedMap() {
    this.sketchViewGraphicsLayer = new GraphicsLayer();
    this.map.add(this.sketchViewGraphicsLayer);
    this.sketchViewModel = this.createLassoSketchTool();

    this.sketchViewModel.on('create', (evt) => {
      if (evt.state === 'complete') {
        this.setGraphic(evt);
      }
    });

    this.facilityLayer = new GraphicsLayer();
    this.map.add(this.facilityLayer);

    // Add click event listener to the map view
    this.mapView.on('click', (evt) => {
      this.handleFacilityClick(evt);
    });
  }

  handleFacilityClick(evt: any) {
    this.mapView.hitTest(evt).then((response) => {
      // only get the graphics returned from myLayer
      const graphicHits = response.results?.filter(
        (hitResult) =>
          hitResult.type === 'graphic' && hitResult.graphic.layer === this.facilityLayer
      );
      if (graphicHits?.length) {
        // Have to double check graphicHit type even though it should be a graphic to satisfy type checks
        if (graphicHits[0].type === 'graphic') {
          const selectedGraphic = graphicHits[0].graphic;
          const selectedFacility = this.infrastructureData.find(
            (facility) => facility.graphic === selectedGraphic
          );

          if (selectedFacility) {
            this.selectedFacility = selectedFacility;
            const currentValue = this.analysisService.analysisInfo().componentAnalysis;
            const updatedValue = {
              ...currentValue,
              selectedAsset: this.selectedFacility.facilityData,
            };
            this.analysisService.updateAnalysis({
              componentAnalysis: updatedValue,
            });
          }
        }
      }
    });
  }

  setGraphic(event: { graphic: Graphic }) {
    event.graphic.attributes.id = 'bboxid';
    this.sketchViewGraphicsLayer.add(event.graphic);
    if (event.graphic.geometry) {
      let geojson = webMercatorUtils.webMercatorToGeographic(event.graphic.geometry).toJSON();
      let currBBox: GridBounds = {
        min_lat: 90,
        min_lon: 180,
        max_lat: -90,
        max_lon: -180,
      };
      geojson['rings'][0].forEach((ring: number[]) => {
        if (ring[0] > currBBox.max_lon) {
          currBBox.max_lon = ring[0];
        }
        if (ring[0] < currBBox.min_lon) {
          currBBox.min_lon = ring[0];
        }
        if (ring[1] > currBBox.max_lat) {
          currBBox.max_lat = ring[1];
        }
        if (ring[1] < currBBox.min_lat) {
          currBBox.min_lat = ring[1];
        }
      });
      this.bbox.next(currBBox);
    } else {
      console.log('ERROR');
    }
    this.sketchViewModel.cancel();
  }

  updateBBox(bbox: GridBounds) {
    const newExtent = new Extent({
      xmin: bbox.min_lon,
      xmax: bbox.max_lon,
      ymin: bbox.min_lat,
      ymax: bbox.max_lat,
      spatialReference: { wkid: 4326 },
    });
    const newPolygon = Polygon.fromExtent(newExtent);
    const existingGraphic = this.sketchViewGraphicsLayer.graphics.find(
      (graphic) => graphic.attributes.id === 'bboxid'
    );
    if (existingGraphic) {
      existingGraphic.geometry = newPolygon;
      let bufferedGeometry = geometryEngine.geodesicBuffer(newPolygon, 10, 'miles');
      this.mapView.goTo(bufferedGeometry, { animate: true, duration: 4000 });
    } else {
      console.log('Graphic not found, adding new graphic');
      const fillSymbol = new SimpleFillSymbol({
        color: new Color('rgba(153, 255, 243, 0.2)'),
        style: 'solid',
        outline: {
          color: new Color('white'),
          width: 1,
        },
      });
      this.sketchViewGraphicsLayer.add(new Graphic({ geometry: newPolygon, symbol: fillSymbol }));
      let bufferedGeometry = geometryEngine.geodesicBuffer(newPolygon, 10, 'miles');
      this.mapView.goTo(bufferedGeometry, { animate: true, duration: 4000 });
    }
  }

  updateFillColor(data: Array<number>, time: number) {
    const max = Math.max(...data);
    const min = Math.min(...data);
    const val = data[time];
    const t = max > min ? (val - min) / (max - min) : 0;
    let color = interpolateViridis(t); // default
    if (this.selectedHazard === 'Heat Stress') {
      color = interpolateInferno(t); // inferno
    } else if (this.selectedHazard === 'Drought') {
      color = interpolateYlOrRd(t); // ylorrd
    } else if (this.selectedHazard === 'Extreme Cold') {
      color = interpolateRdBu(t); // rdbu
    }
    this.sketchViewGraphicsLayer.graphics.forEach((graphic) => {
      graphic.symbol = new SimpleFillSymbol({
        color: color,
        style: 'solid',
        outline: {
          color: new Color('white'),
          width: 1,
        },
      });
    });
  }

  createLassoSketchTool(): SketchViewModel {
    const sketch = new Sketch({
      layer: this.sketchViewGraphicsLayer,
      view: this.mapView,
      creationMode: 'single',
    });
    sketch.defaultUpdateOptions.enableRotation = false;
    sketch.defaultUpdateOptions.enableScaling = false;
    sketch.defaultUpdateOptions.toggleToolOnClick = false;
    sketch.defaultUpdateOptions.multipleSelectionEnabled = false;
    sketch.viewModel.pointSymbol = new SimpleMarkerSymbol({
      style: 'square',
      color: new Color('#8A2BE2'),
      size: 16,
      outline: {
        color: new Color([255, 255, 255]),
        width: 3,
      },
    });
    sketch.viewModel.polylineSymbol = new SimpleLineSymbol({
      color: new Color('#8A2BE2'),
      width: 4,
      style: 'dash',
    });
    sketch.viewModel.polygonSymbol = new SimpleFillSymbol({
      color: new Color('rgba(153, 255, 243, 0.2)'),
      style: 'solid',
      outline: {
        color: new Color('white'),
        width: 1,
      },
    });
    return sketch.viewModel;
  }

  drawRectangleButton(shape: any) {
    this.reset();
    this.sketchViewModel.create(shape);
  }

  reset() {
    if (this.sketchViewModel) {
      this.sketchViewModel.cancel();
    }
    if (this.sketchViewGraphicsLayer) {
      this.sketchViewGraphicsLayer.removeAll();
    }
    if (this.facilityLayer) {
      this.facilityLayer.removeAll();
    }
  }

  drawFacilities(facilities: any[], hazard: string) {
    this.selectedHazard = hazard;
    this.analysisService.generateColorMapping(facilities);
    let facilityMapItems: FacilityMapItem[] = [];
    let facilityGraphics: Graphic[] = [];
    facilities.forEach((facility) => {
      let facilityMapItem = this.createFacilityMapItem(facility);
      if (facilityMapItem) {
        facilityMapItems.push(facilityMapItem);
        facilityGraphics.push(facilityMapItem.graphic);
      }
    });
    this.facilityLayer.addMany(facilityGraphics);
    this.infrastructureData = facilityMapItems;
  }

  createFacilityMapItem(facility: any): FacilityMapItem | null {
    if (facility['latitude'] && facility['longitude']) {
      const facilityPoint = this.createFacilityPoint(facility['latitude'], facility['longitude']);
      const color = new Color(this.colorMapping()[facility.facilityTypeName]);
      let symbol = new SimpleMarkerSymbol({
        style: 'circle',
        color: color,
        size: 10,
        outline: {
          color: new Color([0, 0, 0]),
          width: 1,
        },
      });
      let graphic = new Graphic({
        geometry: facilityPoint,
        symbol: symbol,
        attributes: facility,
        popupTemplate: MapPopupTemplate.mainMapFacilityTemplate,
      });
      // the popuptemplate allows highlight facility - popup itself is not working
      let newFacGraphic = new FacilityMapItem();
      newFacGraphic.graphic = graphic;
      newFacGraphic.facilityData = facility;
      return newFacGraphic;
    }
    return null;
  }

  public createFacilityPoint(latitude: number, longitude: number) {
    let pointGeographic = {
      type: 'point',
      longitude: longitude,
      latitude: latitude,
      spatialReference: {
        wkid: 4326,
      },
    };
    return new Point(pointGeographic);
  }
}
