import PopupTemplate from '@arcgis/core/PopupTemplate';

export class MapPopupTemplate {
  static readonly mainMapFacilityTemplate = new PopupTemplate({
    title: '{name}',
    content:
      '<b>Component Type:</b> {component_type}<br/>' +
      '<b>Owner:</b>  {owner}<br/>' +
      '<b>Country:</b>  {country}<br/>' +
      '<b>State:</b>  {state}<br/>' +
      '<b>County:</b>  {county}<br/>' +
      '<b>Postal Code:</b>  {zip}<br/>'
  });
}
