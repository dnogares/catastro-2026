# Requirements Gathering: GIS Visor Improvements (UI/UX)

## Objective
Elevate the "Visor GIS" to a professional-grade, premium tool for cadastral analysis, ensuring intuitive usability and rich visual feedback.

## 1. Professional GIS Controls
- [ ] **Scale Bar**: Add a metric scale bar (`L.control.scale`) to the map corner for spatial reference.
- [ ] **Coordinate Display**: Implement a dynamic control showing cursor coordinates (Lat/Lon and UTM) in real-time.
- [ ] **MiniMap**: Add a collapsible overview map plugin for better context navigation.

## 2. Enhanced Layer Management (UI)
- [ ] **Visual Toggles**: Replace standard HTML checkboxes with modern CSS "Toggle Switches" for a more app-like feel.
- [ ] **Basemap Picker**: Create a visual "Basemap Gallery" (thumbnails) instead of hidden list items, allowing quick switching between Satellite (PNOA), Map (OpenStreetMap), and Hybrid.
- [ ] **Legend Integration**: Ensure the legend is dynamic or collapsible, not cluttering the view.

## 3. UX & Workflow Improvements
- [ ] **Context Menu**: Right-click on map to "Analyze this location" or "Copy Coordinates".
- [ ] **Loading Indicators**: Better visual feedback (skeleton screens or overlay spinners) when WMS layers are loading.
- [ ] **Responsive Adjustments**: Ensure the `split-view` stacks vertically on smaller screens (tablets).

## 4. Measurement & Analysis
- [ ] **Styling**: Improve the look of measurement tooltips (currently basic Leaflet defaults).
- [ ] **Export Options**: Make the "Export Image" feature allow selecting resolution/format.

---
*Created for session planning. Ready for implementation.*
