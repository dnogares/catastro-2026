# Tasks - Suite Tasación SaaS

## Current Objective
Stabilize the application and ensure successful integration between Backend (FastAPI) and Frontend (Leaflet/HTML).

## Todo List
- [x] Fix `NameError: analyzer` in `main.py` <!-- id: 0 -->
- [x] Fix `contextily` import error in `vector_analyzer.py` <!-- id: 1 -->
- [x] Refactor `catastro_downloader.py` structure <!-- id: 2 -->
- [x] **Frontend Verification** <!-- id: 3 -->
    - [x] Check `index.html` API calls against `main.py` endpoints.
    - [x] Ensure map handling expects the correct JSON structure.
- [x] **Urbanismo Module Check** <!-- id: 4 -->
    - [x] Verify `analisisurbano_mejorado.py` usage in `main.py`.
- [x] **Final Integration Test** <!-- id: 5 -->
    - [x] Static analysis of imports in `catastro_downloader.py`.
- [x] **Requirements Gathering for Visor** <!-- id: 6 -->
    - [x] Created `visor_requirements.md`.
- [ ] **Implement Visor Enhancements** <!-- id: 7 -->
    - [x] Added Scale Bar & Coordinates.
    - [x] Added Toggle Switches (partial).
    - [ ] Complete Toggle Switches for all layers.
    - [ ] Add MiniMap (optional).
