# Himawari-8/9 Low-RAM Processor: Technical Details

This document explains how the program works internally: its architecture, the
processing pipeline, every user-facing control and the function behind it, and
the algorithms used by the newer features. It is written for researchers and
developers who want to understand, audit, or extend the code. The program lives
in a single module, `himawari_lowram_processor.py`; references below use the
function and class names from that file.

For a task-oriented user guide, see `README.md`.

---

## 1. High-Level Architecture

The program is a single-file Python application with three layers:

1. **Configuration layer.** A frozen-by-convention dataclass, `ProcessorConfig`,
   holds every setting. Module-level constants (near the top of the file) supply
   the default value for each field, so adding a new field with a default is
   automatically backward-compatible with saved settings.

2. **Processing layer.** Pure functions that download data, build composites,
   reproject, render, and save. The entry point is `run()`, which orchestrates
   per-frame work through `process_frame()`. This layer has no Tk dependency and
   can be driven from the command line.

3. **GUI layer.** The `HimawariProcessorApp` class builds a Tkinter interface,
   reads and writes `ProcessorConfig` through Tk variables, and runs the
   processing layer on a background thread, communicating results back through a
   thread-safe `queue.Queue`.

Scientific work uses Satpy (the `ahi_hsd` reader and its composites), xarray and
Dask for lazy arrays, pyresample for geometry/reprojection, NumPy and Pillow for
pixel math, and requests for HTTP. Imports are at the top; the GUI runs under
Tkinter.

### Data flow at a glance

```
URL or local files
   -> parse_url -> UrlInfo (host, satellite, timestamp, area, segment count)
   -> frame_datetimes -> list of scan times (1 for single image, many for timelapse)
   -> master area:
        flat:   flat_map_area  (Web Mercator, cropped to bounds)
        native: common_area_from_frames (geostationary full disk)
   -> per frame: process_frame
        required_bands -> make_download_tasks -> download_segments
        (optional) day/night check -> select_active_composite
        Satpy Scene -> build composite -> resample/direct-sample -> save image
        -> apply overlays/styling -> (optional) write metadata sidecar
   -> (timelapse) assemble_timelapse -> GIF/MP4
```

---

## 2. Configuration and Persistence

- `ProcessorConfig` (dataclass) is the single source of truth for a run. Each
  field has a module-constant default (for example `flat_resolution_deg =
  FLAT_RESOLUTION_DEG`).
- `default_config()` returns a config built from those defaults.
- `serialize_gui_settings()` / `load_gui_settings()` save and restore settings as
  JSON. Loading starts from `default_config().__dict__` and overlays only keys
  that match `processor_config_field_names()`, so old files missing new keys load
  cleanly with the new defaults. The settings schema version is deliberately not
  bumped when fields are added this way.
- `layer_defaults_config()` applies the **satellite layer** presets (`standard`,
  `live`, `hd`): `live`/`hd` force true-color reproduction, the flat map, Zoom
  Earth styling, hybrid night fallback, and a subtle border colour.
- `validate_configuration()` checks every field and raises `ValueError` with a
  human message on bad input. It validates colours via `parse_rgb_color`, the
  flat-map bounds via `checked_flat_map_parameters`, the overlay theme against
  `OVERLAY_THEME_ORDER`, and so on.

---

## 3. Source Parsing and Downloading

- `parse_url()` turns a NOAA S3 object URL into a `UrlInfo`: the bucket root, the
  satellite id (`HS_H08`/`HS_H09`), the scan timestamp, the area code (`FLDK` for
  full disk), and `total_segments` (10 for standard full-disk scans, derived from
  the `Sxxyy` token in the filename).
- `required_bands()` returns the bands a product needs. True color reproduction
  needs `B01`-`B04`; with hybrid night fallback, `B13` is added so the night side
  can be filled.
- `make_download_tasks()` builds one `DownloadTask` (URL + destination) per band
  per segment. It accepts an optional `segment_numbers` list so a regional crop
  can request a subset of segments (see Section 8.1).
- `download_segments()` downloads tasks concurrently (workers capped at 4),
  streaming and decompressing `.bz2` in chunks, writing `.part` files that are
  renamed on success and cleaned up on cancellation. It reuses complete files
  already present in the Temp folder.
- `check_data_host_connectivity()` is a standalone reachability probe (Section
  8.4) used by the **Test Data Host** button.

---

## 4. Composites

`build_custom_composite()` dispatches on the chosen product:

- **Single band** (`B01`-`B16`): `single_band_to_rgb()` maps one band to a
  grayscale or calibrated RGB.
- **True color** (`True Color Reproduction Image`, `True Color RGB (Enhanced)`):
  Satpy's built-in `true_color_reproduction` composite is preferred. When it is
  unavailable, `create_true_color_reproduction_fallback()` builds an RGB directly
  from `B01`-`B04` using lazy xarray math (reflectance scaling, black-point,
  contrast, saturation, and a slight warm bias). This same fallback is also used
  for the low-RAM flat-map path.
- **Custom composites**: `create_sandwich_composite` (B03+B13),
  `create_b03_b13_night`, `create_heavy_rainfall_rgb` (B08/B13/B15),
  `create_day_snow_fog_rgb` (B03/B05/B13).
- **Night handling**: `apply_hybrid_night_if_needed()` and
  `create_hybrid_day_night_rgb()` blend B13 infrared into dark visible pixels so
  a partly-lit disk does not show a black half. `select_active_composite()`
  chooses the day or night variant based on a sampled darkness check.

---

## 5. Geometry: Native and Flat Map

### Native (geostationary)
`common_area_from_frames()` builds the full-disk geostationary `AreaDefinition`
at the finest pixel size the product needs (`target_pixel_size_m`,
`area_reference_band`). Native output is the raw product; overlays may still be
drawn on top.

### Flat map (Web Mercator)
- `flat_map_area()` builds a Web Mercator `AreaDefinition` cropped to the
  configured latitude/longitude bounds.
- `web_mercator_extent()` converts lat/lon bounds to Web Mercator metres via
  pyproj; `flat_map_shape()` converts the extent and the chosen degrees-per-pixel
  into output width/height; `checked_flat_map_parameters()` validates bounds and
  the pixel budget (`MAX_FLAT_MAP_PIXELS`).
- `direct_flat_map_sample_band()` samples a loaded band directly at the target
  grid using the loaded scene's own area
  (`get_array_coordinates_from_lonlat`). Because Satpy builds the area to match
  the loaded (possibly partial) segments, sampling by lat/lon stays correct even
  when only a subset of segments is downloaded.

---

## 6. The Per-Frame Pipeline (`process_frame`)

For each scan time:

1. Compute `required_bands` and, for flat maps, the segment subset via
   `segments_for_flat_bounds()`.
2. `make_download_tasks` + `download_segments`.
3. If the product is day-only and night fallback is on, sample a coarse area to
   decide `is_night`, then `select_active_composite`.
4. Build a Satpy `Scene` from the downloaded files.
5. Choose the output filename and enforce a safe format
   (`enforce_safe_output_format` switches oversized PNG to GeoTIFF).
6. Render and save:
   - Flat-map true color: `save_custom_composite_output()` direct-samples and
     writes the image, then applies styling.
   - Satpy composites: load, `resample_scene_low_ram()`, `save_satpy_dataset_output()`.
7. On success (`frame_succeeded`), the `finally` block optionally writes the
   metadata sidecar (Section 8.3) and cleans up partial downloads.

`process_frame` has several success return points; all set `frame_succeeded` and
return the output path, so the sidecar hook in `finally` runs exactly once per
successful frame with a valid `output_path` and chosen composite `active`.

---

## 7. Rendering, Styling, and Overlays

The styling entry point for written images is `apply_flat_map_style_to_image()`.
It operates on an RGBA image and an `AreaDefinition`:

1. Fill invalid (off-disk / out-of-bounds) pixels.
2. For true-color products, run artifact cleanup and a tone curve (Section 8.5).
3. For Zoom Earth style, composite a synthetic basemap under translucent overlays.
4. Draw, in order: coastline/border overlays (`direct_overlay_to_image`), the
   night boundary (`draw_night_boundary`), labels (`draw_zoom_earth_labels`), and
   the crosshair (`draw_crosshair`).

Supporting pieces:
- `parse_rgb_color()` accepts a colour name, `#RRGGBB`, or `R,G,B`.
- `build_overlay_options()` configures pycoast for coastlines/borders.
- `draw_night_boundary()` computes the day/night terminator from the sub-solar
  point (`solar_declination_and_subsolar_lon`, `night_boundary_points`) and draws
  it as a haloed line whose colour is themeable.
- `draw_zoom_earth_labels()` places city/region labels with a haloed font; the
  label colour is themeable.

---

## 8. Feature Internals

### 8.1 Segment-aware regional downloads

Full-disk scans are 10 equal-height horizontal segments per band. Segment k
covers the same fraction of the disk at every band resolution, so one latitude
table applies to all bands.

- `_geos_pixel_to_lonlat()` implements the CGMS/LRIT geostationary inverse
  navigation (using the fixed AHI full-disk constants `AHI_*`) to map a 2 km
  pixel (column, line) to (lon, lat); off-disk pixels return NaN. The disk centre
  maps to roughly (140.7 E, 0).
- `fldk_segment_latitude_bounds(total)` samples a dense grid of columns and lines
  within each segment and returns each segment's (min_lat, max_lat) band.
- `segments_intersecting_lat_bounds(total, min_lat, max_lat, margin)` returns a
  contiguous, sorted list of segments whose band overlaps the requested
  latitudes, padded by `margin` segments on each side. If nothing intersects or
  the geometry cannot be evaluated, it returns all segments, so the result is
  always a safe superset.
- `segments_for_flat_bounds(info, config)` returns the subset for the current
  flat crop, or `None` (download everything) when segment-aware mode is off, the
  source is not full disk, the view is native, or the crop already spans the
  whole disk.

Correctness argument: any segment containing a pixel within the requested
latitude band necessarily has its (min_lat, max_lat) overlap that band, so it is
selected; the margin adds slack. No needed latitude data is dropped, and the
selected segments are contiguous so Satpy can build a clean partial area.

### 8.2 Live preview (Quick Look)

- `build_preview_config(config)` derives a fast config: force the flat map (using
  the current bounds, or `PREVIEW_FLAT_FALLBACK_BOUNDS` when the user is on the
  native view), coarsen the resolution to at least `PREVIEW_FLAT_RESOLUTION_DEG`
  (~2 km), force a single PNG, enable segment-aware downloads and quality
  fallback, and disable the sidecar and GPU. Product and overlay choices are
  preserved so the preview resembles the final image.
- The GUI handler `_start_preview()` validates the preview config, runs the
  normal `run()` on a worker thread, and posts the result. `_show_preview_window()`
  opens a Toplevel and displays the image downscaled to
  `PREVIEW_MAX_DIMENSION_PX`. Because it reuses the real pipeline, the preview is
  representative rather than a separate approximation.

### 8.3 Output metadata sidecar

- `write_output_sidecar(output_path, config, info, scan_time, area, product)`
  writes, next to each image:
  - a `.json` describing the product, scan time, satellite, view, pixel
    dimensions, projection (proj4 and extent), pixel size, and the key settings;
    plus geographic bounds (the exact crop in degrees for flat maps, or
    approximate corner lat/lon for native, omitted for a full disk whose corners
    fall off the Earth);
  - an ESRI world file (`_world_file_path` picks `.pgw`/`.tfw`/`.jgw`/`.wld`)
    whose six lines are the pixel sizes and the centre of the upper-left pixel,
    computed from the area extent and dimensions;
  - a `.prj` with the projection WKT when available.
- It is hooked into `process_frame`'s `finally` block, gated on `frame_succeeded`,
  and wrapped so any failure is logged and never breaks a successful run.

### 8.4 Connectivity check (Test Data Host)

- `data_host_for_source(source)` resolves which bucket host to test from the
  current source URL (falling back to the Himawari-9 bucket).
- `check_data_host_connectivity(source, timeout)` performs a DNS lookup
  (`socket.getaddrinfo`, timed) and a lightweight HTTP `HEAD` to the bucket
  (timed), and returns a `ConnectivityResult`. Any HTTP status (including S3's
  403/405 on the root) counts as reachable because it proves the request reached
  the host. `ConnectivityResult.display_text()` renders a human summary; the GUI
  runs the probe on a worker thread and shows it.

### 8.5 True-color tone curves (the flat-map colour fix)

Two tone curves exist for true-color flat maps, selected in
`apply_flat_map_style_to_image`:

- **Cosmetic** (`apply_zoom_earth_true_color_enhancement` followed by speckle
  cleanup and `finish_zoom_earth_true_color_quality`): a deliberately punchy,
  stylised look used for Zoom Earth style and the `live`/`hd` layers. It lifts
  shadows strongly, warms clouds, and boosts saturation.
- **Faithful** (`apply_faithful_true_color_enhancement`): a gentle,
  hue-preserving correction used for standard flat maps. It applies a small
  shadow lift (`lift_true_color_shadows`, strength 0.22), mild
  contrast/colour/brightness/sharpness, and a soft highlight roll-off
  (`compress_true_color_highlights`). This keeps the ocean a deep natural blue
  and clouds a clean neutral white, closely matching the native True Color
  Reproduction image, instead of the previous washed-out, yellow-tinted result.

The branch condition is "cosmetic if `config.zoom_earth_style` or
`is_enhanced_satellite_layer(config)`, else faithful". Speckle cleanup
(`cleanup_true_color_chroma_speckles`) runs in both branches to remove impossible
red/green/magenta flecks while sparing genuine land and warm cloud tops.

### 8.6 Overlay themes

- `OVERLAY_THEMES` maps a theme name to four colours (border, label,
  night_boundary, crosshair); `OVERLAY_THEME_CUSTOM` means "keep my colours".
- `overlay_theme_colors(name)` returns the palette dict or `None` for custom.
- The GUI handler `_on_overlay_theme_change()` applies a chosen palette to the
  four colour variables at once. At render time, `apply_flat_map_style_to_image`
  reads `config.map_label_color` and `config.night_boundary_color` and passes
  them to the draw functions, which accept an optional colour and fall back to
  their original hard-coded colour when none is given.

### 8.7 Visual region picker (Pick Region)

`RegionPickerDialog` is a pure-Tkinter Toplevel:

- It draws an equirectangular mini-map (longitude 60-220, latitude -85..85) with
  a graticule, the Himawari full-disk coverage outline, and the sub-satellite
  point.
- Dragging a box converts canvas pixels to lat/lon
  (`_x_to_lon`/`_y_to_lat`), clamps latitude to the Web Mercator limit, and shows
  a live estimate computed with the real `flat_map_shape()` (output pixels and a
  rough peak-memory figure), warning when the area exceeds `MAX_FLAT_MAP_PIXELS`.
- Confirming sets the main window's flat-bound variables and switches it to the
  flat view.

### 8.8 Time estimation

- `estimate_run_seconds(config, frame_count, band_count, segments_per_frame,
  output_megapixels)` combines parallel download time (segments / workers x a
  per-segment constant), per-frame render time (a fixed overhead plus a
  per-megapixel cost), and timelapse assembly. The constants (`ESTIMATE_*`) are
  conservative averages.
- `estimated_output_megapixels(config, info)` derives the output size from the
  flat-map area (flat) or the full-disk size at the product's finest pixel size
  (native).
- `build_run_summary()` computes the segment-aware download count and the
  estimate and stores them on `RunSummary`, whose `display_text()` shows the
  download breakdown and `format_estimated_duration()` (a coarse, friendly
  bucket like "about 4 min").
- `ProgressEtaEstimator` produces the live ETA during a run; it now smooths the
  per-unit rate with an exponential moving average so the displayed ETA settles
  instead of jumping on each tick.

---

## 9. The GUI

`HimawariProcessorApp` builds the window in `_build_ui()`: a **Run Setup** tab
(Source, Product, Timelapse, Options, Simple View, Setup Status), an **Advanced**
tab (Performance, Paths and Resampling, Custom Presets, Output Region), a
**Recent Runs** tab, and a bottom button row.

Settings flow:
- `__init__` creates a Tk variable for every config field.
- `_read_config()` builds a `ProcessorConfig` from the variables.
- `_set_config_vars()` pushes a config back into the variables.
- `_install_setup_watchers()` traces all variables so any change refreshes the
  setup status and run summary (`_update_setup_status`, which calls
  `build_setup_status` and `build_run_summary`).

Running:
- `_start()` reads and validates the config, runs preflight
  (`preflight_run`), shows the run summary for confirmation, then launches
  `_run_worker()` on a daemon thread.
- `_run_worker()` calls `run()` and posts `("done"/"error"/"canceled", ...)` to
  `self.messages`.
- `_poll_messages()` (scheduled via `root.after`) drains the queue and updates
  the UI: log lines, progress and ETA, completion dialogs, environment results,
  connectivity results, preview windows, and recent-run records.

Button handlers of note: `_start`, `_stop_current_task`, `_open_environment_check`
/ `_open_environment_fix` / `_open_environment_auto_fix`, `_check_overlays`,
`_fill_latest_fldk_url`, `_open_scan_browser`, `_choose_local_hsd_files`,
`_apply_area_preset`, `_on_zoom_earth_toggle`, `_on_overlay_theme_change`,
`_start_preview`, `_open_region_picker`, `_start_connectivity_check`,
`_update_from_github`, and `_open_help_window`. Tooltips come from
`_button_help_texts()` via `_install_tooltips()`, and the same text populates the
Help window.

---

## 10. Output, Recent Runs, and Updates

- Output filenames come from a template (`output_template`, validated by
  `validate_output_template`) with tokens such as scan time, area, and product.
- `assemble_timelapse()` builds a GIF or MP4 from per-frame images; timelapse
  runs use a manifest so they can resume.
- Recent runs are recorded (`build_recent_run_record`) and shown on the Recent
  Runs tab with actions to open outputs/logs and re-run settings.
- **Update App** (`_update_from_github`) downloads the latest source, verifies it
  compiles before replacing anything, backs up replaced files under `backups/`,
  and updates the running script in place.

---

## 11. Extending the Program

- **Add a setting**: add a module-constant default, add the field to
  `ProcessorConfig`, wire it into `_read_config`, `_set_config_vars`, the setup
  watchers, and (if user-facing) a control in `_build_ui` plus help text. Saved
  settings remain compatible because loading fills missing keys with defaults.
- **Add a product**: extend `COMPOSITE_BANDS`/`required_bands` and add a builder
  in `build_custom_composite`.
- **Add an overlay theme**: add an entry to `OVERLAY_THEMES`.
- **Tune true-color colour**: adjust `apply_faithful_true_color_enhancement`
  (standard flat maps) or `apply_zoom_earth_true_color_enhancement` (cosmetic
  modes); both preserve alpha and hue.

Because the processing layer is import-safe and free of Tk, individual functions
(navigation, segment selection, tone curves, world-file math) can be unit-tested
in isolation without launching the GUI.
