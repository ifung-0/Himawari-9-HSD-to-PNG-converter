# Himawari-8/9 Low-RAM Imagery Processor

This project downloads Himawari-8 and Himawari-9 AHI-L1b HSD segments from NOAA AWS S3,
processes them with Satpy, and writes either a single PNG or a GIF/MP4
timelapse. The pipeline is designed for a conservative 10 GiB memory budget,
with defaults that favor lower peak RAM over speed.

## Which Entry Point Should I Use?

- `run_gui.bat` / `himawari_lowram_processor.py`: the main HSD processor.
  Use this for normal Himawari-8/9 AHI HSD downloads, true color, B13 infrared,
  flat-map output, overlays, single images, and timelapses.
- `run_cli.bat` / `runcli.bat` / `himawari_cli.py`: the terminal interface for the same HSD
  processor. Use this for repeatable commands or scripted runs.

## Why This Version Is Low RAM

- Downloads are streamed and decompressed incrementally.
- Active downloads can be canceled from the GUI; partial `.part` files are
  cleaned up when cancellation is requested.
- Dask chunks default to `32MiB`; `16MiB` is available for slower, lower-RAM runs.
- Download concurrency defaults to 2 workers and is capped to 4 workers.
- Dask execution is capped to 1 worker by default, with a hard max of 2.
- Custom composites use lazy `xarray`/Dask operations.
- The old full-frame `np.array(...values)` and percentile colorization paths
  are gone from processing.
- Full-disk output uses Satpy's `native` resampler by default to avoid
  bilinear KD-tree index arrays that can consume multiple GiB before saving.
- Timelapse frame areas are snapped to the finest native grid needed by the
  selected product, with compatibility checks across the required bands.
- Set `IMAGE_FORMAT = "tif"` to use Satpy/rasterio chunked GeoTIFF writing for
  very large full-disk outputs.
- Large PNG requests are automatically switched to GeoTIFF because the
  PNG/Pillow writer has to assemble one huge image in memory.

## Usage

Install dependencies first:

```powershell
python install_requirements.py
```

If true color fails with a Satpy error like
`No dataset matching 'true_color' found` or
`No dataset matching 'true_color_reproduction' found`, run the environment
checker with the same Python that launches the GUI:

```powershell
python check_environment.py
python check_environment.py --auto
python check_environment.py --fix
```

The checker verifies the active Python executable, installed package versions,
Satpy's AHI reader/composite configuration, `pyspectral`, and GeoTIFF/overlay
helpers, including the optional `tkinterdnd2` package used by local file
drag/drop. With `--fix`, it upgrades packages from `requirements.txt` using the
current Python, creates required app cache/log/temp folders, archives broken
GUI/history JSON into `cleanup_archive/`, clears stale `.part` downloads from
app temp/cache folders, installs the overlay data used by border lines, and
archives confirmed obsolete root-level helper programs into `cleanup_archive/`.
The archives are reversible and ignored by git; no cleanup path deletes user
outputs. Use `--archive-unused` to run only the root-file archive step, or
`--no-archive-unused` with `--fix`/`--auto` to skip it. With `--auto`, the
checker does the same repair automatically and, if the active Python is
unsupported, tries to create/use a local `.venv` with Python 3.12 or 3.13 from
the Windows `py` launcher.

Experimental GPU acceleration is optional and NVIDIA/CUDA-only in this version.
It uses CuPy for compatible custom composite math after Satpy has loaded and
resampled on the CPU, then returns data to CPU chunks before saving. Speedups
depend on the product and output size; HSD reading, Pyresample reprojection, and
PNG/GeoTIFF writing remain CPU paths. GPU mode is intentionally limited to
`True Color Reproduction Image` and `True Color RGB (Enhanced)` in this build;
other products are blocked during setup/preflight so the run does not continue
with misleading settings. Install and check GPU support separately:

```powershell
python check_environment.py --gpu --plain
python check_environment.py --gpu-fix
```

## First Thing To Try

If the app fails on another machine, start from the project folder and run:

```powershell
git pull origin main
python himawari_cli.py --version
python check_environment.py --plain
python check_environment.py --auto
checkenv.bat
```

Expected current version: `2026.06.08.10`. If `--version` shows an older value,
the machine is not running the latest update. If `--plain` reports a different
Python than the one used to launch the GUI, use `run_gui.bat`, `run_cli.bat`,
`runcli.bat`, `check_environment.bat`, or `checkenv.bat` so the same Python
environment is selected.

Launch the GUI:

```powershell
python himawari_lowram_processor.py
run_gui.bat
```

The window opens in a simpler setup view with the source URL, safe presets,
output mode, product, output folder, run summary, setup status, and start
controls. Switch the View Mode selector to `Advanced` for timelapse timing,
Dask/download worker caps, output filename templates, resampling, temp folders,
overlays, and custom preset management.
Use `Latest FLDK` to fill the URL from the most recent NOAA AWS full-disk scan
the app can find, or `Choose Scan` to pick a recent FLDK scan and band from a
bounded NOAA AWS listing. Use `Local Files...` to import already-downloaded
Himawari HSD `.DAT` or `.DAT.bz2` segment files for offline processing; the app
streams compressed files into the normal temp cache, disables auto-download, and
processes the cached scan through the same low-RAM pipeline. Drag/drop accepts
the same files when `tkinterdnd2` is installed; otherwise the `Local Files...`
button remains available. Safe presets are
available for a balanced true-color
image, a quick B13 infrared check, and a lower-RAM B13 timelapse. Custom presets
can save and reload your current settings without changing the locked safe
presets.
The `Satellite Layer` selector controls how the source scan and flat-map styling
are chosen:

- `standard` keeps the configured URL and rendering settings. Use it for manual
  workflows, native round-disk output, single-band checks, or custom advanced
  settings.
- `live` resolves the latest available NOAA Himawari FLDK scan when processing
  starts, then applies the flat-map true-color defaults. It is latest-at-run-start
  satellite imagery, not continuous streaming.
- `hd` keeps the selected/current scan URL and applies the stronger local
  Zoom Earth-style flat-map rendering: brighter true color, chroma-speckle
  cleanup, invalid limb/disk fill replacement, green borders, labels on, and
  crosshair off.

`live` and `hd` use Himawari satellite data only. They do not download external
Zoom Earth, Google, OpenStreetMap, commercial map, radar, wind, or fire tiles.
It also has optional coastline/country border overlays with a user-selected
line color. The default border color is green, and the selected color is used
for native and Zoom Earth-style flat maps.
Flat map label text size is configurable in the GUI beside the `Labels` toggle
and from the CLI with `--map-label-size`.
In timelapse mode, `Image Format` controls the individual frame files
(`.png` or `.tif`) and `Timelapse Format` controls the assembled animation
(`.gif` or `.mp4`). If only one frame finishes successfully, the app now keeps
that single frame image instead of creating a one-frame GIF/MP4.
Overlay rendering uses Satpy/pycoast, so install requirements first and place
pycoast-compatible coastline/border shapefiles under the project `overlays/`
folder if your environment does not already provide them. `Check Overlay Setup`
creates/opens the overlay folder and reports the exact pycoast files required;
processing is blocked when border lines are enabled but those files are missing.

The progress bar advances while segment downloads and timelapse frame assembly
run, the phase/status strip summarizes the current stage, and the live log panel
shows memory checkpoints and processing details. During phases with numeric
progress, the GUI and CLI also show an estimated time remaining so you can leave
long downloads or timelapse builds running and come back later.
In Advanced > Performance, `Safe Mode` and `Best Performance` inspect current
CPU/RAM headroom and update worker, chunk-size, and RAM-limit settings without
starting a run.
Night fallback now defaults to `hybrid` for true-color products: the sunlit side
stays true color while the dark side is filled with B13 infrared grayscale, with
a soft transition near the terminator. Choose `whole_frame_ir` if you prefer the
older behavior of switching the entire frame to infrared when a scene is dark.
Advanced users can enable `flat` map view to reproject the output to a bounded
Web Mercator map, the same projection style used by Google Maps and OpenStreetMap,
while keeping the satellite image as the output. The default flat extent is lat
`-60..60`, lon `80E..200E`, at approximately `0.05` degrees per pixel at the
equator; this creates a default target around `2400x3018` pixels because Web
Mercator stretches latitude. Flat mode does not add Google labels, roads, or
live map tiles. Zoom Earth-style true-color flat maps keep central Himawari
pixels as the image and blend the low-confidence geostationary limb into a
generated rectangular ocean/land basemap, using the local GSHHS overlay data
for land fill when it is available. They can also add user-colored border
lines, static country/ocean labels, a high-contrast approximate night boundary,
and a configurable center crosshair. True-color flat maps use a direct low-RAM
night check, richer contrast/saturation, chroma-speckle cleanup, and subtler
selected-color overlays so borders and crosshairs do not look like data noise.
Single-band products keep their normal band rendering.
Radar, wind animation, heat spots, active fires, tropical systems, and
temperatures are shown as unavailable because this app does not include those
external data feeds. Native disk output remains the low-RAM default.
Use `Stop Processing` to cancel the current run, including active segment
downloads and pending frames. `Check Env` runs the diagnostic checker inline;
`Quick Fix` repairs the current Python in a separate console; `Auto Fix` uses
the stronger `.venv` repair path for wrong or unsupported Python installs.
Advanced > Performance also has `Use GPU (Experimental)` and `GPU Fix`; the
button installs optional packages from `requirements-gpu.txt` instead of the
normal CPU requirements file.
Before processing, the GUI shows a run summary with source, frames, bands, segment estimate, output
behavior, warnings, timelapse resume details, and blocking setup errors. The
same setup/preflight checks now catch unwritable output/temp folders, missing
offline cache segments, unsupported GPU/product combinations, broken border
overlay setup, invalid flat-map bounds, and unsafe output sizes before expensive
downloads or Satpy processing start. After completion, use `Open Last` and
`Copy Paths`; after a failure, use `Copy Error` for a support report. The
`Recent Runs` tab persists completed, canceled, and failed GUI runs, including
outputs, manifest/frame locations, per-run log files, re-run settings,
copy/open actions, and a safe preview for normal image outputs. Very large
images, GeoTIFFs, MP4s, and unsupported files show metadata only.

Launch the terminal interface:

```powershell
python himawari_cli.py --help
python himawari_cli.py --menu
run_cli.bat
runcli.bat
```

The CLI uses the same `ProcessorConfig` and processing functions as the GUI.
Run without arguments, or with `--menu`, for a guided menu. Use `--run` with
flags for repeatable commands:

```powershell
python himawari_cli.py --run --composite "B13 (Infrared Window)" --mode "Single Image"
python himawari_cli.py --run --composite "True Color Reproduction Image" --night-fallback-mode hybrid
python himawari_cli.py --run --satellite-layer live
python himawari_cli.py --run --satellite-layer hd
python himawari_cli.py --run --map-view flat --flat-min-lat -60 --flat-max-lat 60 --flat-min-lon 80 --flat-max-lon 200 --flat-resolution-deg 0.05
python himawari_cli.py --run --map-view flat --zoom-earth-style yes --map-labels yes --map-label-size 20 --night-boundary yes --crosshair yes --crosshair-type plus --crosshair-color "#ff0077"
python himawari_cli.py --run --gpu-acceleration yes --composite "True Color Reproduction Image"
```

Use `--satellite-layer standard`, `--satellite-layer live`, or
`--satellite-layer hd` from the CLI for the same layer modes shown in the GUI.

If you enable GPU acceleration from the CLI, choose one of the two supported
true-color products. For B13, B11/SO2, and other single-band or specialty RGB
products, leave `--gpu-acceleration no` so the normal CPU low-RAM path is used.

Windows helper launchers prefer `.venv\Scripts\python.exe` when it exists,
then `py -3.13`, `py -3.12`, and finally `python`.

Verify the installed app version:

```powershell
python himawari_cli.py --version
python check_environment.py --plain
```

Current fixed build: `2026.06.08.10`. Processing logs should include:

```text
App version: 2026.06.16.01
```

This build accepts Himawari-8 and Himawari-9 raw HSD file names and NOAA S3 URLs.
It also avoids the high-memory KD-tree path for custom true-color flat maps.
Flat true-color outputs now force the direct low-RAM RGB writer instead of Satpy's
normal composite PNG writer.
It samples the Himawari source grid into Web Mercator tiles directly, then
keeps the existing Zoom Earth-style basemap blend, borders, labels, night
boundary, and crosshair overlays. The direct flat-map night check avoids the
old high-memory visible-band reprojection path before hybrid IR blending.
For direct true-color flat maps, invalid pixels now require both valid
geostationary geometry and real sampled-band signal before they remain satellite
data, which prevents disk/limb fill pixels from becoming random red or green
artifacts in the rectangular map.

For official-looking true color and true color reproduction, keep the
lower-quality fallback checkbox off and make sure `pyspectral` is installed in
the same Python environment that launches the GUI. `install_requirements.py`
installs it from `requirements.txt`.

You can still change the default values near the top of
`himawari_lowram_processor.py` if you want the GUI to open with different
initial settings:

```python
USER_URL = "https://noaa-himawari9.s3.amazonaws.com/..."
MODE = "Single Image"
COMPOSITE_CHOICE = "True Color Reproduction Image"
IMAGE_FORMAT = "png"
```

GUI settings are saved in `himawari_gui_settings.json` and loaded on startup.
Recent run history is saved in `himawari_recent_runs.json`, and custom presets
are saved in `himawari_custom_presets.json`; all three files are ignored by git
because they contain local paths and preferences. Persistent processing logs
are written under `%LOCALAPPDATA%\Himawari9LowRamProcessor\logs`; use the GUI
`Copy Log` button to copy the latest saved run log, or the visible log panel
when a saved run log is not available yet. Outputs are written to `outputs/`.
Downloaded `.DAT` files are cached under `temp/` for retry reuse.
Incomplete `.part` files are cleaned after each frame. Timelapse frame images
are written under `outputs/frames/<run_id>/`, with a manifest under
`outputs/manifests/`. Retrying the same timelapse automatically reuses completed
frame images recorded by the manifest before assembling the GIF or MP4. The
output filename template supports `{scan_time}`, `{area}`, `{product}`, `{mode}`,
`{band}`, and `{format}` tokens and rejects unsafe path characters.

Cancellation is cooperative. Downloads stop at the next streamed chunk or
request timeout, while Satpy load/resample/save calls finish their current call
before the processor observes the stop request.

## What's New In This Build (2026.06.15.00)

### Brighter, more natural true color
True-color flat maps previously looked almost entirely dark over ocean. A
highlight-preserving shadow/midtone lift now raises dark water and faint
cloud/land detail into a legible range while protecting cloud highlights, so the
ocean reads as a brighter blue instead of near-black. The **live** and **hd**
satellite layers were also dialled back from an over-saturated, garish look to a
crisp-but-natural rendering, and their default coastline/border colour changed
from bright green to a subtle light grey-blue. If you want to fine-tune the look,
the relevant functions are `lift_true_color_shadows` and
`apply_zoom_earth_true_color_enhancement`.

### Output region presets
The **Advanced** tab has an **Output Region (Area Preset)** selector. Pick a
region to frame the output image:

- **Full Disk (native)** switches to the native full-disk view (the whole round
  Earth as the satellite sees it).
- A regional preset (Australia, Central Asia, New Zealand, Pacific Islands 1-10,
  Southeast Asia 1-3, South Asia) switches to the flat Web-Mercator map and fills
  in the latitude/longitude bounds for you.
- **Custom (manual bounds)** leaves the bounds exactly as you set them.

Because Himawari-9 sits at 140.7&deg;E, regions near the western limb (Central
Asia, South Asia) can show some edge falloff. You can always tweak the bounds by
hand after applying a preset.

### Labels on the native view
Map labels (and the night boundary, crosshair, and coastline/border overlays)
now also draw on the **native** full-disk view, not only on flat maps. Turn on
**Labels** in Options and they will be burned into native PNG/GeoTIFF output too.
Native output otherwise stays the raw satellite product (no flat-map tone
styling is applied to it).

### Copy Settings button
The new **Copy Settings** button (bottom button row) copies the settings of your
last run as formatted JSON to the clipboard. If no run has happened yet it copies
the current settings. This is handy for sharing the exact configuration with an
AI agent or in a bug report.

### Update App button
The new **Update App** button downloads the latest default branch of
`github.com/ifung-0/Himawari-9-HSD-to-PNG-converter` and replaces the local
program files. It is intentionally cautious:

- It asks for confirmation first.
- It refuses any archive entry that would write outside the project folder.
- It checks that the downloaded code compiles **before** replacing anything.
- It backs up every replaced file to a timestamped folder under `backups/`
  first, so a bad update can be undone by restoring those files.
- It updates the actually-running script file in place (so a renamed working
  copy such as `himawari_lowram_processor_claude.py` is updated correctly).

After it finishes, close and relaunch the app. If anything looks wrong, restore
the files from the newest folder under `backups/`.

## Supported Choices

All standalone bands are supported from `B01` through `B16`.

Built-in Satpy composites include true color, true color reproduction, natural
color, day/night microphysics, dust, airmass, day snow-fog, and day convective
storm mappings where available in the installed Satpy AHI configuration.

Custom lazy composites:

- `Sandwich (B03 + B13)`
- `B03 and B13 at night`
- `Heavy Rainfall Potential`

## Common Errors and Fixes

### Quick Repair Commands

Use these commands from the project folder before trying a more specific fix:

```powershell
git pull origin main
python himawari_cli.py --version
python check_environment.py --plain
python check_environment.py --auto
checkenv.bat
```

If `check_environment.py --plain` shows optional warnings only, core processing
can still work. If it shows critical failures, use the Python executable shown
by the checker when installing or launching the app.

### Wrong version or old files

Symptom: a friend says the fix is installed, but logs do not show:

```text
App version: 2026.06.08.10
```

Likely cause: their folder is still on an old commit, or they downloaded a ZIP
before the latest push.

Quick fix:

```powershell
cd path\to\Himawari-9-HSD-to-PNG-converter
git pull origin main
python himawari_cli.py --version
```

If they do not use Git, download the latest ZIP from GitHub again, then check
`python himawari_cli.py --version`.

### GUI or CLI uses the wrong Python

Symptom: requirements were installed, but the app still reports missing Satpy,
`pyspectral`, `rasterio`, or other packages.

Likely cause: packages were installed into one Python, while the GUI/CLI runs
with another Python.

Quick fix:

```powershell
python check_environment.py --plain
check_environment.bat
run_gui.bat
run_cli.bat
```

If the checker reports Python 3.14 or another unsupported version, run:

```powershell
python check_environment.py --auto
```

### Downloading 50 segments feels slow

Symptom: one full-disk true color frame downloads 40 or 50 files.

Likely cause: full-disk (`FLDK`) Himawari files are split into 10 scan segments
per band. True color reproduction needs `B01`, `B02`, `B03`, and `B04`; with
hybrid night fallback enabled the app also downloads `B13` so the night side can
be filled without producing a black half-disk.

Quick fix:

```powershell
python himawari_cli.py --run --composite "True Color Reproduction Image" --night-fallback no
```

Or in the GUI, turn off `Use night fallback for day-only products` for faster
daytime single images. Already-downloaded complete `.DAT` files are kept under
`temp/`, so retrying the same timestamp can reuse them.

### True color has a black night side

Symptom: a partly-lit FLDK true-color image shows a black side at night.

Fix: leave `Use night fallback for day-only products` enabled and keep Night
Fallback Mode set to `hybrid`. The app blends B13 infrared into dark visible
pixels lazily with xarray/Dask, so it avoids full-frame materialization. If you
want the whole image to become infrared at night, set the mode to
`whole_frame_ir`.

After pressing `Stop Processing`, wait for the canceled message before starting
again. The app lets active download workers close their `.part` files first so
Windows does not leave temporary files locked.

### `No dataset matching 'true_color' found`

Symptom: the log says Satpy cannot find `true_color`.

Likely cause: old Satpy install, broken Satpy AHI composite config, or the app
is using the wrong Python environment.

The app now falls back automatically to a custom low-RAM RGB approximation and
continues writing an output. The log will say:

```text
Satpy true_color unavailable; using custom low-RAM fallback
```

Use the checker below if you want to repair the official Satpy composite.

Quick fix:

```powershell
python check_environment.py --plain
python check_environment.py --auto
python check_environment.py --fix
```

If it still fails, check that the reported Python executable is the same one
used to launch `himawari_lowram_processor.py`.

### `Satpy true_color_reproduction unavailable; using custom low-RAM fallback`

Symptom: the saved image does not look like official true color reproduction.

Likely cause: Satpy could not create `true_color_reproduction`, so the app used
its approximate RGB fallback. That output is useful, but it is not official
Satpy/JMA true color reproduction.

Update the app first. Current versions let Satpy resample the prerequisites and
then save the generated `true_color_reproduction` dataset. If Satpy still cannot
create it, the app falls back automatically and the log will say:

```text
Satpy true_color_reproduction unavailable; using custom low-RAM fallback approximation (not official Satpy/JMA true color reproduction)
```

Use the checker below to verify that `pyspectral` and Satpy's AHI composite
configuration are available to the same Python used by the app.

Quick fix:

```powershell
git pull origin main
python himawari_cli.py --version
python check_environment.py --plain
python check_environment.py --auto
python check_environment.py --fix
```

If it still fails, check that the reported Python executable is the same one
used to launch `himawari_lowram_processor.py`. Python 3.12 or 3.13 is
recommended; if the checker reports Python 3.14, use `--auto` to create a
supported `.venv` when Python 3.12/3.13 is installed.

### True color says `pyspectral` is missing

Symptom: official-looking true color reports missing `pyspectral`.

Likely cause: `pyspectral` is not installed in the active Python environment.

Quick fix:

```powershell
python check_environment.py --fix
python check_environment.py --plain
```

As a temporary workaround, enable the lower-quality fallback checkbox or choose
`B13 (Infrared Window)`.

### Full-disk PNG changes to GeoTIFF

Symptom: image format is set to `png`, but the app saves `.tif`.

Likely cause: this is expected for very large outputs, especially 500 m
full-disk true color. PNG/Pillow has to build a huge image in memory, so the
app switches to chunked GeoTIFF for low-RAM writing.

Quick fix if GeoTIFF output fails:

```powershell
python check_environment.py --fix
```

Custom RGB outputs, including single-band flat maps such as B11/SO2, are written
with the app's chunked rasterio path and checked after saving. If an RGB GeoTIFF
is constant or near-black, the run fails instead of reporting a bad file as
saved.

Use `Single Image` for full-disk 500 m true color. Use smaller Target areas or
coarser products for PNG workflows.

### Saving GeoTIFF takes a long time

Symptom: the GUI status stays on `Saving ... .tif` for 10 minutes or longer.

Likely cause: full-disk 500 m true color is about 484 million pixels. Satpy and
Dask do much of the real composite calculation during save, so the output file
may stay small until computation finishes.

Quick fix:

```powershell
python himawari_cli.py --run --composite "B13 (Infrared Window)" --image-format tif
```

If the quick B13 test works, the original true color job is probably just heavy.
For faster true color tests, use a Target area URL such as `R301` instead of
full-disk `FLDK`.

### OneDrive or cloud sync makes saving slow

Symptom: large `.tif` saves are much slower in a synced Desktop/OneDrive folder.

Likely cause: cloud sync scans or uploads the large GeoTIFF while Satpy/rasterio
is still writing it.

Quick fix:

```powershell
python himawari_cli.py --menu
```

In the menu, set output and temp folders to local non-synced folders such as:

```text
%LOCALAPPDATA%\Himawari9LowRamProcessor\outputs
%LOCALAPPDATA%\Himawari9LowRamProcessor\temp
```

New installs use these local folders by default, even when the project code
lives in OneDrive.

### Timelapse finishes with no GIF/MP4

Symptom: processing ends but no GIF/MP4 is created.

Likely cause: all frames failed or only one frame finished. Common causes are
invalid timestamps, missing NOAA segments, network/DNS failures, or unsupported
composites in the active Satpy environment.

If one frame succeeds, the output is intentionally the frame image
(`Image Format`, usually PNG) instead of a one-frame animation
(`Timelapse Format`, GIF/MP4). Increase `Hours Back`, reduce `Interval`, or
rerun after network errors if you need an actual animation.

Full-disk 500 m true-color timelapses are intentionally rejected when the app
would need GeoTIFF frame output. GIF/MP4 assembly has to read every finished
frame into memory, which is not low-RAM at full-disk true-color size.

Quick fix:

```powershell
python check_environment.py --plain
python himawari_cli.py --run --mode Timelapse --composite "B13 (Infrared Window)" --hours-back 2 --interval-minutes 60 --timelapse-format gif
```

If B13 works, use `Single Image` for full-disk 500 m true-color GeoTIFF output
or switch to a smaller Target area for true-color timelapses.

### Single image says failed and no output was created

Symptom: the GUI reports failure and no image appears in `outputs/`.

Likely cause: the only requested frame failed. The error now includes the real
frame failure summary and the persistent run log path; open that log from
`Recent Runs > Open Log` or copy it with `Copy Error`.

Quick fix:

```powershell
git pull origin main
python check_environment.py --auto
```

### `Aggregation factors are not integers` or `Expand factor must be a whole number`

Use the latest app version. Target areas are now snapped to the finest native
grid needed by the selected product, so 500 m true color uses the B03 grid
instead of a B13-derived grid. Avoid changing the resampler to bilinear for
full-disk low-RAM work.

Quick fix:

```powershell
git pull origin main
python himawari_cli.py --version
python -m unittest discover -s tests
```

### Downloads are slow, stuck, or partially written

Symptom: downloads take a long time, stop midway, or leave `.part` files.

Likely cause: large full-disk products need many compressed NOAA segments, and
unstable networks can interrupt workers.

Quick fix:

```powershell
python himawari_cli.py --run --composite "B13 (Infrared Window)" --download-workers 1
```

Retry the same timestamp to reuse completed `.DAT` files. Partial `.part` files
are cleaned up automatically.

### Border overlays do not draw

Symptom: coastline/country borders are missing or the log says overlay failed.

Likely cause: overlay rendering needs `pycoast`, `aggdraw`, and compatible
coastline/border shapefiles. The Python packages are installed by requirements,
but shapefile data may still need to be placed under `overlays/`.

Quick fix:

```powershell
python check_environment.py --fix
```

Quick Fix installs/upgrades the overlay Python packages in the current Python,
creates required app folders, backs up corrupt GUI/history JSON, clears stale
partial downloads from app temp/cache folders, creates the project `overlays/`
folder if needed, downloads the official GSHHG shapefile archive from SOEST
mirrors, and extracts the low-resolution GSHHS/WDBII files the app needs. If
every mirror is unreachable, place
`gshhg-shp-2.3.7.zip` in the project folder, `overlays/`, `downloads/`, the app
overlay cache, or your Windows `Downloads` folder, then run Quick Fix again. It
will use the local archive automatically. For the default low-resolution overlay
setting, the app expects these files:

```text
overlays/GSHHS_shp/l/GSHHS_l_L1.shp
overlays/GSHHS_shp/l/GSHHS_l_L1.dbf
overlays/WDBII_shp/l/WDBII_border_l_L1.shp
overlays/WDBII_shp/l/WDBII_border_l_L1.dbf
```

If border lines are enabled and these files or the `.shx` sidecars are missing
or empty, the GUI and CLI block Start before running the long image-processing
job. Check Env also reports a `pycoast overlay runtime` result so package/data
problems are visible before processing. Disable border lines to process without
overlays.

### MP4 output falls back to GIF

Symptom: timelapse format is set to MP4, but a GIF is written.

Likely cause: MP4 writing needs `imageio-ffmpeg`. If it is missing, the app
logs a warning and writes a GIF instead.

Quick fix:

```powershell
python check_environment.py --fix
```

### `URL not recognised`

Symptom: the app rejects the URL before downloading.

Likely cause: the app expects a NOAA AWS AHI object URL or NOAA AWS index URL.
Make sure the URL includes an AHI-L1b area (`FLDK`, `Target`, or `Japan`),
timestamp, band, resolution, and segment pattern.

Example object URL:

```text
https://noaa-himawari9.s3.amazonaws.com/AHI-L1b-FLDK/2024/07/25/0400/HS_H09_20240725_0400_B01_FLDK_R10_S0110.DAT.bz2
```

Himawari-8 raw HSD object URLs and local filenames are also accepted:

```text
https://noaa-himawari8.s3.amazonaws.com/AHI-L1b-FLDK/2022/12/13/0400/HS_H08_20221213_0400_B01_FLDK_R10_S0110.DAT.bz2
HS_H08_20221213_0400_B13_FLDK_R20_S0110.DAT
```

## Verification

Run the local checks:

```powershell
python -m py_compile himawari_lowram_processor.py
python -m unittest discover -s tests
python -c "import himawari_lowram_processor as h; print(len(h.BAND_RESOLUTION))"
```

Full live rendering requires network access to NOAA AWS S3 and the Satpy AHI
reader dependencies.
