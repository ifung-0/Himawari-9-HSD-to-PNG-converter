# Himawari-9 Low-RAM Imagery Processor

This project downloads Himawari-9 AHI-L1b HSD segments from NOAA AWS S3,
processes them with Satpy, and writes either a single PNG or a GIF/MP4
timelapse. The pipeline is designed for a hard 10 GiB memory budget.

## Why This Version Is Low RAM

- Downloads are streamed and decompressed incrementally.
- Active downloads can be canceled from the GUI; partial `.part` files are
  cleaned up when cancellation is requested.
- Dask chunks default to `64MiB`.
- Download concurrency is capped to 4 workers.
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
- Full-disk 500 m PNG requests are automatically switched to GeoTIFF because
  the PNG/Pillow writer has to assemble one huge image in memory.

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
helpers. With `--fix`, it upgrades packages from `requirements.txt` using the
current Python. With `--auto`, it does the same repair automatically and, if
the active Python is unsupported, tries to create/use a local `.venv` with
Python 3.12 or 3.13 from the Windows `py` launcher.

## First Thing To Try

If the app fails on another machine, start from the project folder and run:

```powershell
git pull origin main
python himawari_cli.py --version
python check_environment.py --plain
python check_environment.py --auto
```

Expected current version: `2026.05.26.1`. If `--version` shows an older value,
the machine is not running the latest update. If `--plain` reports a different
Python than the one used to launch the GUI, use `run_gui.bat`, `run_cli.bat`,
or `check_environment.bat` so the same Python environment is selected.

Launch the GUI:

```powershell
python himawari_lowram_processor.py
run_gui.bat
```

The window lets you edit the Himawari URL, choose any supported band or
composite, switch between single-image and timelapse modes, set timelapse
hours/interval/FPS, choose PNG or GeoTIFF output, cap download and Dask workers,
choose the low-memory resampler, toggle night fallback, and pick output/temp
folders.
Use `Latest FLDK` to fill the URL from the most recent NOAA AWS full-disk scan
the app can find. Safe presets are available for a balanced true-color image,
a quick B13 infrared check, and a lower-RAM B13 timelapse.
It also has optional coastline/country border overlays with a user-selected
line color. The default border color is green.
Overlay rendering uses Satpy/pycoast, so install requirements first and place
pycoast-compatible coastline/border shapefiles under the project `overlays/`
folder if your environment does not already provide them. `Check Overlay Setup`
reports missing packages or missing shapefile data without downloading data.

The progress bar advances while segment downloads and timelapse frame assembly
run, and the live log panel shows memory checkpoints and processing stages.
Use `Stop Processing` to cancel the current run, including active segment
downloads and pending frames. `Check Env` runs the diagnostic checker inline;
`Quick Fix` opens the repair command in a separate console. Before processing,
the GUI shows a run summary with source, frames, bands, segment estimate, output
behavior, warnings, and blocking setup errors. After completion, use `Open Last`
and `Copy Paths`; after a failure, use `Copy Error` for a support report.

Launch the terminal interface:

```powershell
python himawari_cli.py --help
python himawari_cli.py --menu
run_cli.bat
```

The CLI uses the same `ProcessorConfig` and processing functions as the GUI.
Run without arguments, or with `--menu`, for a guided menu. Use `--run` with
flags for repeatable commands:

```powershell
python himawari_cli.py --run --composite "B13 (Infrared Window)" --mode "Single Image"
```

Windows helper launchers prefer `.venv\Scripts\python.exe` when it exists,
then `py -3.13`, `py -3.12`, and finally `python`.

Verify the installed app version:

```powershell
python himawari_cli.py --version
python check_environment.py --plain
```

Current fixed build: `2026.05.26.1`. Processing logs should include:

```text
App version: 2026.05.26.1
```

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
Outputs are written to `outputs/`. Downloaded `.DAT` files are cached under
`temp/` for retry reuse. Incomplete `.part` files are cleaned after each frame.
Timelapse frame images are written under `outputs/frames/<run_id>/`, with a
manifest under `outputs/manifests/`. Retrying the same timelapse automatically
reuses completed frame images recorded by the manifest before assembling the
GIF or MP4.

Cancellation is cooperative. Downloads stop at the next streamed chunk or
request timeout, while Satpy load/resample/save calls finish their current call
before the processor observes the stop request.

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
```

If `check_environment.py --plain` shows optional warnings only, core processing
can still work. If it shows critical failures, use the Python executable shown
by the checker when installing or launching the app.

### Wrong version or old files

Symptom: a friend says the fix is installed, but logs do not show:

```text
App version: 2026.05.26.1
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
night fallback enabled the app also downloads `B13`.

Quick fix:

```powershell
python himawari_cli.py --run --composite "True Color Reproduction Image" --night-fallback no
```

Or in the GUI, turn off `Use night fallback for day-only products` for faster
daytime single images. Already-downloaded complete `.DAT` files are kept under
`temp/`, so retrying the same timestamp can reuse them.

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
C:\Himawari\outputs
C:\Himawari\temp
```

### Timelapse finishes with no GIF/MP4

Symptom: processing ends but no GIF/MP4 is created.

Likely cause: all frames failed or were unavailable. Common causes are invalid
timestamps, missing NOAA segments, or unsupported composites in the active
Satpy environment.

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

Likely cause: the only requested frame failed. Check the log panel for the
first `Frame failed` message.

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

Then add pycoast-compatible coastline/border data to the project `overlays/`
folder.

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

## Verification

Run the local checks:

```powershell
python -m py_compile himawari_lowram_processor.py
python -m unittest discover -s tests
python -c "import himawari_lowram_processor as h; print(len(h.BAND_RESOLUTION))"
```

Full live rendering requires network access to NOAA AWS S3 and the Satpy AHI
reader dependencies.
