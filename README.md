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
- Timelapse frame areas are snapped to a native-compatible B13 grid before
  resampling so Satpy's native resampler keeps integer expansion/reduction
  factors across shifted Target scans.
- Set `IMAGE_FORMAT = "tif"` to use Satpy/rasterio chunked GeoTIFF writing for
  very large full-disk outputs.
- Full-disk 500 m PNG requests are automatically switched to GeoTIFF because
  the PNG/Pillow writer has to assemble one huge image in memory.

## Usage

Install dependencies first:

```powershell
python install_requirements.py
```

If true color reproduction fails with a Satpy error like
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

Launch the GUI:

```powershell
python himawari_lowram_processor.py
```

The window lets you edit the Himawari URL, choose any supported band or
composite, switch between single-image and timelapse modes, set timelapse
hours/interval/FPS, choose PNG or GeoTIFF output, cap download and Dask workers,
choose the low-memory resampler, toggle night fallback, and pick output/temp
folders.
It also has optional coastline/country border overlays with a user-selected
line color. The default border color is green.
Overlay rendering uses Satpy/pycoast, so install requirements first and place
pycoast-compatible coastline/border shapefiles under the project `overlays/`
folder if your environment does not already provide them.

The progress bar advances while segment downloads and timelapse frame assembly
run, and the live log panel shows memory checkpoints and processing stages.
Use `Stop Download` to cancel the current run, including active segment
downloads and pending frames. Use `Terminate` to request cancellation and close
the GUI.

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
COMPOSITE_CHOICE = "True Color RGB (Enhanced)"
IMAGE_FORMAT = "png"
```

Outputs are written to `outputs/`. Temporary DAT files are written under
`temp/` and cleaned after each frame.

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

### Downloading 50 segments feels slow

Full-disk (`FLDK`) Himawari files are split into 10 scan segments per band.
True color reproduction needs `B01`, `B02`, `B03`, and `B04`; with night
fallback enabled the app also downloads `B13`, so one frame can be 50 files.

For a faster daytime single image, turn off `Use night fallback for day-only
products` to avoid the extra `B13` band. Keep the temp folder between retries
so already-downloaded segments can be reused.

After pressing `Stop Download`, wait for the canceled message before starting
again. The app lets active download workers close their `.part` files first so
Windows does not leave temporary files locked.

### `No dataset matching 'true_color_reproduction' found`

Satpy cannot find the AHI true color reproduction composite. This is usually
an old Satpy install, a broken Satpy config install, or the GUI running with a
different Python than the one where requirements were installed.

The app now falls back automatically to a custom low-RAM RGB approximation and
continues writing an output. The log will say:

```text
Satpy true_color_reproduction unavailable; using custom low-RAM fallback
```

Use the checker below if you want to repair the official Satpy composite.

Fix:

```powershell
python check_environment.py
python check_environment.py --auto
python check_environment.py --fix
```

If it still fails, check that the reported Python executable is the same one
used to launch `himawari_lowram_processor.py`. Python 3.12 or 3.13 is
recommended; if the checker reports Python 3.14, use `--auto` to create a
supported `.venv` when Python 3.12/3.13 is installed.

### True color says `pyspectral` is missing

The official-looking true color composites need `pyspectral`.

Fix:

```powershell
python install_requirements.py --upgrade
python check_environment.py
```

As a temporary workaround, enable the lower-quality fallback checkbox or choose
`B13 (Infrared Window)`.

### Full-disk PNG changes to GeoTIFF

This is expected for very large outputs, especially 500 m full-disk true color.
PNG/Pillow has to build a huge image in memory, so the app switches to chunked
GeoTIFF for low-RAM writing.

Fix: install `rasterio` if GeoTIFF output fails.

```powershell
python check_environment.py --fix
```

### Timelapse finishes with no GIF/MP4

This usually means all frames failed or were unavailable. Check the log panel
for the first frame error. Common causes are invalid timestamps, missing NOAA
segments, or unsupported composites in the active Satpy environment.

Fix:

```powershell
python check_environment.py
```

Then try a simple timelapse with `B13 (Infrared Window)`, `gif`, and a short
time range before using heavier true color products.

### Single image says failed and no output was created

Older builds could show a final "Done" dialog with an empty output list after
the only frame failed. Current builds report this as a failure. Check the log
panel for the first `Frame failed` message, then run:

```powershell
python check_environment.py --auto
```

### `Aggregation factors are not integers` or `Expand factor must be a whole number`

Use the latest app version. Timelapse target areas are now snapped to a native
B13 grid before Satpy native resampling. Avoid changing the resampler to
bilinear for full-disk low-RAM work.

Fix:

```powershell
git pull
python -m unittest discover -s tests
```

### Downloads are slow, stuck, or partially written

Large full-disk products need many compressed NOAA segments. Use `Stop
Download` to cancel safely. Partial `.part` files are cleaned up automatically.

Fix: retry the same timestamp, lower the download worker count if the network
is unstable, or test with a single low-resolution band like `B13`.

### Border overlays do not draw

Overlay rendering needs `pycoast`, `aggdraw`, and compatible coastline/border
shapefiles. The Python packages are installed by requirements, but shapefile
data may still need to be placed under `overlays/`.

Fix:

```powershell
python check_environment.py --fix
```

Then add pycoast-compatible coastline/border data to the project `overlays/`
folder.

### MP4 output falls back to GIF

MP4 writing needs `imageio-ffmpeg`. If it is missing, the app logs a warning
and writes a GIF instead.

Fix:

```powershell
python check_environment.py --fix
```

### `URL not recognised`

The app expects a NOAA AWS AHI object URL or NOAA AWS index URL. Make sure the
URL includes an AHI-L1b area (`FLDK`, `Target`, or `Japan`), timestamp, band,
resolution, and segment pattern.

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
