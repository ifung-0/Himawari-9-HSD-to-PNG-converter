# Himawari-8/9 Low-RAM Imagery Processor

This app downloads Himawari-8 and Himawari-9 satellite data (AHI L1b HSD segments
from the NOAA public archive on AWS), turns it into pictures, and saves either a
single PNG or a GIF/MP4 timelapse. It is built to run on a modest machine: the
defaults favour low memory use (a conservative 10 GiB budget) over raw speed, so
it works on laptops that cannot hold a whole full-disk image in memory at once.

If you just want a picture quickly, read the Quick Start. If something breaks,
jump to Troubleshooting. For a deep, function-by-function explanation of how the
program works, see the companion `Details.md`.

---

## Quick Start

1. Install the Python packages (use the same Python you will launch the app with):

   ```powershell
   python install_requirements.py
   ```

2. Launch the app:

   - Windows: double-click `run_gui.bat`
   - Any system: `python himawari_lowram_processor.py`

3. In the window:
   - Click **Latest FLDK** to fill in the most recent full-disk scan.
   - Leave the product on **True Color Reproduction Image**.
   - Click **Quick Look** to see a fast, coarse preview first (optional but recommended).
   - Click **Start Processing**. Read the run summary (it now includes a rough
     time estimate), then confirm.

4. When it finishes, click **Open Outputs** to see your image.

That is the whole loop: pick a scan, pick a product, preview, run.

---

## Two Ways To Run It

- **Graphical app** (`run_gui.bat` or `himawari_lowram_processor.py`): the normal
  way. Buttons, previews, presets, and a live log.
- **Command line** (`run_cli.bat`, `runcli.bat`, or `himawari_cli.py`): the same
  engine without the window, for repeatable or scripted runs.

Both share the same processing code, so an image looks the same either way.

---

## What's New In This Build (2026.06.17.01)

### True color flat maps now match the native look
Flat-map (rectangular) true color used to come out washed out and yellow-tinted
compared with the round native view: hazy cream clouds over a muddy teal ocean.
Standard flat maps now use a gentle, faithful tone curve instead of the heavy
stylised one, so the ocean stays a deep natural blue and clouds stay clean white,
closely matching the native image. The punchy look is still used on purpose for
the **live** and **hd** satellite layers and for **Zoom Earth-style** flat maps.

### Quick Look (live preview)
A new **Quick Look** button renders a small, coarse flat-map preview (about 2 km
per pixel) in a few seconds so you can check framing and colour before committing
to a slow full-resolution run. It never changes your real settings.

### Pick Region (clickable map)
A new **Pick Region** button opens a small world map with the Himawari coverage
area outlined. Drag a box to set your flat-map crop instead of typing latitude
and longitude by hand. As you drag, it shows the resulting image size in pixels
and a rough memory estimate, and warns if the area is too large.

### Segment-aware downloads
Full-disk scans are split into 10 horizontal strips ("segments") per band. When
you crop to a region, the app now works out which strips actually cover your
latitude band and downloads only those. A crop over New Zealand or Japan might
fetch 4 of 10 segments per band instead of all 10, which is roughly half the
download. This is on by default and can be toggled in Options.

### Test Data Host (connectivity check)
A new **Test Data Host** button checks whether the NOAA data server is reachable
(a DNS lookup plus a quick web request) and reports the result with timings. Use
it to tell "my run failed" apart from "my network/VPN/firewall is blocking it".

### Metadata sidecar files
The app can write a small companion file next to each image describing exactly
what it is: a `.json` with the geographic bounds, scan time, projection, pixel
size, and the settings used, plus an ESRI world file (and a `.prj`) so the image
lines up correctly when dropped into GIS software. This is on by default and can
be toggled in Options.

### Overlay colour themes
A new **Overlay Theme** dropdown in Options offers ready-made colour palettes for
the borders, labels, night-line, and crosshair (for example "Subtle Light",
"High-Contrast Night", "Warm Amber"). Choosing one fills in all four colours at
once. Pick "Custom (keep my colors)" to leave your own colours alone. You can
still set each colour by hand, and the label and night-line colours now have
their own pickers.

### Better time estimates
The run summary now shows a rough wall-clock estimate before you start, based on
how many segments will download, how many workers you use, and the output size.
The live progress estimate during a run is also smoothed so it stops jumping
around. Estimates are clearly marked as rough; the live one becomes accurate as
real download speed is measured.

---

## The Main Controls

The window has three tabs: **Run Setup**, **Advanced**, and **Recent Runs**.

### Source
Where the satellite data comes from.

- **Latest FLDK**: fill the URL with the newest full-disk scan.
- **Choose Scan**: browse recent scan times and pick one.
- **Local Files**: use `.DAT`/`.DAT.bz2` segment files you already have instead
  of downloading.

### Product
What kind of image to make.

- **True Color Reproduction Image** and **True Color RGB (Enhanced)**: natural
  daytime colour.
- **B01-B16**: any single band on its own.
- Custom composites: `Sandwich (B03 + B13)`, `B03 and B13 at night`,
  `Heavy Rainfall Potential`, `Day Snow-Fog RGB`, and the built-in Satpy
  composites available in your install.

### Map View
- **native**: the round full Earth, exactly as the satellite sees it.
- **flat**: a rectangular Web Mercator map you can crop to a region.

### Options
Overlays and behaviour:

- Auto-download missing files, night fallback for day-only products, and
  "allow lower-quality fallback" if a true-color dependency is missing.
- Coastline/border lines, labels (with size), night boundary, and crosshair
  (with type and colour).
- **Overlay Theme** plus colour pickers for border, label, and night-line.
- **Segment-aware downloads** and **Write a metadata sidecar** toggles.

### Tools (bottom buttons)
- **Quick Look**: fast coarse preview.
- **Pick Region**: drag-a-box region chooser.
- **Test Data Host**: network reachability check.
- **Check Env**, **Quick Fix**, **Auto Fix**: environment health and repair.
- **Open Outputs**, **Open Last**, **Copy Paths/Error/Log/Settings**: results and
  sharing helpers.
- **Update App**, **Help (?)**.

Hover any button to see a one-line tooltip; **Help (?)** lists them all.

### Advanced tab
Performance (download workers, Dask workers and chunk size, RAM limit, GPU
toggle), output and temp folders, the resampler, custom presets, and the
**Output Region (Area Preset)** chooser with regional presets that fill in flat
bounds for you.

---

## Why This Version Is Low RAM

- Downloads are streamed and decompressed in pieces, not held whole in memory.
- Regional crops download only the scan segments they need (segment-aware).
- Dask chunks default to 32 MiB (16 MiB is available for tighter machines).
- Download concurrency defaults to 2 workers, capped at 4; Dask is capped at 2.
- Custom composites use lazy xarray/Dask math instead of building giant arrays.
- Full-disk output uses Satpy's `native` resampler to avoid huge index arrays.
- Very large PNG requests automatically switch to GeoTIFF, which can be written
  in chunks instead of assembled in one piece in memory.
- Flat-map true color is sampled directly at the target resolution, so a small
  regional crop never materialises the full disk.

---

## Where Files Go

- Finished images and timelapses: your **Output** folder (set on the Advanced
  tab; **Open Outputs** opens it).
- Each image can have a `.json` + world file (`.pgw`/`.tfw`) + `.prj` beside it.
- Temporary downloads: the **Temp** folder, reused across retries of the same
  scan so you do not re-download complete segments.
- Run logs: a per-run log file (**Copy Log** copies the on-screen log; **Open
  Last** and the Recent Runs tab help you find outputs and logs again).

---

## Optional: GPU Acceleration

GPU mode is experimental and NVIDIA/CUDA-only. It uses CuPy for compatible custom
composite math after Satpy has loaded and resampled on the CPU. It is limited to
the two true-color products in this build; other products are blocked during
setup so a run does not continue with misleading settings.

```powershell
python check_environment.py --gpu --plain
python check_environment.py --gpu-fix
```

---

## Troubleshooting

Start here for almost anything. From the project folder:

```powershell
python himawari_cli.py --version
python check_environment.py --plain
python check_environment.py --auto
```

If `--plain` shows only optional warnings, core processing still works. If it
shows critical failures, install packages with the exact Python the checker
reports, or launch through `run_gui.bat` / `run_cli.bat` so the right Python is
used.

**A run failed and I am not sure why.** Click **Test Data Host** first. If it
reports the host is unreachable, the problem is your network (VPN, firewall,
proxy, or an outage), not the imagery. If the host is reachable, use **Copy
Error** and check the log.

**The app reports missing Satpy, pyspectral, or rasterio even after installing.**
Packages went into one Python while the app runs under another. Run
`check_environment.py --plain` to see which Python is active, then launch with
`run_gui.bat` / `run_cli.bat`, or run `check_environment.py --auto` if the active
Python version is unsupported (for example 3.14).

**A friend says the fix is installed but the version looks old.** Their folder is
on an old copy. `git pull origin main`, then `python himawari_cli.py --version`.
Without Git, re-download the latest ZIP.

**Downloading feels slow / it fetches many files.** Full-disk scans are 10
segments per band, and true color needs four bands (plus B13 when hybrid night
fallback is on). Crop to a region so segment-aware downloads skip unused
segments, or turn off night fallback for daytime single images. Complete files in
the Temp folder are reused on retry.

**True color has a black night side.** Keep "Use night fallback for day-only
products" on with mode `hybrid`; the app blends B13 infrared into the dark side.
Use `whole_frame_ir` if you want the whole night image to be infrared. After
pressing **Stop**, wait for the canceled message before starting again.

**Satpy says it cannot find `true_color` / `true_color_reproduction`.** Run
`python check_environment.py --auto` to repair the Satpy AHI configuration. If
the built-in composite is unavailable, the app falls back to its own low-RAM true
color automatically.

**True color says `pyspectral` is missing.** Install it with the active Python
(the environment fixer does this), or enable "allow lower-quality fallback" to
render without it.

**A full-disk PNG turns into a GeoTIFF.** That is intentional: a PNG that large
must be assembled in memory, so the app switches to chunked GeoTIFF writing.
Choose a smaller region or a coarser product for PNG.

**Saving a GeoTIFF is slow, or cloud sync makes saving slow.** Point the Output
folder at a local (non-synced) folder, or use a smaller area/coarser product.

**A timelapse finished with no GIF/MP4.** You need at least two frames; increase
Hours Back or reduce the interval. Very large frames are blocked for GIF/MP4
assembly, so use Single Image or a smaller area for big outputs.

**Border overlays do not draw.** Use **Check Overlay Setup**, then **Quick Fix**
to install the coastline/border data, or turn off border lines.

**URL not recognised.** Use **Latest FLDK** or **Choose Scan** to insert a
correctly formatted URL rather than pasting one by hand.

---

## At A Glance

- Products: all bands `B01`-`B16`, true color, true color reproduction, several
  custom composites, and the built-in Satpy AHI composites in your install.
- Views: native full disk, or flat Web Mercator with a region crop.
- Output: PNG or GeoTIFF single image, or GIF/MP4 timelapse, optionally with a
  metadata sidecar.
- Built for low memory, with previews, region picking, connectivity testing, and
  rough time estimates to make runs predictable.

For the full internals (every button and function explained), see `Details.md`.
