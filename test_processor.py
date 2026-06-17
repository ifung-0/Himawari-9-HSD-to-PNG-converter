"""Unit tests for the zoom-earth true-color colour-quality fixes.

Covers the two regressions fixed in build 2026.06.17.02:

1. ``apply_zoom_earth_true_color_enhancement`` used to apply a warm "blue cut"
   to the *entire* disk, turning the Earth yellow/brown (disk B/R ~0.79 vs the
   reference ~0.95).  A synthetic neutral RGB disk must now stay neutral.
2. ``generated_flat_map_ocean_image`` used to lay down a saturated deep-blue
   gradient that showed through invalid pixels as a blue veil / "blue spot at
   the top".  The background must now be a neutral dark gray-blue.
"""

from __future__ import annotations

import types

import numpy as np
import pytest
from PIL import Image

import hlrp  # loaded by conftest.py


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _neutral_disk_image(width: int = 160, height: int = 160) -> Image.Image:
    """A neutral gray disk on a neutral dark background (matches the basemap)."""
    rgb = np.full((height, width, 3), hlrp.FLAT_MAP_BASEMAP_OCEAN, dtype=np.uint8)
    yy, xx = np.indices((height, width))
    cy, cx = height / 2.0, width / 2.0
    radius = min(width, height) * 0.42
    disk = np.sqrt((yy - cy) ** 2 + (xx - cx) ** 2) < radius
    # Neutral mid-gray disk: R == G == B everywhere on the disk.
    rgb[disk] = 150
    return Image.fromarray(rgb, mode="RGB")


class _FakeArea:
    """Minimal stand-in for a pyresample AreaDefinition.

    ``generated_flat_map_ocean_image`` only reads ``width``/``height`` from the
    area (the projection math in ``flat_map_lonlat_vectors`` is monkeypatched in
    the ocean test), so this tiny object is sufficient.
    """

    def __init__(self, width: int, height: int) -> None:
        self.width = width
        self.height = height


def _fake_flat_map_lonlat_vectors(area):
    """Synthetic lon/lat vectors spanning the full Web-Mercator latitude band."""
    width = int(area.width)
    height = int(area.height)
    lon = np.linspace(-180.0, 180.0, width, dtype=np.float32)
    lat = np.linspace(85.0, -85.0, height, dtype=np.float32)
    return lon, lat


# ---------------------------------------------------------------------------
# Change 4a: version bump
# ---------------------------------------------------------------------------
def test_app_version_bumped():
    assert hlrp.APP_VERSION == "2026.06.17.06"


# ---------------------------------------------------------------------------
# Change 1: zoom-earth enhancement keeps a neutral disk neutral
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("hd", [False, True])
def test_zoom_earth_enhancement_keeps_disk_neutral(hd):
    disk = _neutral_disk_image()
    mask = np.asarray(disk.convert("RGB"))[:, :, 0] == 150

    enhanced = hlrp.apply_zoom_earth_true_color_enhancement(disk, hd=hd)
    arr = np.asarray(enhanced.convert("RGB"), dtype=np.float32)
    disk_pixels = arr[mask]

    mean_r = disk_pixels[:, 0].mean()
    mean_g = disk_pixels[:, 1].mean()
    mean_b = disk_pixels[:, 2].mean()

    # Target disk ratio R:G:B ~= 1.00 : 0.98 : 0.95.  Allow a small tolerance
    # band: the disk must stay neutral/bright, never yellow-brown (B/R ~0.79).
    assert mean_r > 0
    g_over_r = mean_g / mean_r
    b_over_r = mean_b / mean_r
    assert 0.95 <= g_over_r <= 1.05, f"G/R out of range for hd={hd}: {g_over_r:.3f}"
    assert 0.95 <= b_over_r <= 1.05, f"B/R out of range for hd={hd}: {b_over_r:.3f}"


@pytest.mark.parametrize("hd", [False, True])
def test_zoom_earth_enhancement_does_not_cut_blue_globally(hd):
    """The old code cut blue by ~14% everywhere; B/R must not collapse to ~0.8."""
    disk = _neutral_disk_image()
    mask = np.asarray(disk.convert("RGB"))[:, :, 0] == 150
    enhanced = hlrp.apply_zoom_earth_true_color_enhancement(disk, hd=hd)
    arr = np.asarray(enhanced.convert("RGB"), dtype=np.float32)
    disk_pixels = arr[mask]
    b_over_r = disk_pixels[:, 2].mean() / disk_pixels[:, 0].mean()
    # The reference target is ~0.95; the broken output was ~0.79.  Guard both.
    assert b_over_r >= 0.90, f"blue was cut too hard (B/R={b_over_r:.3f})"


# ---------------------------------------------------------------------------
# Change 2: neutral ocean basemap
# ---------------------------------------------------------------------------
def test_flat_map_basemap_ocean_is_neutral():
    r, g, b = hlrp.FLAT_MAP_BASEMAP_OCEAN
    # A neutral dark gray-blue: blue must not dominate red by more than a small
    # margin (the old (15, 32, 52) had blue ~3.5x red). Allow up to 16 so the
    # basemap can be a visible dark blue-gray (not near-black) without being a
    # saturated blue veil.
    assert b - r <= 16, f"basemap ocean too blue: {hlrp.FLAT_MAP_BASEMAP_OCEAN}"
    assert b - g <= 12, f"basemap ocean too blue vs green: {hlrp.FLAT_MAP_BASEMAP_OCEAN}"
    # Must be visible (not near-black) so off-disk corners don't look black.
    assert b >= 14, f"basemap ocean too dark (corners would look black): {hlrp.FLAT_MAP_BASEMAP_OCEAN}"


def test_flat_map_invalid_fill_is_neutral():
    r, g, b = hlrp.FLAT_MAP_INVALID_FILL
    # Invalid fill shows through outside the disk; it must not be a saturated
    # blue (the old (12, 26, 44) caused the "blue spot at the top").
    assert b - r <= 16, f"invalid fill too blue: {hlrp.FLAT_MAP_INVALID_FILL}"
    # Must be visible (not near-black) so off-disk corners don't look black.
    assert b >= 14, f"invalid fill too dark (corners would look black): {hlrp.FLAT_MAP_INVALID_FILL}"


def test_generated_flat_map_ocean_background_is_neutral(monkeypatch):
    monkeypatch.setattr(hlrp, "flat_map_lonlat_vectors", _fake_flat_map_lonlat_vectors)
    area = _FakeArea(width=180, height=120)
    ocean = hlrp.generated_flat_map_ocean_image(area)
    arr = np.asarray(ocean.convert("RGB"), dtype=np.int16)
    red, green, blue = arr[:, :, 0], arr[:, :, 1], arr[:, :, 2]

    # Background blue must not exceed red + green by more than a small margin.
    excess = blue - (red + green)
    assert excess.max() <= 8, f"background blue exceeds red+green by {excess.max()}"

    # And there must be no saturated blue region (old background reached ~103).
    assert blue.max() < 60, f"background has saturated blue region: max={blue.max()}"
    # Background should be dark and even (no strong blue latitude gradient).
    assert blue.max() - blue.min() <= 25, (
        f"blue gradient still too strong: range={blue.max() - blue.min()}"
    )


# ===========================================================================
# Build 2026.06.17.03 — regression tests for the three high-severity fixes
# ===========================================================================


# ---------------------------------------------------------------------------
# Fix #1: max_safe_png_pixels is bound to the GUI and round-trips through
# settings. The bug was that _read_config never passed it to the constructor,
# so every save reset it to the default.
# ---------------------------------------------------------------------------
def test_max_safe_png_pixels_in_config_field_names():
    """The field must be a recognised config field so it round-trips via JSON."""
    assert "max_safe_png_pixels" in hlrp.processor_config_field_names()


def test_max_safe_png_pixels_round_trips_through_settings(tmp_path):
    custom = 99_999_999
    cfg = hlrp.ProcessorConfig(max_safe_png_pixels=custom)
    serialized = hlrp.serialize_gui_settings(cfg, tmp_path, tmp_path)
    assert serialized["config"].get("max_safe_png_pixels") == custom

    restored = hlrp.config_from_mapping(serialized["config"])
    assert restored is not None
    assert restored.max_safe_png_pixels == custom


def test_read_config_binds_max_safe_png_pixels():
    """_read_config must pass max_safe_png_pixels from a GUI variable.

    The original bug: the field existed and was validated/used at runtime, but
    _read_config omitted it, so every save wrote the default. We assert the
    binding is present in the source so the regression cannot silently return.
    """
    import inspect

    src = inspect.getsource(hlrp.HimawariProcessorApp._read_config)
    assert "max_safe_png_pixels" in src, (
        "_read_config no longer reads max_safe_png_pixels from the GUI; the "
        "fix-#1 regression has returned."
    )
    set_src = inspect.getsource(hlrp.HimawariProcessorApp._set_config_vars)
    assert "max_safe_png_pixels" in set_src, (
        "_set_config_vars no longer pushes max_safe_png_pixels to the GUI."
    )


def test_max_safe_png_pixels_var_initialised_in_init():
    """The GUI var must be created in __init__ so the bindings resolve."""
    import inspect

    src = inspect.getsource(hlrp.HimawariProcessorApp.__init__)
    assert "max_safe_png_pixels_var" in src


# ---------------------------------------------------------------------------
# Fix #2: _poll_messages must not die when a handler raises. The original bug:
# the reschedule (root.after) was outside any try/except, so one handler
# exception permanently froze the GUI.
# ---------------------------------------------------------------------------
class _StubRoot:
    """Minimal stand-in for tk.Tk: records after() callbacks instead of running."""

    def __init__(self):
        self.after_calls = []

    def after(self, _ms, fn):
        self.after_calls.append(fn)


def _make_stub_app():
    """Build an object with just enough state for the poll-loop methods.

    We bind the REAL unbound methods from the class so the test exercises the
    actual fixed code, not a copy. No Tk root or widgets are needed because the
    poll loop only touches self.messages, self.root.after and _handle_message.
    """
    import queue

    class _Stub:
        pass

    app = _Stub()
    app.root = _StubRoot()
    app.messages = queue.Queue()
    # Bind the real methods.
    app._poll_messages = hlrp.HimawariProcessorApp._poll_messages.__get__(app)
    app._handle_message = hlrp.HimawariProcessorApp._handle_message.__get__(app)
    return app


def test_poll_messages_survives_handler_exception():
    """A handler that raises must NOT stop the poll loop from rescheduling."""
    app = _make_stub_app()
    # Queue: a message whose handler raises, then a normal message.
    app.messages.put(("boom", None))
    app.messages.put(("log", "still alive"))

    # Make _handle_message raise for "boom" but succeed for "log".
    original = app._handle_message

    def exploding(kind, payload):
        if kind == "boom":
            raise RuntimeError("simulated handler failure")
        original(kind, payload)

    app._handle_message = exploding

    app._poll_messages()

    # The poll loop must have drained past the failing message and processed the
    # normal one (no exception escaped), and must have rescheduled itself.
    assert app.root.after_calls, "poll loop did not reschedule itself after a handler exception"
    assert app.messages.empty(), "poll loop stopped before draining the queue"


def test_poll_messages_reschedules_on_empty_queue():
    """Even with an empty queue the reschedule must fire (the normal path)."""
    app = _make_stub_app()
    app._poll_messages()
    assert len(app.root.after_calls) == 1


# ---------------------------------------------------------------------------
# Fix #3: closing the window / picking a colour must persist settings. The
# original bug: no WM_DELETE_WINDOW handler, and _choose_color never saved.
# ---------------------------------------------------------------------------
def test_on_close_window_persists_settings():
    """_on_close_window must call _write_current_settings before destroying."""
    import inspect

    src = inspect.getsource(hlrp.HimawariProcessorApp._on_close_window)
    assert "_write_current_settings" in src, (
        "_on_close_window no longer saves settings; fix-#3 regression returned."
    )


def test_wm_delete_window_registered_in_init():
    """__init__ must register the WM_DELETE_WINDOW close handler."""
    import inspect

    src = inspect.getsource(hlrp.HimawariProcessorApp.__init__)
    assert 'WM_DELETE_WINDOW' in src, (
        "__init__ no longer registers WM_DELETE_WINDOW; settings would be lost on close."
    )


def test_choose_color_persists_after_pick():
    """_choose_color must save settings after a successful colour pick."""
    import inspect

    src = inspect.getsource(hlrp.HimawariProcessorApp._choose_color)
    assert "_write_current_settings" in src, (
        "_choose_color no longer persists the picked colour; fix-#3 regression returned."
    )


def test_choose_color_end_to_end_calls_save():
    """End-to-end: a successful colour pick triggers _write_current_settings.

    Uses a stub app (no real Tk) with the real _choose_color method bound, and
    a monkeypatched colorchooser that returns a value.
    """
    saved = {"calls": 0}

    class _StubVar:
        def __init__(self, value):
            self._value = value

        def get(self):
            return self._value

        def set(self, value):
            self._value = value

    class _StubApp:
        def __init__(self):
            self.some_var = _StubVar("#ff0000")

        def _write_current_settings(self):
            saved["calls"] += 1

    app = _StubApp()
    method = hlrp.HimawariProcessorApp._choose_color.__get__(app)

    # Monkeypatch the colorchooser used inside the module.
    original_askcolor = hlrp.colorchooser.askcolor
    hlrp.colorchooser.askcolor = lambda color=None, title=None: ((0, 255, 0), "#00ff00")
    try:
        method(app.some_var, "title", "#000000")
    finally:
        hlrp.colorchooser.askcolor = original_askcolor

    assert app.some_var.get() == "#00ff00"
    assert saved["calls"] == 1, f"expected one save call, got {saved['calls']}"


# ===========================================================================
# Build 2026.06.17.04 — regression tests for fixes #4 and #5
# ===========================================================================


# ---------------------------------------------------------------------------
# Fix #4: load_gui_settings must not discard ALL settings when the only error
# is GPU-not-ready. It should auto-disable GPU and keep everything else.
# ---------------------------------------------------------------------------
def _write_settings_file(path, config_dict, output_dir, temp_dir):
    import json

    payload = {
        "schema_version": hlrp.GUI_SETTINGS_SCHEMA_VERSION,
        "app_version": hlrp.APP_VERSION,
        "config": config_dict,
        "output_dir": str(output_dir),
        "temp_dir": str(temp_dir),
    }
    path.write_text(json.dumps(payload))


def test_load_settings_preserves_non_gpu_settings_when_gpu_unavailable(
    tmp_path, monkeypatch
):
    """GPU enabled + GPU not ready -> GPU auto-disabled, other settings kept."""
    # A valid config with GPU on and a distinctive custom URL + bounds.
    cfg = hlrp.default_config()
    cfg = hlrp.replace(cfg, gpu_acceleration=True, user_url="https://example/custom/URL")
    cfg = hlrp.replace(cfg, flat_min_lat=-40.0, flat_max_lat=10.0)
    custom_pixels = 55_555_555
    cfg = hlrp.replace(cfg, max_safe_png_pixels=custom_pixels)

    settings_path = tmp_path / "settings.json"
    _write_settings_file(settings_path, cfg.__dict__, tmp_path, tmp_path)

    # Force GPU support to be unavailable and ensure the GPU composite check
    # does not add an unrelated error.
    monkeypatch.setattr(
        hlrp, "gpu_support_status", lambda: hlrp.GpuSupportStatus(False, "no CUDA")
    )
    monkeypatch.setattr(hlrp, "can_build_gpu_custom_composite", lambda *_a, **_k: True)

    result = hlrp.load_gui_settings(settings_path)
    assert result is not None, "settings were discarded instead of GPU being auto-disabled"
    loaded_cfg, _out, _tmp = result

    # GPU must be off now ...
    assert loaded_cfg.gpu_acceleration is False
    # ... but every other setting must be preserved (this is the bug being fixed).
    assert loaded_cfg.user_url == "https://example/custom/URL"
    assert loaded_cfg.flat_min_lat == -40.0
    assert loaded_cfg.flat_max_lat == 10.0
    assert loaded_cfg.max_safe_png_pixels == custom_pixels


def test_load_settings_still_rejects_genuinely_broken_settings(tmp_path, monkeypatch):
    """A non-GPU error (e.g. bad product) must still reject the file."""
    monkeypatch.setattr(
        hlrp, "gpu_support_status", lambda: hlrp.GpuSupportStatus(True, "ok")
    )
    monkeypatch.setattr(hlrp, "can_build_gpu_custom_composite", lambda *_a, **_k: True)

    cfg = hlrp.default_config()
    # Inject a product that is not in COMPOSITE_BANDS.
    cfg = hlrp.replace(cfg, composite_choice="Not A Real Product")
    settings_path = tmp_path / "settings.json"
    _write_settings_file(settings_path, cfg.__dict__, tmp_path, tmp_path)

    assert hlrp.load_gui_settings(settings_path) is None


def test_gpu_related_error_classifier():
    assert hlrp._gpu_related_error("GPU acceleration is enabled, but GPU support is not ready.") is True
    assert hlrp._gpu_related_error("Himawari URL is required.") is False


# ---------------------------------------------------------------------------
# Fix #5: _apply_satellite_layer_defaults must mirror the map_label_size
# clamping that layer_defaults_config applies at runtime.
# ---------------------------------------------------------------------------
class _StringVar:
    """Minimal tk.StringVar stand-in (no display needed)."""

    def __init__(self, value=""):
        self._value = str(value)

    def get(self):
        return self._value

    def set(self, value):
        self._value = str(value)


def test_apply_satellite_layer_defaults_clamps_map_label_size():
    """Setting 'live' must clamp map_label_size to the same value the runtime uses."""
    # A label size that is out of range so clamping changes it.
    bad_size = str(hlrp.MAP_LABEL_SIZE_MAX + 50)
    expected = hlrp.satellite_layer_map_label_size(bad_size)
    assert expected != int(bad_size), "test precondition: bad size must be clamped"

    class _StubApp:
        def __init__(self):
            self.satellite_layer_var = _StringVar("standard")
            self.composite_var = _StringVar()
            self.map_view_var = _StringVar()
            self.zoom_earth_style_var = _StringVar("false")
            self.night_fallback_var = _StringVar()
            self.night_fallback_mode_var = _StringVar()
            self.border_lines_var = _StringVar()
            self.map_labels_var = _StringVar()
            self.crosshair_var = _StringVar()
            self.border_color_var = _StringVar()
            self.border_width_var = _StringVar("1.0")
            self.map_label_size_var = _StringVar(bad_size)
            self._log = []

        def _append_log(self, msg):
            self._log.append(msg)

        def _update_setup_status(self):
            pass

        def _write_current_settings(self):
            pass

    app = _StubApp()
    app.satellite_layer_var.set("live")
    method = hlrp.HimawariProcessorApp._apply_satellite_layer_defaults.__get__(app)
    method()

    assert int(app.map_label_size_var.get()) == expected, (
        f"GUI label size {app.map_label_size_var.get()} != runtime clamped {expected}"
    )
    # And the runtime path must agree (the original divergence source).
    runtime_cfg = hlrp.layer_defaults_config(
        hlrp.replace(hlrp.default_config(), satellite_layer_mode="live", map_label_size=int(bad_size))
    )
    assert runtime_cfg.map_label_size == expected


def test_apply_satellite_layer_defaults_does_not_touch_label_size_in_standard_mode():
    """In 'standard' mode no clamping should happen (matches runtime)."""

    class _StubApp:
        def __init__(self):
            self.satellite_layer_var = _StringVar("standard")
            self.map_label_size_var = _StringVar("33")

        def _append_log(self, msg):
            pass

        def _update_setup_status(self):
            pass

        def _write_current_settings(self):
            pass

    app = _StubApp()
    method = hlrp.HimawariProcessorApp._apply_satellite_layer_defaults.__get__(app)
    method()
    assert app.map_label_size_var.get() == "33"


# ===========================================================================
# Build 2026.06.17.05 — Pick Region shows a real map
# ===========================================================================


def test_pick_region_landmass_data_is_valid():
    """Every landmass polygon must be a closed-ish list of valid lon/lat points."""
    assert hlrp.SIMPLIFIED_LANDMASSES, "no landmass data defined"
    for polygon in hlrp.SIMPLIFIED_LANDMASSES:
        assert len(polygon) >= 3, f"polygon too small: {polygon}"
        for lon, lat in polygon:
            assert -360.0 <= lon <= 360.0, f"longitude out of range: {lon}"
            assert -90.0 <= lat <= 90.0, f"latitude out of range: {lat}"


def test_pick_region_map_is_not_blank():
    """At least several landmass polygons must intersect the picker window.

    The picker shows longitude 60E..140W and latitude 85S..85N. If none of the
    embedded polygons fell inside that window the map would be blank and the
    feature useless, so guard the coverage.
    """
    lon_min, lon_max = 60.0, 220.0
    lat_min, lat_max = -85.0, 85.0

    def intersects(poly):
        lons = [p[0] for p in poly]
        lats = [p[1] for p in poly]
        return not (
            max(lons) < lon_min
            or min(lons) > lon_max
            or max(lats) < lat_min
            or min(lats) > lat_max
        )

    visible = [p for p in hlrp.SIMPLIFIED_LANDMASSES if intersects(p)]
    assert len(visible) >= 8, (
        f"only {len(visible)} landmass polygons are visible in the picker; the map would be sparse"
    )


def test_pick_region_has_orientation_labels():
    """The map must carry at least a handful of place-name labels in-window."""
    lon_min, lon_max = 60.0, 220.0
    lat_min, lat_max = -85.0, 85.0
    in_window = [
        t for t, lon, lat in hlrp.SIMPLIFIED_LAND_LABELS
        if lon_min <= lon <= lon_max and lat_min <= lat <= lat_max
    ]
    assert len(in_window) >= 8, f"only {len(in_window)} labels fall in the picker window"


def test_pick_region_landmasses_constant_referenced_by_dialog():
    """The dialog's drawing code must actually use the landmass data."""
    import inspect

    src = inspect.getsource(hlrp.RegionPickerDialog._draw_landmasses)
    assert "SIMPLIFIED_LANDMASSES" in src


# ===========================================================================
# Build 2026.06.17.06 — fixes for red spots, blue top edge, black corners
# ===========================================================================


def test_chroma_cleanup_runs_after_finish_in_pipeline():
    """Red spots: cleanup must run AFTER finish_zoom_earth_true_color_quality so
    it catches specks re-emphasised by the final saturation/sharpen pass."""
    import inspect

    src = inspect.getsource(hlrp.apply_flat_map_style_to_image)
    finish_pos = src.find("finish_zoom_earth_true_color_quality")
    cleanup_pos = src.find("cleanup_true_color_chroma_speckles", finish_pos if finish_pos >= 0 else 0)
    assert finish_pos >= 0 and cleanup_pos >= 0, "pipeline functions missing"
    assert cleanup_pos > finish_pos, (
        "chroma cleanup must run AFTER finish_zoom_earth_true_color_quality, not before"
    )


def test_subtle_blue_limb_detector_exists():
    """Blue top: a subtle-blue-limb detector must be present so the gentler
    enhancement's mild blue edge tint gets neutralised."""
    import inspect

    src = inspect.getsource(hlrp.flat_map_edge_artifact_mask)
    assert "subtle_blue_limb" in src, "subtle blue limb detector missing"


def test_basemap_blend_forces_alpha_on_invalid_pixels():
    """Black corners: the blend alpha must be forced to 1.0 on all invalid
    pixels so far corners fully show the basemap instead of the raw fill."""
    import inspect

    src = inspect.getsource(hlrp.flat_map_basemap_blend_alpha)
    assert "normalized_invalid" in src and "alpha[normalized_invalid] = 1.0" in src, (
        "basemap blend no longer forces alpha=1 on invalid pixels (black corners would return)"
    )


def test_off_disk_fill_is_not_near_black():
    """The invalid fill / basemap must be visible (not near-black) so off-disk
    corners don't look black against the brighter disk."""
    r, g, b = hlrp.FLAT_MAP_INVALID_FILL
    assert max(r, g, b) >= 14, f"invalid fill too dark: {hlrp.FLAT_MAP_INVALID_FILL}"
    r, g, b = hlrp.FLAT_MAP_BASEMAP_OCEAN
    assert max(r, g, b) >= 14, f"basemap ocean too dark: {hlrp.FLAT_MAP_BASEMAP_OCEAN}"
