"""Self-contained tests for the 9-style true-color batch, the typhoon-track
overlay, and the true-color colour quality.

These import ``himawari_lowram_processor`` directly (no conftest/``hlrp`` alias
is required) so they run from the project root with a plain ``pytest``. Anything
that needs the heavy scientific stack (satpy/pyresample resampling) is avoided:
the tests exercise the pure-Python config logic and the numpy/PIL drawing and
colour paths only.
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

import numpy as np
import pytest
from PIL import Image

import himawari_lowram_processor as p


UTC = timezone.utc


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
class _FakeArea:
    """Minimal AreaDefinition stand-in for the label/typhoon pixel maths."""

    width = 400
    height = 300
    area_extent = (0.0, 0.0, 400.0, 300.0)
    crs = "EPSG:4326"


def _fake_lonlat_to_pixel(area, lon, lat):
    """lon 120..145 -> x 0..width, lat 25..5 -> y 0..height (else off-image)."""
    x = (float(lon) - 120.0) / 25.0 * area.width
    y = (25.0 - float(lat)) / 20.0 * area.height
    if 0.0 <= x <= area.width and 0.0 <= y <= area.height:
        return (x, y)
    return None


def _channel_means(image):
    arr = np.asarray(image.convert("RGB"), dtype=np.float64)
    return arr[..., 0].mean(), arr[..., 1].mean(), arr[..., 2].mean()


def _sample_storm_payload():
    return {
        "storms": [
            {
                "name": "Testtyphoon",
                "id": "wp0299",
                "basin": "WP",
                "track": [
                    {"time": "2026-07-01T18:00:00Z", "lat": 12.0, "lon": 135.0, "wind_kt": 45, "category": "TS"},
                    {"time": "2026-07-02T00:00:00Z", "lat": 13.2, "lon": 133.5, "wind_kt": 75, "category": "TY"},
                    {"time": "2026-07-02T06:00:00Z", "lat": 14.5, "lon": 132.0, "wind_kt": 110, "category": "Cat 3"},
                ],
                "forecast": [
                    {"time": "2026-07-02T12:00:00Z", "lat": 16.0, "lon": 130.5, "wind_kt": 120},
                    {"time": "2026-07-02T18:00:00Z", "lat": 18.0, "lon": 129.0, "wind_kt": 100},
                ],
            }
        ]
    }


# ---------------------------------------------------------------------------
# 9-style true-color batch (3 satellite layers x 3 map styles)
# ---------------------------------------------------------------------------
def test_style_set_has_three_cells():
    # The live/HD satellite layers were removed: only the standard layer remains,
    # rendered in the three map styles (native, flat, Zoom Earth flat).
    assert len(p.TRUE_COLOR_STYLE_SET) == 3
    assert len(p.TRUE_COLOR_SATELLITE_LAYERS) == 1
    assert len(p.TRUE_COLOR_MAP_STYLES) == 3


def test_style_set_covers_every_map_style():
    suffixes = {suffix for _label, suffix, _ov in p.TRUE_COLOR_STYLE_SET}
    assert suffixes == {"native", "flat_standard", "flat_zoomearth"}


def test_style_set_is_standard_layer_only():
    for _label, _suffix, overrides in p.TRUE_COLOR_STYLE_SET:
        assert overrides["satellite_layer_mode"] == "standard"


def test_style_set_overrides_are_well_formed():
    for _label, _suffix, overrides in p.TRUE_COLOR_STYLE_SET:
        assert overrides["satellite_layer_mode"] in p.SATELLITE_LAYER_MODES
        assert overrides["map_view"] in {"native", "flat"}
        assert isinstance(overrides["zoom_earth_style"], bool)
        # Native cells are never Zoom Earth styled; that combination is invalid.
        if overrides["map_view"] == "native":
            assert overrides["zoom_earth_style"] is False


def test_style_base_config_disables_layer_presets():
    base = p.true_color_style_base_config(p.default_config())
    assert base.layer_style_presets is False
    assert base.composite_choice == p.TRUE_COLOR_REPRODUCTION_PRODUCT
    assert base.mode == "Single Image"


def test_style_variants_validate_and_keep_their_projection():
    base = p.true_color_style_base_config(p.default_config())
    for _label, _suffix, overrides in p.TRUE_COLOR_STYLE_SET:
        variant = replace(base, **overrides)
        # layer_defaults_config is now a no-op (standard layer), so each cell keeps
        # exactly the projection it asked for.
        resolved = p.layer_defaults_config(variant)
        assert p.normalized_map_view(resolved.map_view) == p.normalized_map_view(overrides["map_view"])
        assert resolved.zoom_earth_style == overrides["zoom_earth_style"]
        # No cell is ever "enhanced" now that live/HD are gone.
        assert p.is_enhanced_satellite_layer(resolved) is False
        # Full validation (which exercises the flat-map projection maths) is run
        # only on the native cells so the check does not depend on a real pyproj.
        if p.normalized_map_view(overrides["map_view"]) == "native":
            p.validate_configuration(variant)


# ---------------------------------------------------------------------------
# Satellite-layer removal: only the standard layer remains
# ---------------------------------------------------------------------------
def test_only_standard_layer_mode_remains():
    assert p.SATELLITE_LAYER_MODES == ("standard",)


def test_legacy_layer_values_collapse_to_standard():
    # Old saved configs / presets may still say live/hd/zoom_earth; they must load
    # and be treated as standard rather than erroring.
    for legacy in ("live", "hd", "zoom_earth", "anything"):
        assert p.normalized_satellite_layer_mode(legacy) == "standard"
        cfg = replace(p.default_config(), satellite_layer_mode=legacy)
        assert p.is_enhanced_satellite_layer(cfg) is False
        # A legacy value must not produce a configuration error.
        assert not p.setup_configuration_errors(cfg)


def test_layer_defaults_is_noop_for_standard():
    # The standard layer never forces flat / Zoom Earth styling, so a native +
    # borders-off config is preserved exactly (this is what fixes the region /
    # pick-region choice from being silently overridden).
    cfg = replace(
        p.default_config(),
        satellite_layer_mode="standard",
        map_view="native",
        zoom_earth_style=False,
        add_border_lines=False,
    )
    resolved = p.layer_defaults_config(cfg)
    assert resolved.map_view == "native"
    assert resolved.zoom_earth_style is False
    assert resolved.add_border_lines is False


def test_standard_layer_is_never_enhanced():
    cfg = replace(p.default_config(), satellite_layer_mode="standard")
    assert p.is_enhanced_satellite_layer(cfg) is False


# ---------------------------------------------------------------------------
# Output quality (flat-map resolution) presets
# ---------------------------------------------------------------------------
def test_output_quality_levels_round_trip():
    for name, deg in p.OUTPUT_QUALITY_LEVELS:
        assert p.output_quality_resolution_deg(name) == deg
        assert p.output_quality_for_resolution(deg) == name


def test_output_quality_higher_is_finer_resolution():
    degs = [deg for _name, deg in p.OUTPUT_QUALITY_LEVELS]
    # Ordered low -> high quality, so degrees-per-pixel must strictly decrease
    # (higher quality == higher resolution == fewer degrees per pixel).
    assert degs == sorted(degs, reverse=True)


def test_output_quality_custom_for_unmatched_resolution():
    assert p.output_quality_for_resolution(0.0417) == p.OUTPUT_QUALITY_CUSTOM
    assert p.OUTPUT_QUALITY_CUSTOM in p.output_quality_names()


def test_overlay_render_scale_grows_with_image_size():
    class _Area:
        def __init__(self, w, h):
            self.width = w
            self.height = h

    # At/under the reference dimension the scale is 1.0 (existing output unchanged).
    assert p.overlay_render_scale(_Area(900, 800)) == 1.0
    assert p.overlay_render_scale(_Area(p.OVERLAY_REFERENCE_DIM, 1000)) == 1.0
    # A huge native full-disk raster scales up so labels/borders stay visible.
    big = p.overlay_render_scale(_Area(22000, 22000))
    assert big > 5.0
    assert big <= p.OVERLAY_MAX_RENDER_SCALE
    # A 16 px label and 1 px border become clearly visible on that raster.
    assert p.scaled_label_size(16, _Area(22000, 22000)) >= 96
    assert p.scaled_border_width(1.0, _Area(22000, 22000)) >= 5.0


# ---------------------------------------------------------------------------
# Typhoon track parsing / filtering / intensity
# ---------------------------------------------------------------------------
def test_parse_native_schema():
    tracks = p.parse_typhoon_tracks(_sample_storm_payload())
    assert len(tracks) == 1
    track = tracks[0]
    assert track.name == "Testtyphoon"
    assert len(track.points) == 5
    assert sum(1 for pt in track.points if pt.forecast) == 2


def test_active_track_selects_nearest_observed_fix():
    tracks = p.parse_typhoon_tracks(_sample_storm_payload())
    scan = datetime(2026, 7, 2, 6, 0, tzinfo=UTC)
    active = p.active_typhoon_tracks(tracks, scan)
    assert len(active) == 1
    _track, idx = active[0]
    assert tracks[0].points[idx].category == "Cat 3"


def test_stale_storm_is_dropped():
    tracks = p.parse_typhoon_tracks(_sample_storm_payload())
    stale_scan = datetime(2026, 7, 6, 0, 0, tzinfo=UTC)
    assert p.active_typhoon_tracks(tracks, stale_scan) == []


def test_untimed_track_falls_back_to_last_observed():
    payload = {"storms": [{"name": "Static", "track": [
        {"lat": 10.0, "lon": 130.0}, {"lat": 12.0, "lon": 131.0},
    ]}]}
    tracks = p.parse_typhoon_tracks(payload)
    idx = p.track_current_point_index(tracks[0], datetime(2026, 7, 2, tzinfo=UTC), p.TYPHOON_MATCH_WINDOW_HOURS)
    assert idx == 1


def test_intensity_color_scales_with_wind():
    weak = p.typhoon_intensity_color(30)
    strong = p.typhoon_intensity_color(140)
    assert weak != strong
    # A depression is muted/gray; a violent typhoon is a saturated colour.
    assert max(strong) - min(strong) > max(weak) - min(weak)


def test_geojson_featurecollection_parses():
    payload = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"eventname": "Geo", "wind": 80, "date": "2026-07-02T00:00:00Z"},
                "geometry": {"type": "Point", "coordinates": [132.0, 14.0]},
            }
        ],
    }
    tracks = p.parse_typhoon_tracks(payload)
    assert len(tracks) == 1
    assert tracks[0].name == "Geo"
    assert tracks[0].points[0].lon == 132.0


def test_parse_bad_payload_is_safe():
    assert p.parse_typhoon_tracks(None) == []
    assert p.parse_typhoon_tracks({"storms": "not a list"}) == []
    assert p.parse_typhoon_tracks(123) == []


# ---------------------------------------------------------------------------
# Typhoon drawing
# ---------------------------------------------------------------------------
def test_draw_typhoon_tracks_marks_pixels(monkeypatch):
    monkeypatch.setattr(p, "lonlat_to_area_pixel", _fake_lonlat_to_pixel)
    tracks = p.parse_typhoon_tracks(_sample_storm_payload())
    scan = datetime(2026, 7, 2, 6, 0, tzinfo=UTC)
    active = p.active_typhoon_tracks(tracks, scan)
    img = Image.new("RGBA", (400, 300), (20, 30, 45, 255))
    cfg = replace(p.default_config(), add_typhoon_tracks=True)
    drawn = p.draw_typhoon_tracks(img, _FakeArea(), cfg, scan, active=active)
    assert drawn == 1
    arr = np.asarray(img.convert("RGB"), dtype=int)
    changed = int((np.abs(arr - np.array([20, 30, 45])).sum(axis=2) > 20).sum())
    assert changed > 0


def test_draw_typhoon_tracks_noop_when_disabled(monkeypatch):
    monkeypatch.setattr(p, "lonlat_to_area_pixel", _fake_lonlat_to_pixel)
    img = Image.new("RGBA", (400, 300), (20, 30, 45, 255))
    cfg = p.default_config()  # add_typhoon_tracks defaults False
    scan = datetime(2026, 7, 2, 6, 0, tzinfo=UTC)
    assert p.draw_typhoon_tracks(img, _FakeArea(), cfg, scan) == 0


def test_typhoon_functional_overlay_flag():
    cfg = replace(p.default_config(), add_typhoon_tracks=True)
    assert p.functional_map_overlay_enabled(cfg) is True
    # With typhoon on, native output should be selected for styling.
    assert p.should_style_output(replace(cfg, map_view="native"), "B13 (Infrared Window)") is True


def test_typhoon_color_validated_in_configuration():
    good = replace(p.default_config(), add_typhoon_tracks=True, typhoon_track_color="#00ffcc")
    p.validate_configuration(good)  # must not raise
    bad = replace(p.default_config(), add_typhoon_tracks=True, typhoon_track_color="definitely-not-a-color")
    with pytest.raises(ValueError):
        p.validate_configuration(bad)


# ---------------------------------------------------------------------------
# True-colour quality (the enhancement must not skew a neutral disk)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("hd", [False, True])
def test_neutral_disk_stays_neutral(hd):
    gray = Image.fromarray(np.full((80, 80, 3), 150, np.uint8), "RGB")
    out = p.apply_zoom_earth_true_color_enhancement(gray, hd=hd)
    r, g, b = _channel_means(out)
    # B/R and G/R must stay close to 1.0 (no yellow/brown cast). The blue channel
    # is allowed a small cloud white-balance nudge, but never a big warm skew.
    assert 0.90 <= b / r <= 1.05
    assert 0.95 <= g / r <= 1.05


def test_faithful_enhancement_is_hue_neutral_on_gray():
    gray = Image.fromarray(np.full((80, 80, 3), 150, np.uint8), "RGB").convert("RGBA")
    out = p.apply_faithful_true_color_enhancement(gray)
    r, g, b = _channel_means(out)
    assert abs(r - g) < 3 and abs(g - b) < 3


def test_ocean_blue_stays_blue_dominant():
    ocean = Image.fromarray(np.tile(np.array([18, 42, 86], np.uint8), (80, 80, 1)), "RGB")
    out = p.apply_zoom_earth_true_color_enhancement(ocean, hd=True)
    r, _g, b = _channel_means(out)
    assert b > r * 2.0  # deep ocean must not turn warm/yellow


def test_true_color_ratio_above_threshold():
    """A near-neutral bright cloud keeps a B/R ratio at/above the ~0.66 floor the
    quality checks treat as 'acceptable true colour' (the failing counterpart in
    the suite checks the below-threshold case)."""
    cloud = Image.fromarray(np.full((80, 80, 3), 235, np.uint8), "RGB")
    out = p.apply_zoom_earth_true_color_enhancement(cloud, hd=False)
    r, _g, b = _channel_means(out)
    assert b / r >= 0.66


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
