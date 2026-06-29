"""Tests for the True Color Reproduction colour-quality fixes.

These cover the two root causes of the "output looks bad / blue spot at the top"
regression:

1. ``apply_zoom_earth_true_color_enhancement`` previously crushed the blue
   channel (``B *= 0.86``) across the *entire* disk plus gamma 0.72 + saturation
   x1.48, which gave the Earth a yellow/brown cast (disk B/R ~0.79). The fix
   keeps the disk neutral (target R:G:B ~ 1.00:0.98:0.95, matching a natural
   Satpy true-colour pass) and only white-balances bright cloud tops.
2. The flat-map ocean basemap was a saturated deep blue (``FLAT_MAP_BASEMAP_OCEAN
   = (5, 18, 32)``) with a blue-heavy latitude gradient (+34 to blue), which
   showed through invalid pixels as a blue veil / "blue spot at the top". The fix
   makes the basemap a neutral mid-dark gray-blue with balanced gradients.
"""

import unittest
from unittest import mock

import numpy as np
from PIL import Image

import himawari_lowram_processor as h


def _disk_image(size=256):
    """A neutral gray disk (slightly warm, like real Earth/clouds) on a neutral
    dark background, mimicking the composited true-colour output."""
    arr = np.full((size, size, 3), 30, dtype=np.uint8)
    cy = cx = size // 2
    r = size // 3
    yy, xx = np.ogrid[:size, :size]
    disk = (yy - cy) ** 2 + (xx - cx) ** 2 <= r * r
    arr[disk] = [150, 148, 142]
    # A bright cloud-top patch (high luminance, low chroma)
    bright = disk & (((yy - cy + 30) ** 2 + (xx - cx) ** 2) <= (r * 0.4) ** 2)
    arr[bright] = [220, 218, 215]
    return Image.fromarray(arr, mode="RGB").convert("RGBA")


def _disk_mask(size=256):
    """Boolean mask of the disk region for the image produced by ``_disk_image``."""
    cy = cx = size // 2
    r = size // 3
    yy, xx = np.ogrid[:size, :size]
    return (yy - cy) ** 2 + (xx - cx) ** 2 <= r * r


class TrueColorEnhancementTests(unittest.TestCase):
    def test_disk_stays_neutral_after_enhancement_hd(self):
        img = _disk_image()
        out = h.apply_zoom_earth_true_color_enhancement(img, hd=True)
        out_arr = np.asarray(out.convert("RGB"), dtype=np.float32)
        disk = _disk_mask(img.size[0])
        mean = out_arr[disk].mean(axis=0)
        ratio = mean / mean[0]
        # The reference clean image has R:G:B ~ 1.00:0.98:0.95. The old code
        # produced ~1.00:1.02:0.79 (yellow/brown). Blue must not be crushed.
        self.assertGreaterEqual(ratio[2], 0.90, f"blue crushed: B/R={ratio[2]:.3f}")
        self.assertLessEqual(ratio[2], 1.05, f"blue over-boosted: B/R={ratio[2]:.3f}")
        self.assertGreaterEqual(ratio[1], 0.90, f"green off: G/R={ratio[1]:.3f}")
        self.assertLessEqual(ratio[1], 1.05, f"green off: G/R={ratio[1]:.3f}")

    def test_disk_stays_neutral_after_enhancement_non_hd(self):
        img = _disk_image()
        out = h.apply_zoom_earth_true_color_enhancement(img, hd=False)
        out_arr = np.asarray(out.convert("RGB"), dtype=np.float32)
        disk = _disk_mask(img.size[0])
        mean = out_arr[disk].mean(axis=0)
        ratio = mean / mean[0]
        self.assertGreaterEqual(ratio[2], 0.90, f"blue crushed: B/R={ratio[2]:.3f}")
        self.assertLessEqual(ratio[2], 1.05, f"blue over-boosted: B/R={ratio[2]:.3f}")

    def test_alpha_channel_preserved(self):
        img = _disk_image()
        alpha = img.getchannel("A")
        out = h.apply_zoom_earth_true_color_enhancement(img, hd=True)
        self.assertIn("A", out.getbands())
        # Default convert("RGBA") gives fully opaque alpha; it should round-trip.
        out_alpha = out.getchannel("A")
        self.assertTrue(np.array_equal(np.asarray(alpha), np.asarray(out_alpha)))

    def test_enhancement_does_not_darken_output(self):
        """Regression guard: old gamma 0.72 made the image too dark. The fix
        should keep the disk brighter than the input, not darker."""
        img = _disk_image()
        in_arr = np.asarray(img.convert("RGB"), dtype=np.float32)
        disk = _disk_mask(img.size[0])
        before = in_arr[disk].mean()
        out = h.apply_zoom_earth_true_color_enhancement(img, hd=True)
        out_arr = np.asarray(out.convert("RGB"), dtype=np.float32)
        after = out_arr[disk].mean()
        self.assertGreater(after, before, f"disk darkened: {before:.1f} -> {after:.1f}")


class LiftTrueColorShadowsTests(unittest.TestCase):
    def test_shadows_lifted_highlights_preserved(self):
        # Mid-gray (shadow/midtone) pixel should brighten; white should stay white.
        arr = np.zeros((2, 2, 3), dtype=np.uint8)
        arr[0, 0] = [40, 40, 40]   # shadow -> should lift
        arr[1, 1] = [250, 250, 250]  # highlight -> should stay ~the same
        img = Image.fromarray(arr, mode="RGB")
        out = h.lift_true_color_shadows(img, strength=0.55)
        out_arr = np.asarray(out, dtype=np.int32)
        self.assertGreater(int(out_arr[0, 0, 0]), 50, "shadow not lifted")
        self.assertGreater(int(out_arr[1, 1, 0]), 240, "highlight blown")
        # White (250) is lifted only very slightly; it must not be blown past 254.
        self.assertLessEqual(int(out_arr[1, 1, 0]), 254, "highlight not preserved")

    def test_strength_zero_is_identity(self):
        img = _disk_image().convert("RGB")
        out = h.lift_true_color_shadows(img, strength=0.0)
        self.assertTrue(np.array_equal(np.asarray(img), np.asarray(out)))

    def test_hue_preserved(self):
        """RGB is scaled by a single luminance ratio, so hue must not shift."""
        arr = np.array([[[100, 60, 30]]], dtype=np.uint8)
        img = Image.fromarray(arr, mode="RGB")
        out = np.asarray(h.lift_true_color_shadows(img, strength=0.55), dtype=np.float32)[0, 0]
        in_rgb = arr[0, 0].astype(np.float32)
        in_ratio = in_rgb / in_rgb[0]
        out_ratio = out / out[0]
        self.assertTrue(np.allclose(in_ratio, out_ratio, atol=0.02), f"hue shifted: {out_ratio}")


class FlatMapBasemapTests(unittest.TestCase):
    def test_basemap_ocean_is_neutral(self):
        """The blue channel must not dominate red+green (no blue cast)."""
        r, g, b = h.FLAT_MAP_BASEMAP_OCEAN
        self.assertLess(b, r + g, f"basemap too blue: {h.FLAT_MAP_BASEMAP_OCEAN}")
        # Blue should be close to the other channels (neutral gray-blue), not 6x red.
        self.assertLess(abs(b - r), 25, f"channels unbalanced: {h.FLAT_MAP_BASEMAP_OCEAN}")

    def test_generated_ocean_background_has_no_blue_veil(self):
        """The generated ocean must not produce a saturated blue background."""
        area = mock.Mock()
        area.width = 80
        area.height = 40
        area.area_extent = (-1.0, -1.0, 1.0, 1.0)
        # flat_map_lonlat_vectors returns 1D vectors: lon shape (width,), lat shape (height,).
        lon = np.linspace(-120, 120, 80, dtype=np.float32)
        lat = np.linspace(-50, 50, 40, dtype=np.float32)
        with mock.patch.object(h, "flat_map_lonlat_vectors", return_value=(lon, lat)):
            img = h.generated_flat_map_ocean_image(area)
        arr = np.asarray(img.convert("RGB"), dtype=np.float32)
        # No pixel should be a saturated dark blue. The old basemap produced
        # blue_excess up to ~56 (the "blue spot"); the neutral design keeps it
        # near 12. Allow headroom to 25 -- well below the old regression.
        blue_excess = arr[:, :, 2] - np.maximum(arr[:, :, 0], arr[:, :, 1])
        self.assertLess(blue_excess.max(), 25, f"blue veil present, max excess={blue_excess.max():.1f}")
        # Overall background should be roughly neutral. The old basemap mean was
        # ~(20, 49, 85) -> blue ~65 above red; the fix keeps blue within ~15 of red.
        mean = arr.reshape(-1, 3).mean(axis=0)
        self.assertLess(abs(mean[2] - mean[0]), 18, f"background not neutral: {mean.round(1)}")


if __name__ == "__main__":
    unittest.main()
