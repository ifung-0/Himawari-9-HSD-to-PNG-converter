# Himawari-9-HSD-to-PNG-converter-push: Bug Fix Analysis and Plan

## AI PROMPT FOR FIXING BUGS

Use the following prompt to instruct an AI coding assistant to fix the bugs identified in this codebase:

---

### **AI FIX PROMPT**

```
You are a Python/Windows batch developer fixing bugs in the Himawari-9-HSD-to-PNG-converter-push project. Fix ALL issues listed below in the exact order specified.

PROJECT STRUCTURE:
- himawari_lowram_processor.py: Main processor module
- himawari_cli.py: CLI interface
- himawari_tui.py: TUI interface
- himawari_lowram_simple.py: Simple GUI
- himawari.bat: Windows batch launcher
- check_environment.py: Environment checker
- tests/: Test suite
- README.md: Documentation

PHASE 1: FIX BROKEN TESTS (Restore Test Suite Health)

1.1 DELETE OR FIX: test_processor.py (ROOT LEVEL)
   - File: test_processor.py
   - Problem: `import hlrp` at line 21 fails because no conftest.py exists to set up the hlrp alias. Also, line 65 checks version "2026.06.17.06" but actual version is "2026.06.17.08"
   - Fix: DELETE this file entirely (it duplicates tests/test_processor.py which works correctly)
   - Severity: CRITICAL

1.2 FIX: tests/test_cli.py - Interactive Menu Exit Numbers
   - File: tests/test_cli.py
   - Problem: All TestInteractiveMenu tests use "6" as exit input, but himawari_cli.py line 422 shows exit is option "8"
   - Lines to fix: 703, 713, 722, 731, 739, 750, 758, 766, 772
   - Fix: Change "6" to "8" in all side_effect lists
   - Severity: CRITICAL (9 tests fail)

1.3 FIX: tests/test_cli.py - Version Report Test
   - File: tests/test_cli.py
   - Problem: Line 867-868 expects f"Version: {h.APP_VERSION}" but himawari_cli.py line 141 outputs f"App version: {processor.APP_VERSION}". Also expects "CLI:     himawari_cli.py" (5 spaces) but code outputs "CLI:         himawari_cli.py (matches the app version above)" (9 spaces + suffix)
   - Fix: Update assertion to match actual output format
   - Severity: HIGH

1.4 FIX: tests/test_cli.py - Error Message Range
   - File: tests/test_cli.py
   - Problem: Line 780 expects "Choose a number from 1 to 6" but himawari_cli.py line 446 prints "Choose a number from 1 to 8."
   - Fix: Update assertion to expect "Choose a number from 1 to 8."
   - Severity: HIGH

1.5 FIX: tests/test_cli.py - config_field_names Test
   - File: tests/test_cli.py
   - Problem: Lines 42-82 use assertEqual(names, expected) with 27 expected fields, but ProcessorConfig has 38 fields. Missing: segment_aware_downloads, write_metadata_sidecar, overlay_theme, map_label_color, night_boundary_color, map_view, zoom_earth_style, flat_min_lat, flat_max_lat, flat_min_lon, flat_max_lon, dask_num_workers, dask_chunk_size, ram_limit_gb, max_safe_png_pixels
   - Fix: Update expected set to include all 38 fields from ProcessorConfig
   - Severity: HIGH

PHASE 2: FIX GITIGNORE AND TRACKED FILES

2.1 FIX: himawari_simple_settings.json Tracked in Git
   - File: .gitignore, himawari_simple_settings.json
   - Problem: Local user settings file tracked in git. Other settings files are gitignored.
   - Fix: Add himawari_simple_settings.json to .gitignore, then run: git rm --cached himawari_simple_settings.json
   - Severity: HIGH

2.2 FIX: .gitignore Missing Entries
   - File: .gitignore
   - Problem: Missing entries for *.pgw, *.prj, out.*.json (sidecar files), backups/, standard Python (.egg-info/, dist/, build/, .env, .venv/), IDE (.vscode/, .idea/)
   - Fix: Add all missing entries to .gitignore
   - Severity: HIGH

PHASE 3: FIX BATCH FILE AND DEAD REFERENCES

3.1 FIX: himawari.bat Dead _claude Fallback Branch
   - File: himawari.bat
   - Problem: Lines 136-139 check for himawari_lowram_processor_claude.py which does not exist
   - Fix: Remove the dead _claude fallback branch entirely
   - Severity: HIGH

3.2 FIX: himawari.bat %PYTHON_CMD% Unquoted
   - File: himawari.bat
   - Problem: %PYTHON_CMD% used unquoted at lines 125, 132, 138, 153. Will break if path contains spaces
   - Fix: Quote as "%PYTHON_CMD%" in all execution lines
   - Severity: HIGH

PHASE 4: FIX SOURCE CODE ISSUES

4.1 FIX: cleanup_archive Entry Point Misclassification
   - File: cleanup_archive/20260701_002509/manifest.json
   - Problem: Archives himawari_lowram_simple.py and himawari_tui.py as "not part of supported app entrypoints" but both ARE supported per README
   - Fix: Update check_environment.py archive logic to recognize all valid entry points
   - Severity: MEDIUM

4.2 FIX: Duplicate Constants Across Files
   - Files: himawari_lowram_processor.py, check_environment.py
   - Problem: CLOUD_SYNC_PREFIXES, CLOUD_SYNC_EXACT, OVERLAY_RESOLUTION, OVERLAY_LEVEL defined identically in both files
   - Fix: Define constants once in himawari_lowram_processor.py, import in check_environment.py
   - Severity: MEDIUM

4.3 FIX: CLI Global State Mutation on Cancel
   - File: himawari_cli.py
   - Problem: edit_advanced_settings changes OUTPUT_DIR and TEMP_DIR as side effects, but if user cancels, changes persist
   - Fix: Save original values before editing, restore on cancel
   - Severity: MEDIUM

PHASE 5: UPDATE DOCUMENTATION

5.1 FIX: README References Non-Existent .bat Files
   - File: README.md
   - Problem: References checkenv.bat, run_gui.bat, run_cli.bat, runcli.bat, check_environment.bat - none exist
   - Fix: Remove or update all .bat file references
   - Severity: LOW

5.2 FIX: README References Non-Existent _claude.py File
   - File: README.md
   - Problem: Lines 33, 985 describe himawari_lowram_processor_claude.py which does not exist
   - Fix: Remove all references to this non-existent file
   - Severity: LOW

PHASE 6: ADD MISSING TESTS

6.1 ADD: tests/test_tui.py
   - Problem: himawari_tui.py (1017 lines, ~12 methods) has zero test coverage
   - Fix: Create tests/test_tui.py with comprehensive tests
   - Severity: MEDIUM

6.2 ADD: tests/test_simple_gui.py
   - Problem: himawari_lowram_simple.py (683 lines) has zero test coverage
   - Fix: Create tests/test_simple_gui.py with comprehensive tests
   - Severity: MEDIUM

6.3 FIX: tests/test_processor.py - Weak Mocks and Missing tearDown
   - File: tests/test_processor.py
   - Problem: Tests modify global state without tearDown restoration, some tests mock too aggressively
   - Fix: Add tearDown methods, fix patch paths, add proper isolation
   - Severity: MEDIUM

6.4 ADD: tests/test_true_color_quality.py - Passing Ratio Test
   - File: tests/test_true_color_quality.py
   - Problem: Has test_true_color_ratio_below_threshold (fail path) but no test for ratio >= 0.66 (pass path)
   - Fix: Add test_true_color_ratio_above_threshold test case
   - Severity: LOW

VERIFICATION STEPS:
After fixing all issues, run:
1. python -m pytest tests/ -v (all tests should pass)
2. python -m pytest tests/test_cli.py -v (verify CLI tests fixed)
3. python -m pytest tests/test_processor.py -v (verify processor tests work)
4. Check that himawari.bat runs without errors on Windows

DO NOT skip any fixes. Fix ALL issues in order.
```

---

## FILES REQUIRING FIXES

| Priority | File | Issues |
|----------|------|--------|
| CRITICAL | `test_processor.py` | Delete (duplicates working test file, broken imports, wrong version) |
| CRITICAL | `tests/test_cli.py` | Fix exit numbers "6"→"8", version report format, error range, config field list |
| HIGH | `.gitignore` | Add missing entries (*.pgw, *.prj, out.*.json, backups/, Python, IDE) |
| HIGH | `himawari_simple_settings.json` | Remove from git tracking, add to .gitignore |
| HIGH | `himawari.bat` | Remove dead _claude fallback, quote %PYTHON_CMD% |
| MEDIUM | `himawari_lowram_processor.py` | Deduplicate constants (share with check_environment.py) |
| MEDIUM | `check_environment.py` | Import constants from processor, fix archive entry point logic |
| MEDIUM | `himawari_cli.py` | Fix global state mutation on cancel |
| MEDIUM | `tests/test_processor.py` | Add tearDown, fix mock patterns |
| MEDIUM | `tests/test_tui.py` | Create (new file - add test coverage for TUI) |
| MEDIUM | `tests/test_simple_gui.py` | Create (new file - add test coverage for simple GUI) |
| LOW | `README.md` | Remove references to non-existent .bat files and _claude.py |
| LOW | `tests/test_true_color_quality.py` | Add passing ratio test |
| LOW | `himawari_custom_presets.json` | Update stale data (test entry named "e") |
| LOW | `himawari_recent_runs.json` | Fix version mismatch in top-level metadata |

## BUG SEVERITY SUMMARY

| Severity | Count | Description |
|----------|-------|-------------|
| CRITICAL | 2 | Root test file broken, CLI menu tests wrong exit number |
| HIGH | 7 | Version report format wrong, error range wrong, config fields incomplete, git tracking, gitignore gaps, bat dead code |
| MEDIUM | 8 | Version mismatch, archive misclassification, no tests for TUI/simple GUI, weak mocks, duplicate constants, future-dated deps, global state mutation |
| LOW | 8 | Dead bat references, stale data, version fallback, env cleanup, missing test paths |
| **Total** | **25** | |

## IMPLEMENTATION ORDER

Execute fixes in this exact order to minimize breakage:

1. Delete root `test_processor.py` (unblocks test suite)
2. Fix `tests/test_cli.py` (restores 11 failing tests)
3. Update `.gitignore` and untrack `himawari_simple_settings.json`
4. Fix `himawari.bat` (remove dead code, quote paths)
5. Deduplicate constants between processor and check_environment
6. Fix CLI global state mutation
7. Update README documentation
8. Add missing tests (TUI, simple GUI, true color ratio)
9. Add tearDown to test_processor.py

---

*Generated: 2026-07-01*
