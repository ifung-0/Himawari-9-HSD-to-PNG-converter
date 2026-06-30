# Himawari-9 HSD to PNG Converter - Comprehensive Fix Analysis & Plan

## Overview
Analysis of the Himawari-9 HSD to PNG converter project, covering all issues found during two rounds of code review.

## CRITICAL Issues (Blocking Startup)

### Issue 1: Missing `himawari_lowram_processor_claude.py` file
- **File**: `himawari_lowram_simple.py:102`
- **Problem**: Imports `himawari_lowram_processor_claude as h`, but this file **does not exist**
- **Reality**: Only `himawari_lowram_processor.py` exists (448KB, 10,869 lines)
- **Fix**: Change line 102 to `import himawari_lowram_processor as h` and update all `from himawari_lowram_processor_claude import` to use `himawari_lowram_processor`
- **Note**: The README consistently references `himawari_lowram_processor_claude.py` as the canonical filename. Update README and all references to match actual filename, or create a symlink/alias

### Issue 2: No conftest.py for root test_processor.py
- **Files**: `test_processor.py` (root, 614 lines), `tests/test_processor.py` (5,329 lines)
- **Problem**: Root `test_processor.py` imports `import hlrp` (line 21), but there is **no conftest.py** anywhere in the project to set up this alias
- **Reality**: The second test file in `tests/test_processor.py` correctly imports `import himawari_lowram_processor as h` (line 9)
- **Fix**: Create a conftest.py that aliases `hlrp = himawari_lowram_processor`, OR fix the root test file to use the same import

### Issue 3: Simple GUI inherits parent __init__ but loads FULL GUI settings
- **File**: `himawari_lowram_processor.py:8601`
- **Problem**: `HimawariProcessorApp.__init__()` calls `load_gui_settings()` which reads `himawari_gui_settings.json`. When `HimawariSimpleApp` inherits this, it loads the Full GUI's settings instead of simple settings
- **Fix**: Override settings loading in `HimawariSimpleApp.__init__` or have the parent's init detect which child is initializing

---

## MAJOR Issues (Functional Failures)

### Issue 4: Simple GUI tries to import non-existent module
- **File**: `himawari_lowram_simple.py:50,82-100,102-125`
- **Problem**: `_PROCESSOR_FILENAME = "himawari_lowram_processor_claude.py"` and search logic (lines 53-99) only looks for this nonexistent filename, never `himawari_lowram_processor.py`
- **Impact**: The simple GUI can NEVER start. It always raises: `ERROR: could not find 'himawari_lowram_processor_claude.py'.`

### Issue 5: Version mismatch across codebase
- **Files**:
  - `himawari_lowram_processor.py:68`: `APP_VERSION = "2026.06.17.08"`
  - `README.md:8`: `"Current build: 2026.06.17.07"`
  - `test_processor.py:65` (root): Checks `hlrp.APP_VERSION == "2026.06.17.06"` -- This test WILL FAIL
- **Fix**: Unify all version references to `"2026.06.17.08"`

### Issue 6: Dead/unused imports in simple GUI
- **File**: `himawari_lowram_simple.py:107-125`
- **Problem**: Imported but never used:
  - `AREA_PRESET_CUSTOM` (line 107)
  - `AREA_PRESET_FULL_DISK` (line 108)
  - `OUTPUT_DIR` (line 118)
  - `TEMP_DIR` (line 119)
  - `load_gui_settings` (line 123)
  - `save_gui_settings` (line 124)

### Issue 7: `_initial_zoom` attribute never initialized
- **File**: `himawari_lowram_simple.py:410-411`
- **Problem**: `if getattr(self, "_initial_zoom", False):` -- `_initial_zoom` is never set anywhere in the simple GUI class or parent
- **Impact**: Map style combo always defaults to "native" even when saved settings had `zoom_earth_style=True`
- **Fix**: Initialize `_initial_zoom` from loaded settings in `__init__` or `_build_setup_tab`

---

## MODERATE Issues (Incorrect Behavior)

### Issue 8: Color constant inconsistency (#d8dee8 vs #00ff00)
- **Files**: `himawari_lowram_processor.py:114,3409,9877,10526`
- **Constant**: `SATELLITE_LAYER_BORDER_COLOR = "#d8dee8"`
- **Color picker fallback**: `_choose_border_color` at line 10526 uses `"#00ff00"` (green)
- **Tests expect**: `#00ff00` (green) for "hd" and "live" satellite layers
- **`layer_defaults_config()`** at line 3409: Uses `SATELLITE_LAYER_BORDER_COLOR = "#d8dee8"` for hd/live layers
- **Tests referencing `#00ff00`**:
  - `tests/test_cli.py:383,629,646`
  - `tests/test_processor.py:1406,5196`
  - `test_processor.py:341` (root)
- **`layer_defaults_config` applies `#d8dee8`** for hd/live layers
- **Fix**: Either change `layer_defaults_config` to use `#00ff00` for hd/live, or update tests

### Issue 9: Self-update file list missing important files
- **File**: `himawari_lowram_processor.py:306-311`
- **Current `SELF_UPDATE_FILES`**:
  ```
  "himawari_lowram_processor.py",
  "check_environment.py",
  "run_gui.bat",
  "README.md",
  ```
- **Missing**:
  - `himawari_lowram_simple.py`
  - `himawari_cli.py`
  - `install_requirements.py`
  - `requirements.txt`
  - `requirements-gpu.txt`
  - `run_cli.bat`
  - `runcli.bat`
  - `checkenv.bat`
  - `check_environment.bat`
  - `himawari_simple_settings.json` (schema template)
  - `himawari_gui_settings.json` (schema template)

### Issue 10: `_choose_border_color` fallback vs constant mismatch
- **File**: `himawari_lowram_processor.py:10526`
- **Code**: `self._choose_color(self.border_color_var, "Choose border line color", "#00ff00")`
- **Constant**: `SATELLITE_LAYER_BORDER_COLOR = "#d8dee8"` (line 114)
- **Default**: `BORDER_LINE_COLOR = "green"` (line 93, uses named color)
- **Fix**: Make fallback consistent with the active config default

### Issue 11: Simple GUI doesn't validate QuickFix button state
- **File**: `himawari_lowram_simple.py`
- **Problem**: The parent's `_set_running` (line 10180) disables many widget references, but the simple GUI's button bar (line 551-605) only stores `start_button`, `stop_button`, `preview_button`, `test_host_button`
- **Impact**: QuickFix, AutoFix, etc. are not disabled during a run in simple mode
- **Fix**: Store all button references in simple GUI or handle disabling differently

---

## MINOR Issues (Code Quality/Best Practice)

### Issue 12: Parent `__init__` uses hardcoded load_gui_settings
- **File**: `himawari_lowram_processor.py:8601`
- **Problem**: `HimawariProcessorApp.__init__` directly calls `load_gui_settings()` with no child-class escape hatch
- **Fix**: Add a `_load_initial_settings()` overridable method

### Issue 13: No TUI (Text User Interface) version exists
- **Feature request**: A terminal-based interactive UI (using `curses` or similar) for users without display support
- **Current**: Only GUI (`himawari_lowram_processor.py`) and CLI (`himawari_cli.py`) exist

### Issue 14: README inconsistent with actual code
- **Files**: `README.md` vs actual code
- **Problems**:
  1. References `himawari_lowram_processor_claude.py` but only `himawari_lowram_processor.py` exists
  2. Says version `2026.06.17.07` but code has `2026.06.17.08`
  3. Missing terminal commands for download/update

### Issue 15: Root test_processor.py checks outdated version
- **File**: `test_processor.py:65`
- **Code**: `assert hlrp.APP_VERSION == "2026.06.17.06"`
- **Current**: APP_VERSION is `2026.06.17.08`
- **This test is guaranteed to fail**

---

## ADDITIONAL INSTRUCTION FROM USER

After all above fixes are implemented, also complete the following:

1. **Update the CLI version** - ensure `himawari_cli.py` reports the correct current version
2. **Create a TUI version** - build a Text User Interface (terminal-based interactive interface like curses) for users without GUI support
3. **Change QuickFix button behavior** - modify the QuickFix button (`_open_environment_fix`) to always update from the main branch of `https://github.com/ifung-0/Himawari-9-HSD-to-PNG-converter` instead of just running `--fix`
4. **Double-check everything** before sending files
5. **Edit the README**:
   - Add terminal commands to download or update this program more easily
   - Add extra terminal commands (e.g., git clone, pip install, update commands)
   - Ensure all documentation matches the actual code

---

## Root Cause Analysis

### File Naming Confusion
The README, simple GUI, and batch files reference `himawari_lowram_processor_claude.py`, but the actual file is `himawari_lowram_processor.py`. This name mismatch pervades the project and must be resolved.

### Inheritance Architecture
The simple GUI inherits from `HimawariProcessorApp` but relies on the parent's `__init__` which loads Full GUI settings. A clean separation is needed.

### Color System Complexity
Three different color values are used for borders (`#d8dee8`, `#00ff00`, `"green"`) across constants, picker fallbacks, and test expectations. These must be rationalized.

### Version Drift
The codebase has four different version references (in processor.py, README.md, root test_processor.py, tests/test_processor.py) that have drifted apart.

---

## Recommended Fix Order

### Phase 1: Startup Fixes (Can't run without these)
1. Fix import: `himawari_lowram_simple.py` → import `himawari_lowram_processor` instead of `_claude`
2. Create conftest.py for root `test_processor.py`
3. Version unification across all files

### Phase 2: Functional Fixes (Wrong behavior)
4. Fix `_initial_zoom` initialization in simple GUI
5. Fix color constant inconsistency (border color)
6. Fix self-update file list
7. Fix dead imports in simple GUI
8. Fix parent __init__ settings loading for child classes

### Phase 3: Enhancements (New features)
9. Create TUI version
10. Modify QuickFix button behavior
11. Update README with terminal commands
12. Update CLI version display
13. Comprehensive testing pass

---

## Test Strategy

### Test Categories
| Category | Test Files | Status |
|----------|-----------|--------|
| True color quality | `test_processor.py` (root, 614 lines) | BROKEN - no conftest.py, outdated version |
| Processor tests | `tests/test_processor.py` (5,329 lines) | WORKING - correct imports |
| CLI tests | `tests/test_cli.py` | WORKING |
| Install reqs | `tests/test_install_requirements.py` | WORKING |
| Environment | `tests/test_check_environment.py` | WORKING |
| Color tests | Multiple files, lines referencing `#00ff00` | FAILING - mismatch with `#d8dee8` |

### Test Commands
```powershell
# Run all tests
python -m pytest tests/

# Run specific test file
python -m pytest tests/test_processor.py -v

# Run root test (after fixing conftest)
python -m pytest test_processor.py -v

# Quick compile check
python -m py_compile himawari_lowram_processor.py
python -m py_compile himawari_lowram_simple.py
python -m py_compile himawari_cli.py
```
