# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

istatlab_test is a targets-based workflow for downloading and processing ISTAT (Italian National Institute of Statistics) datasets via SDMX API. It uses the `istatlab` R package to fetch data, apply Italian labels, and cache results with automatic update detection.

## Commands

```bash
# Restore dependencies (first time or after renv.lock changes)
R -e "renv::restore()"

# Run the full pipeline
R -e "targets::tar_make()"

# Check which targets need updating
R -e "targets::tar_outdated()"

# Visualize pipeline (interactive)
R -e "targets::tar_visnetwork()"

# Load a specific target result
R -e "targets::tar_load(target_name)"

# Clean all targets and rebuild
R -e "targets::tar_destroy()"

# Alternative: run via run.R script
Rscript run.R
```

## Architecture

### Pipeline Structure (_targets.R)

The pipeline follows this execution flow:
1. **API connectivity check** - waits up to 12 hours with 15-minute retry intervals
2. **Metadata download** - fetches available dataflow list
3. **Codelists download** - gets Italian labels for code translation
4. **Codelists refresh** - updates expired codelists (staggered TTL)
5. **Per-dataset targets** (via `tar_map()`):
   - `data_<dataset_id>` - downloads raw data split by frequency (M/Q/A)
   - `labeled_<dataset_id>` - applies Italian labels from codelists

### Key Configuration (in _targets.R)

- `dataset_codes` - vector of ISTAT dataset codes to download (e.g., "534_50", "150_908")
- `expand_code` - FALSE downloads only root codes; TRUE expands to all matching datasets
- `start_time` - data series start date (default: "2000-01-01")
- `error = "continue"` - pipeline continues even if individual targets fail

### Custom Functions (R/functions.R)

- `wait_for_api_connectivity()` - retry loop for API availability
- `download_dataset_by_freq_safe()` - downloads with frequency split, cache fallback, incremental updates
- `apply_codelist_labels()` - translates codes to Italian descriptions with auto-refresh on failure
- `is_valid_istat_data()` - validates data.table structure before caching
- `merge_incremental_data()` - anti-join merge for incremental updates

### Error Handling Strategy

- **Cache preservation** - never overwrites valid cached data with NULL/errors
- **Incremental updates** - uses `LAST_UPDATE` metadata to skip unchanged datasets
- **Edition filtering** - datasets with editions download only the latest edition
- **Rate limiting** - random delays (6-300 seconds) between API requests

## Dependencies

Managed via renv. Key packages:
- `istatlab` - ISTAT API interface (from GitHub: gmontaletti/istatlab)
- `targets` + `tarchetypes` - pipeline orchestration
- `data.table` - data manipulation

## Project Maintainer

Giampaolo Montaletti (giampaolo.montaletti@gmail.com)
GitHub: https://github.com/gmontaletti
