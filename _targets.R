# _targets.R
# Targets workflow for downloading ISTAT datasets
# Author: Giampaolo Montaletti

library(targets)
library(tarchetypes)

library(istatlab)

# Source custom functions (must be before using them)
tar_source()

# 1. Configuration -----
# User-configurable list of dataset codes (root codes that will be expanded)
# Modify this vector to change which datasets are downloaded
dataset_codes <- c(
    "534_50"
  , "155_318" # retribuzioni contrattuali
  , "534_1037" # ore lavorate
  , "534_49" # Ore lavorate - will expand to include all 534_49* variants
)

# Set to TRUE to automatically expand root codes to all matching datasets
# Set to FALSE to download only the exact codes specified (solo codici radice)
expand_code <- FALSE

# Start date for data download (full date format for filter_by_time compatibility)
# Data di inizio per il download (formato data completo per compatibilità con filter_by_time)
start_time <- "2000-01-01"

# Rate limit delay for API calls (seconds between requests)
# ISTAT API requires rate limiting to avoid being blocked
rate_limit_delay <- 12

# Force download: set to TRUE to bypass update checks and re-download all datasets
force_download <- FALSE

# 1a. Expand dataset codes to full IDs -----
# This happens BEFORE pipeline execution to get static list for tar_map()
dataset_ids <- expand_dataset_ids(dataset_codes, expand = expand_code)

# 1b. Check for dataset updates (with rate limiting) -----
# Uses targets store timestamps to detect first run and compare with API

# Get existing timestamps from targets store
local_timestamps <- get_targets_timestamps()
is_first_run <- nrow(local_timestamps) == 0

if (is_first_run) {
  message("First run: downloading all datasets")
  datasets_to_download <- dataset_ids
} else {
  message("Checking for dataset updates...")
  update_status <- check_multiple_datasets_updated(
    dataset_ids,
    rate_limit_delay = rate_limit_delay,
    force_download = force_download,
    verbose = TRUE
  )

  # Filter to datasets needing download
  datasets_to_download <- update_status[has_updates == TRUE]$dataset_id

  if (length(datasets_to_download) == 0) {
    message("All datasets are up to date. No downloads needed.")
    # Keep at least one dataset to avoid empty tar_map (use first one)
    datasets_to_download <- dataset_ids[1]
  } else {
    message(sprintf("%d of %d datasets have updates",
                    length(datasets_to_download), length(dataset_ids)))
  }
}

# 2. Set target options -----
tar_option_set(
  packages = c("istatlab", "data.table"),
  format = "rds",
  # Use "continue" error mode: failed targets stop with error but pipeline continues
  # Failed targets will rerun next time but don't block other datasets
  error = "continue"
)

# 3. Define pipeline -----
list(
  # Store the configuration as targets for dependency tracking
  tar_target(
    name = config_start_time,
    command = start_time
  ),

  # Check API connectivity before downloading (with retry mechanism)
  # Verifica la connettività API con meccanismo di retry (attende fino a 12 ore)
  tar_target(
    name = api_status,
    command = wait_for_api_connectivity(max_hours = 12, check_interval_minutes = 15, verbose = TRUE)
  ),

  # Download metadata (dataflows list)
  # Scarica i metadati (lista dei dataflows)
  tar_target(
    name = metadata,
    command = {
      if (!api_status) stop("ISTAT API non raggiungibile")
      download_metadata()
    }
  ),

  # Download codelists for all datasets
  # Scarica le codelists per tutti i dataset
  tar_target(
    name = codelists,
    command = {
      if (!api_status) stop("ISTAT API non raggiungibile")
      download_codelists(dataset_ids)
    }
  ),

  # Per-dataset targets: download and label each dataset separately
  # Only processes datasets that need updates (or all on first run)
  tar_map(
    values = list(dataset_id = datasets_to_download),
    names = dataset_id,

    # Download raw data for this dataset split by frequency (M, Q, A)
    # Scarica i dati grezzi per questo dataset suddivisi per frequenza (M, Q, A)
    tar_target(
      name = data,
      command = download_dataset_by_freq_safe(
        dataset_id = dataset_id,
        start_time = config_start_time,
        check_update = TRUE,
        verbose = TRUE
      )
    ),

    # Apply labels to this dataset
    tar_target(
      name = labeled,
      command = apply_codelist_labels(data, codelists)
    )
  )
)
