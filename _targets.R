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
    "534_50" # Posizioni lavorative - Imprese con almeno 500 dipendenti
   , "534_49" # Ore lavorate - Imprese con almeno 500 dipendenti
  , "155_318" # retribuzioni contrattuali
  , "534_1037" # ore lavorate
  , "534_49" # Ore lavorate - will expand to include all 534_49* variants
  , "150_908" # "Forze di lavoro",
  , "150_915" # = "Tasso di occupazione",
  , "150_916" # = "Tasso di attività",
  , "150_938" # = "Occupati (migliaia)",
  , "151_914" # = "Tasso di disoccupazione",
  , "151_929" # = "Disoccupati",
  , "152_913" #= "Tasso di inattività",
  , "152_928" #= "Inattivi",
  , "154_373" # imprese con dipendenti 
  , "532_930" #= "Popolazione per condizione professionale"

)

# Set to TRUE to automatically expand root codes to all matching datasets
# Set to FALSE to download only the exact codes specified (solo codici radice)
expand_code <- FALSE

# Start date for data download (full date format for filter_by_time compatibility)
# Data di inizio per il download (formato data completo per compatibilità con filter_by_time)
start_time <- "2000-01-01"

# 1a. Expand dataset codes to full IDs -----
# This happens BEFORE pipeline execution to get static list for tar_map()
dataset_ids <- expand_dataset_ids(dataset_codes, expand = expand_code)

# All datasets will be checked for updates during download
# download_dataset_by_freq_safe() checks LAST_UPDATE and skips unchanged datasets
datasets_to_download <- dataset_ids

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

  # Download metadata (dataflows list)
  # Scarica i metadati (lista dei dataflows)
  tar_target(
    name = metadata,
    command = download_metadata()
  ),

  # Download codelists for all datasets
  # Scarica le codelists per tutti i dataset
  tar_target(
    name = codelists,
    command = download_codelists(dataset_ids)
  ),

  # Refresh any expired codelists (staggered TTL check)
  # Aggiorna le codelists scadute (controllo TTL scaglionato)
  tar_target(
    name = codelists_refresh,
    command = {
      result <- refresh_expired_codelists(verbose = TRUE)
      message("Codelists refresh: ", result$refreshed, "/", result$total, " updated")
      TRUE
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

    # Apply labels to this dataset (depends on codelists_refresh)
    tar_target(
      name = labeled,
      command = {
        # Ensure codelists are available before labeling
        ensure_codelists(dataset_id, verbose = TRUE)
        # Force dependency on codelists_refresh
        stopifnot(codelists_refresh)
        apply_codelist_labels(data, codelists)
      }
    )
  )
)
