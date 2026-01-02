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
  , "534_1037" # ore lavorat e imprese con dipe
  , "534_1038" # posti vacanti
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
, "149_319" # temsione contrattuale
, "149_327" # Orario contrattuale, ferie e altre riduzioni orarie - dipendenti a tempo pieno
, "533_957" # RACLI  Retribuzioni orarie  dei dipendenti del settore privato
 
 )

# Set to TRUE to automatically expand root codes to all matching datasets
# Set to FALSE to download only the exact codes specified (solo codici radice)
expand_code <- FALSE

# Start date for data download (full date format for filter_by_time compatibility)
# Data di inizio per il download (formato data completo per compatibilità con filter_by_time)
start_time <- "2000-01-01"

# 1a. Get dataset-frequency combinations (with caching) -----
# Uses cached frequencies if dataset_codes hasn't changed
dataset_freq_combinations <- get_cached_dataset_freq_combinations(
  dataset_codes = dataset_codes,
  expand = expand_code,
  verbose = TRUE
)

# Skip annual (A) frequency - insufficient observations for forecasting
dataset_freq_combinations <- dataset_freq_combinations[dataset_freq_combinations$freq != "A", ]
message("After excluding annual: ", nrow(dataset_freq_combinations), " combinations")

dataset_ids <- unique(dataset_freq_combinations$dataset_id)

# 2. Set target options -----
tar_option_set(
  packages = c("istatlab", "data.table", "forecast"),
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

  # Per-dataset-frequency targets: download and label each dataset-frequency separately
  # Creates targets like: data_534_50_M, data_534_50_Q, labeled_534_50_M, etc.
  tar_map(
    values = dataset_freq_combinations,
    names = c("dataset_id", "freq"),

    # Download raw data for this dataset-frequency combination
    # Scarica i dati grezzi per questa combinazione dataset-frequenza
    tar_target(
      name = data,
      command = download_dataset_single_freq_safe(
        dataset_id = dataset_id,
        freq = freq,
        start_time = config_start_time,
        check_update = TRUE,
        verbose = TRUE
      )
    ),

    # Apply labels to this dataset-frequency (depends on codelists_refresh)
    # Uses istatlab::apply_labels() internally for correct dimension-to-codelist mapping
    tar_target(
      name = labeled,
      command = {
        # Force dependency on codelists_refresh
        stopifnot(codelists_refresh)
        apply_codelist_labels(data)
      }
    ),

    # Filter labeled data to latest base year
    # Filtra i dati etichettati all'ultimo anno base
    tar_target(
      name = filtered,
      command = filter_latest_base_year(labeled, verbose = TRUE)
    ),

    # Generate forecasts for all series (2 years horizon by default)
    # Genera previsioni per tutte le serie (orizzonte 2 anni per default)
    # Uses parallel processing for large datasets (>500 series)
    tar_target(
      name = forecast,
      command = generate_dataset_forecasts(
        filtered,
        horizon = NULL,  # auto-detect 2 years based on frequency
        models = c("auto.arima", "ets", "naive"),
        large_threshold = 500L,  # use parallel + fast models above this
        verbose = TRUE
      )
    ),

    # Combine historical + forecast data for dashboard use
    # Combina dati storici + previsioni per uso dashboard
    tar_target(
      name = combined,
      command = combine_historical_forecast(filtered, forecast)
    ),

    # Prepare COMBINED data for plotting with series statistics
    # Prepara i dati COMBINATI per la visualizzazione con statistiche per serie
    tar_target(
      name = plot_ready,
      command = prepare_plot_data(combined)
    )
  )
)
