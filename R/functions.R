# R/functions.R
# Custom functions for ISTAT data download workflow
# Author: Giampaolo Montaletti

# 1. Timestamp functions -----

#' Get timestamps from targets store
#'
#' Reads file modification times from _targets/objects/ for data_* targets.
#'
#' @param targets_dir Path to targets objects directory (default: "_targets/objects")
#'
#' @return data.table with columns: dataset_id, last_download (POSIXct)
get_targets_timestamps <- function(targets_dir = "_targets/objects") {
  if (!dir.exists(targets_dir)) {
    return(data.table::data.table(
      dataset_id = character(),
      last_download = as.POSIXct(character())
    ))
  }

  # List data_* files (raw data targets)
  files <- list.files(targets_dir, pattern = "^data_", full.names = TRUE)

  if (length(files) == 0) {
    return(data.table::data.table(
      dataset_id = character(),
      last_download = as.POSIXct(character())
    ))
  }

  # Get file info
  info <- file.info(files)

  # Extract dataset IDs from filenames (remove "data_" prefix)
  data.table::data.table(
    dataset_id = gsub("^data_", "", basename(files)),
    last_download = info$mtime
  )
}

# 2. Update checking functions -----

#' Check if multiple datasets have been updated
#'
#' Checks ISTAT API to see if datasets have been updated since last download.
#' Uses SDMX updatedAfter parameter - queries API for data modified after
#' the last download timestamp. Empty response means no updates.
#'
#' Conservative download rules:
#' - API unreachable: always skip (use cached data if available)
#' - API reachable + updates exist: download
#' - API reachable + no updates: skip
#' - Never downloaded + API works: download
#' - Never downloaded + API unreachable: skip
#'
#' @param dataset_ids Character vector of dataset IDs to check
#' @param targets_dir Path to targets objects directory (default: "_targets/objects")
#' @param rate_limit_delay Seconds to wait between API calls (default: 12)
#' @param timeout Timeout in seconds for each API call (default: 60)
#' @param force_download Logical; if TRUE, mark all datasets as needing download regardless of API status (default: FALSE)
#' @param verbose Logical indicating whether to print progress messages
#'
#' @return data.table with columns: dataset_id, last_download, has_updates, reason
check_multiple_datasets_updated <- function(dataset_ids,
                                           targets_dir = "_targets/objects",
                                           rate_limit_delay = 12,
                                           timeout = 60,
                                           force_download = FALSE,
                                           verbose = TRUE) {

  # Get local timestamps from targets store
  local_timestamps <- get_targets_timestamps(targets_dir)

  # Force download: skip all checks and mark all datasets as needing updates
  if (force_download) {
    if (verbose) {
      message("Force download enabled: marking all ", length(dataset_ids), " datasets for download")
    }
    return(data.table::data.table(
      dataset_id = dataset_ids,
      has_updates = TRUE,
      last_download = as.POSIXct(NA),
      reason = "force_download"
    ))
  }

  if (verbose) {
    message("Checking ", length(dataset_ids), " datasets for updates...")
    message("Rate limit delay: ", rate_limit_delay, " seconds between requests")
  }

  results <- list()

  for (i in seq_along(dataset_ids)) {
    ds_id <- dataset_ids[i]

    # Rate limiting between requests
    if (i > 1) {
      Sys.sleep(rate_limit_delay)
    }

    if (verbose) {
      message("Checking dataset ", i, "/", length(dataset_ids), ": ", ds_id)
    }

    # Get local timestamp for this dataset
    # Note: use ds_id (not dataset_id) to avoid data.table column name collision
    local_time <- local_timestamps[dataset_id == ds_id, last_download]
    if (length(local_time) == 0 || is.na(local_time[1])) {
      local_time <- NULL
    } else {
      local_time <- local_time[1]
    }

    # Check API for updates
    tryCatch({
      if (is.null(local_time)) {
        # First download: check if API is reachable by querying metadata
        url <- istatlab::build_istat_url("data",
                                         dataset_id = ds_id,
                                         start_time = "2024")

        # Set timeout
        old_timeout <- getOption("timeout")
        on.exit(options(timeout = old_timeout), add = TRUE)
        options(timeout = timeout)

        # Try to reach API (just need to confirm connectivity)
        result <- readsdmx::read_sdmx(url)

        if (verbose) message("  First download - API reachable, will download")

        results[[i]] <- data.table::data.table(
          dataset_id = ds_id,
          has_updates = TRUE,
          last_download = as.POSIXct(NA),
          reason = "first_download"
        )

      } else {
        # Existing data: check for updates using updatedAfter
        url <- istatlab::build_istat_url("data",
                                         dataset_id = ds_id,
                                         updated_after = local_time)

        # Set timeout
        old_timeout <- getOption("timeout")
        on.exit(options(timeout = old_timeout), add = TRUE)
        options(timeout = timeout)

        # Query API - empty result means no updates
        result <- readsdmx::read_sdmx(url)
        has_updates <- !is.null(result) && nrow(result) > 0

        if (verbose) {
          msg <- if (has_updates) "  Updates available - will download" else "  No updates - skip"
          message(msg)
        }

        results[[i]] <- data.table::data.table(
          dataset_id = ds_id,
          has_updates = has_updates,
          last_download = local_time,
          reason = if (has_updates) "data_modified" else "no_updates"
        )
      }

    }, error = function(e) {
      # API unreachable: always skip download
      if (verbose) {
        message("  API unreachable: ", e$message)
        message("  Skip download (conservative mode)")
      }

      reason <- if (is.null(local_time)) {
        "api_unreachable_first_download_skipped"
      } else {
        "api_unreachable_skip"
      }

      results[[i]] <<- data.table::data.table(
        dataset_id = ds_id,
        has_updates = FALSE,
        last_download = local_time,
        reason = reason
      )
    })
  }

  results_dt <- data.table::rbindlist(results)

  if (verbose) {
    n_updates <- sum(results_dt$has_updates, na.rm = TRUE)
    message("\nUpdate check complete: ", n_updates, " of ",
            length(dataset_ids), " datasets need updates")
  }

  return(results_dt)
}

# 3. Helper functions -----

#' Extract Root Dataset ID from Compound ID
#'
#' Extracts the root dataset ID from compound ISTAT dataset IDs.
#' For example: "534_49_DF_DCSC_GI_ORE_10" -> "534_49"
#'
#' @param dataset_id Character string with full dataset ID
#' @return Character string with root dataset ID
extract_root_dataset_id <- function(dataset_id) {
  if (grepl("_DF_", dataset_id)) {
    root_id <- sub("_DF_.*$", "", dataset_id)
    return(root_id)
  }
  return(dataset_id)
}

# 4. Download functions -----

#' Check if ISTAT data is valid
#'
#' Validates that data is a proper data.table with required structure.
#' Use before deciding whether to cache or return data.
#'
#' @param data Object to validate
#' @param min_rows Minimum number of rows required (default 1)
#'
#' @return Logical TRUE if data is valid, FALSE otherwise
is_valid_istat_data <- function(data, min_rows = 1L) {
  if (is.null(data)) return(FALSE)
  if (!data.table::is.data.table(data)) return(FALSE)
  if (nrow(data) < min_rows) return(FALSE)
  # Check for required SDMX columns
  required <- c("ObsDimension", "ObsValue")
  if (!all(required %in% names(data))) return(FALSE)
  return(TRUE)
}

#' Download dataset with cache fallback
#'
#' Downloads dataset with proper error handling. If download fails but valid
#' cached data exists in targets store, returns the cached version to prevent
#' overwriting valid data with NULL/error results.
#'
#' @param dataset_id Character string with dataset ID
#' @param start_time Character string with start period
#' @param api_status Logical indicating if API is accessible
#' @param targets_dir Path to targets objects directory (default: "_targets/objects")
#'
#' @return data.table with downloaded data, cached data on API failure,
#'   or stops with error if no valid data available
download_dataset_safe <- function(dataset_id, start_time, api_status,
                                  targets_dir = "_targets/objects") {
  # Construct the target object filename
  target_name <- paste0("data_", dataset_id)
  cached_file <- file.path(targets_dir, target_name)

  # Helper function to read and validate cached data
  read_cached <- function() {
    if (file.exists(cached_file)) {
      cached <- tryCatch(readRDS(cached_file), error = function(e) NULL)
      if (is_valid_istat_data(cached)) {
        return(cached)
      }
    }
    NULL
  }

  # If API not accessible, try to use cache
  if (!api_status) {
    cached_data <- read_cached()
    if (!is.null(cached_data)) {
      message("API not accessible. Using cached data for: ", dataset_id,
              " (", nrow(cached_data), " rows)")
      return(cached_data)
    }
    stop("API not accessible and no valid cached data for: ", dataset_id)
  }

  message("Downloading dataset: ", dataset_id)

  # Attempt download
  downloaded_data <- tryCatch({
    data <- istatlab::download_istat_data(
      dataset_id = dataset_id,
      start_time = start_time,
      verbose = TRUE
    )

    # Validate downloaded data - return NULL to tryCatch (not the function)
    if (!is_valid_istat_data(data)) {
      warning("Download returned invalid/empty data for: ", dataset_id)
      NULL  # Don't use return() inside tryCatch - it exits the whole function!
    } else {
      message("Successfully downloaded ", nrow(data), " rows for dataset: ", dataset_id)
      data
    }

  }, error = function(e) {
    warning("Download error for ", dataset_id, ": ", e$message)
    NULL
  })

  # If download succeeded with valid data, return it
  if (!is.null(downloaded_data)) {
    return(downloaded_data)
  }

  # Download failed - try cache fallback
  cached_data <- read_cached()
  if (!is.null(cached_data)) {
    message("Download failed. Preserving cached data for: ", dataset_id,
            " (", nrow(cached_data), " rows)")
    return(cached_data)
  }

  # No cached data available - this is a real failure
  stop("Download failed and no valid cached data available for: ", dataset_id)
}

#' Apply codelist labels to data
#'
#' Applies labels from codelists to create columns with both codes and labels
#'
#' @param data data.table with raw ISTAT data
#' @param codelists List of codelists from download_codelists()
#'
#' @return data.table with original code columns AND new label columns
apply_codelist_labels <- function(data, codelists) {
  if (is.null(data) || nrow(data) == 0) {
    warning("No data to label")
    return(data)
  }

  # Make a copy to avoid modifying original
  dt <- data.table::copy(data)

  # Get the dataset ID
  dataset_id <- dt$id[1]
  codelist_key <- paste0("X", dataset_id)

  # Fallback to root ID if exact match not found
  if (!codelist_key %in% names(codelists)) {
    root_id <- extract_root_dataset_id(dataset_id)
    if (root_id != dataset_id) {
      root_key <- paste0("X", root_id)
      if (root_key %in% names(codelists)) {
        codelist_key <- root_key
        message("Using codelists from root dataset: ", root_id)
      }
    }
  }

  if (!codelist_key %in% names(codelists)) {
    warning("No codelists found for dataset: ", dataset_id)
    return(dt)
  }

  cl <- codelists[[codelist_key]]
  if (!data.table::is.data.table(cl)) {
    data.table::setDT(cl)
  }

  message("Applying labels to dataset: ", dataset_id)
  message("  Codelist has ", nrow(cl), " entries")

  # Get dimension columns (exclude standard SDMX columns)
  exclude_cols <- c("ObsDimension", "ObsValue", "id", "CONF_STATUS", "OBS_STATUS")
  dim_cols <- setdiff(names(dt), exclude_cols)

  # For each dimension column, try to find matching labels in codelist
  for (dim_col in dim_cols) {
    # Look for codelist entries that match the values in this column
    unique_codes <- unique(dt[[dim_col]])

    # Find matching labels in the codelist - get unique mappings only
    matching_labels <- unique(cl[id_description %in% unique_codes, .(id_description, it_description)])

    if (nrow(matching_labels) > 0) {
      # Create the label column name
      label_col <- paste0(dim_col, "_label")

      # Create a named vector for fast lookup
      label_lookup <- stats::setNames(matching_labels$it_description, matching_labels$id_description)

      # Use simple vector lookup instead of join
      dt[, (label_col) := label_lookup[get(dim_col)]]

      # Fill NA labels with the code itself
      dt[is.na(get(label_col)), (label_col) := get(dim_col)]

      message("  Added labels for: ", dim_col, " (", length(label_lookup), " mappings)")
    }
  }

  # Rename ObsDimension to tempo and convert to Date
  if ("ObsDimension" %in% names(dt)) {
    data.table::setnames(dt, "ObsDimension", "tempo")

    # Try to convert to Date based on format
    dt[, tempo := tryCatch({
      if (grepl("-Q", tempo[1])) {
        # Quarterly: 2020-Q1 -> 2020-01-01
        as.Date(zoo::as.yearqtr(gsub("-Q", " Q", tempo)))
      } else if (nchar(tempo[1]) == 7) {
        # Monthly: 2020-01 -> 2020-01-01
        as.Date(paste0(tempo, "-01"))
      } else if (nchar(tempo[1]) == 4) {
        # Annual: 2020 -> 2020-01-01
        as.Date(paste0(tempo, "-01-01"))
      } else {
        tempo
      }
    }, error = function(e) tempo)]
  }

  # Rename ObsValue to valore and convert to numeric
  if ("ObsValue" %in% names(dt)) {
    data.table::setnames(dt, "ObsValue", "valore")
    dt[, valore := as.numeric(valore)]
  }

  message("Labeling complete: ", ncol(dt), " columns, ", nrow(dt), " rows")
  return(dt)
}

#' Summarize downloaded datasets
#'
#' Create a summary table of all downloaded datasets
#'
#' @param data_list List of data.tables
#'
#' @return data.table with summary statistics
summarize_datasets <- function(data_list) {
  summaries <- lapply(data_list, function(dt) {
    if (is.null(dt)) return(NULL)

    data.table::data.table(
      dataset_id = dt$id[1],
      n_rows = nrow(dt),
      n_cols = ncol(dt),
      columns = paste(names(dt), collapse = ", ")
    )
  })

  data.table::rbindlist(summaries[!sapply(summaries, is.null)])
}
