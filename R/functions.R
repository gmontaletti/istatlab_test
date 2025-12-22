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

# 2. API connectivity functions -----

#' Wait for ISTAT API Connectivity with Retry
#'
#' Waits for the ISTAT API to become accessible, retrying at regular intervals
#' up to a maximum time limit. Useful for scheduled pipelines that should wait
#' for API availability before proceeding.
#'
#' @param max_hours Maximum hours to wait before giving up (default 12)
#' @param check_interval_minutes Minutes between connectivity checks (default 15)
#' @param verbose Logical; print status messages (default TRUE)
#'
#' @return TRUE if API becomes accessible, stops with error if max time exceeded
#' @export
wait_for_api_connectivity <- function(max_hours = 12,
                                      check_interval_minutes = 15,
                                      verbose = TRUE) {
  max_attempts <- ceiling((max_hours * 60) / check_interval_minutes)

  for (attempt in seq_len(max_attempts)) {
    result <- istatlab::test_endpoint_connectivity("data", timeout = 30, verbose = FALSE)

    if (result$accessible[1]) {
      if (verbose) {
        message("API ISTAT raggiungibile dopo ", attempt, " tentativo/i")
      }
      return(TRUE)
    }

    if (verbose) {
      message("API non raggiungibile. Tentativo ", attempt, "/", max_attempts,
              ". Prossimo tentativo tra ", check_interval_minutes, " minuti.")
    }

    if (attempt < max_attempts) {
      Sys.sleep(check_interval_minutes * 60)
    }
  }

  stop("API ISTAT non raggiungibile dopo ", max_hours, " ore di tentativi")
}

# 3. Helper functions -----

#' Random Rate Limit Delay
#'
#' Applies a random delay between API calls to avoid rate limiting.
#'
#' @param min_seconds Minimum delay in seconds (default 6)
#' @param max_seconds Maximum delay in seconds (default 300)
#' @param verbose Logical; print delay message (default TRUE)
#'
#' @return Invisible numeric with actual delay applied
random_rate_limit_delay <- function(min_seconds = 6, max_seconds = 300, verbose = TRUE) {
  delay <- runif(1, min = min_seconds, max = max_seconds)
  if (verbose) {
    message("Rate limit delay: ", round(delay, 1), " seconds...")
  }
  Sys.sleep(delay)
  invisible(delay)
}

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

#' Expand Dataset IDs to Frequency Combinations
#'
#' For each dataset ID, queries available frequencies and returns
#' a data.frame with all dataset-frequency combinations for use
#' in tar_map() two-level branching.
#'
#' @param dataset_ids Character vector of dataset IDs
#' @param verbose Logical; print status messages (default TRUE)
#'
#' @return data.frame with columns: dataset_id, freq
expand_dataset_freq_combinations <- function(dataset_ids, verbose = TRUE) {
  if (verbose) message("Expanding dataset-frequency combinations...")

  combinations <- lapply(dataset_ids, function(id) {
    freqs <- tryCatch({
      istatlab::get_available_frequencies(id)
    }, error = function(e) {
      if (verbose) warning("Could not get frequencies for ", id, ": ", e$message)
      NULL
    })

    # Skip datasets with no available frequencies
    if (is.null(freqs) || length(freqs) == 0) {
      if (verbose) message("  ", id, ": no frequencies found, skipping")
      return(NULL)
    }

    if (verbose) message("  ", id, ": ", paste(freqs, collapse = ", "))

    data.frame(
      dataset_id = id,
      freq = freqs,
      stringsAsFactors = FALSE
    )
  })

  result <- do.call(rbind, combinations)

  if (is.null(result) || nrow(result) == 0) {
    stop("No valid dataset-frequency combinations found")
  }

  if (verbose) {
    message("Total combinations: ", nrow(result),
            " (", length(unique(result$dataset_id)), " datasets)")
  }

  return(result)
}

#' Merge Incremental Data with Cached Data
#'
#' Combines new incremental data with existing cached data, replacing
#' old observations that have been updated.
#'
#' @param old_data data.table with cached data
#' @param new_data data.table with new incremental data
#'
#' @return data.table with merged data
merge_incremental_data <- function(old_data, new_data) {
  if (is.null(new_data) || nrow(new_data) == 0) return(old_data)
  if (is.null(old_data) || nrow(old_data) == 0) return(new_data)

  # Key columns = all dimension columns (exclude value and status columns)
  exclude_cols <- c("ObsValue", "CONF_STATUS", "OBS_STATUS")
  key_cols <- intersect(names(old_data), names(new_data))
  key_cols <- setdiff(key_cols, exclude_cols)

  # Anti-join: keep old rows NOT present in new data
  old_unique <- old_data[!new_data, on = key_cols]

  # Combine old unique + new data
  result <- data.table::rbindlist(list(old_unique, new_data), use.names = TRUE, fill = TRUE)
  return(result)
}

#' Get Latest Edition Value from ISTAT API
#'
#' Queries the availableconstraint endpoint to find available edition values
#' and returns the latest (maximum) edition.
#'
#' @param dataset_id Character string with dataset ID
#' @param verbose Logical; print status messages
#'
#' @return Character string with latest edition value, or NULL if not found
get_latest_edition <- function(dataset_id, verbose = TRUE) {
  # Query available constraints for the dataset
  constraints <- tryCatch({
    istatlab::get_available_constraints(dataset_id)
  }, error = function(e) {
    if (verbose) warning("Errore query edizioni: ", e$message)
    NULL
  })

  if (is.null(constraints)) return(NULL)

  # Find edition dimension (case-insensitive)
  edition_col <- grep("^edition$", names(constraints), ignore.case = TRUE, value = TRUE)

  if (length(edition_col) == 0) return(NULL)

  # Get available edition values and return the max
  editions <- unique(constraints[[edition_col[1]]])
  editions <- editions[!is.na(editions)]

  if (length(editions) == 0) return(NULL)

  # Return latest (max) edition - works for numeric or date-like strings
  latest <- max(editions, na.rm = TRUE)
  if (verbose) message("Ultima edizione disponibile: ", latest)

  return(as.character(latest))
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

  # Attempt download using new structured result API
  result <- istatlab::download_istat_data(
    dataset_id = dataset_id,
    start_time = start_time,
    verbose = TRUE,
    return_result = TRUE
  )

  # Check result using structured istat_result object
  if (result$success && is_valid_istat_data(result$data)) {
    md5_info <- if (!is.na(result$md5)) paste0(" (MD5: ", substr(result$md5, 1, 8), "...)") else ""
    message("Successfully downloaded ", nrow(result$data), " rows for dataset: ", dataset_id, md5_info)
    return(result$data)
  }

  # Download failed - distinguish between timeout and other errors
  if (result$is_timeout) {
    warning("Timeout downloading ", dataset_id, " (exit code: ", result$exit_code, ")")
  } else if (!result$success) {
    warning("Download error for ", dataset_id, ": ", result$message,
            " (exit code: ", result$exit_code, ")")
  } else {
    warning("Download returned invalid/empty data for: ", dataset_id)
  }

  # Try cache fallback
  cached_data <- read_cached()
  if (!is.null(cached_data)) {
    message("Download failed. Preserving cached data for: ", dataset_id,
            " (", nrow(cached_data), " rows)")
    return(cached_data)
  }

  # No cached data available - this is a real failure
  stop("Download failed and no valid cached data available for: ", dataset_id)
}

#' Download Dataset Split by Frequency with Cache Fallback
#'
#' Downloads a dataset split by frequency using download_istat_data_by_freq(),
#' then combines all frequencies into a single data.table. Falls back to cached
#' data if download fails.
#'
#' @param dataset_id Character string with dataset ID (root code)
#' @param start_time Character string with start date (format: "YYYY-MM-DD" or "YYYY")
#' @param check_update Logical; check LAST_UPDATE before downloading (default TRUE)
#' @param targets_dir Path to targets objects directory (default "_targets/objects")
#' @param apply_delay Logical; apply random delay before download (default TRUE)
#' @param delay_min Minimum delay in seconds (default 6)
#' @param delay_max Maximum delay in seconds (default 300)
#' @param verbose Logical; print status messages (default TRUE)
#'
#' @return data.table with all frequencies combined, or cached data on failure
download_dataset_by_freq_safe <- function(dataset_id,
                                          start_time,
                                          check_update = TRUE,
                                          targets_dir = "_targets/objects",
                                          apply_delay = TRUE,
                                          delay_min = 6,
                                          delay_max = 300,
                                          verbose = TRUE) {
  # Apply random delay for rate limiting
  if (apply_delay) {
    random_rate_limit_delay(min_seconds = delay_min, max_seconds = delay_max, verbose = verbose)
  }
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

  # Check LAST_UPDATE if requested
  if (check_update) {
    cached_data <- read_cached()
    if (!is.null(cached_data)) {
      # Get LAST_UPDATE from ISTAT
      last_update <- tryCatch({
        istatlab::get_dataset_last_update(dataset_id)
      }, error = function(e) NULL)

      # Get cached file modification time
      if (!is.null(last_update) && file.exists(cached_file)) {
        cache_mtime <- file.info(cached_file)$mtime

        if (last_update <= cache_mtime) {
          if (verbose) message("Dataset ", dataset_id, " non aggiornato. Skip.")
          return(cached_data)
        }

        # Update detected - check for edition column
        has_edition <- any(grepl("^edition$", names(cached_data), ignore.case = TRUE))

        if (has_edition) {
          # Dataset with editions - download only latest edition
          if (verbose) message("Dataset con edizioni - download ultima edizione")

          latest_edition <- get_latest_edition(dataset_id, verbose = verbose)

          if (!is.null(latest_edition)) {
            # Build filter for latest edition
            dims <- tryCatch({
              istatlab::get_dataset_dimensions(dataset_id)
            }, error = function(e) NULL)

            if (!is.null(dims)) {
              edition_pos <- which(tolower(dims) == "edition")

              if (length(edition_pos) > 0) {
                # Build filter with edition in correct position
                filter_parts <- rep("", length(dims))
                filter_parts[edition_pos] <- latest_edition
                edition_filter <- paste(filter_parts, collapse = ".")

                data_list <- tryCatch({
                  istatlab::download_istat_data_by_freq(
                    dataset_id = dataset_id,
                    filter = edition_filter,
                    start_time = start_time,
                    verbose = verbose
                  )
                }, error = function(e) {
                  warning("Errore download edizione: ", e$message)
                  NULL
                })

                if (!is.null(data_list) && length(data_list) > 0) {
                  combined <- data.table::rbindlist(
                    lapply(names(data_list), function(f) {
                      dt <- data_list[[f]]
                      if (!is.null(dt) && nrow(dt) > 0) dt[, FREQ := f]
                      dt
                    }), fill = TRUE
                  )
                  if (nrow(combined) > 0) {
                    if (verbose) message("Download edizione completato: ", nrow(combined), " righe")
                    return(combined)
                  }
                }
              }
            }
          }
          # Edition filter failed, continue to full download below
          if (verbose) message("Filtro edizione fallito, provo download completo")

        } else {
          # No edition - use incremental update
          incremental_date <- format(as.Date(cache_mtime), "%Y-%m-%d")
          if (verbose) message("Aggiornamento incrementale da: ", incremental_date)

          data_list <- tryCatch({
            istatlab::download_istat_data_by_freq(
              dataset_id = dataset_id,
              incremental = incremental_date,
              verbose = verbose
            )
          }, error = function(e) {
            warning("Errore download incrementale: ", e$message)
            NULL
          })

          if (!is.null(data_list) && length(data_list) > 0) {
            combined <- data.table::rbindlist(
              lapply(names(data_list), function(f) {
                dt <- data_list[[f]]
                if (!is.null(dt) && nrow(dt) > 0) dt[, FREQ := f]
                dt
              }), fill = TRUE
            )
            if (nrow(combined) > 0) {
              result <- merge_incremental_data(cached_data, combined)
              if (verbose) message("Merge completato: ", nrow(result), " righe totali")
              return(result)
            }
          }
          # Incremental failed, fall through to full download
          if (verbose) message("Incrementale fallito, provo download completo")
        }
      }
    }
  }

  if (verbose) message("Download dataset: ", dataset_id, " (split per frequenza)")

  # Attempt download split by frequency
  data_list <- tryCatch({
    istatlab::download_istat_data_by_freq(
      dataset_id = dataset_id,
      start_time = start_time,
      verbose = verbose
    )
  }, error = function(e) {
    warning("Errore download ", dataset_id, ": ", e$message)
    NULL
  })

  # Process results
  if (!is.null(data_list) && length(data_list) > 0) {
    # Add frequency column and combine
    combined_list <- lapply(names(data_list), function(freq_name) {
      dt <- data_list[[freq_name]]
      if (!is.null(dt) && nrow(dt) > 0) {
        dt[, FREQ := freq_name]
        return(dt)
      }
      NULL
    })

    # Remove NULLs and combine
    combined_list <- combined_list[!sapply(combined_list, is.null)]

    if (length(combined_list) > 0) {
      result <- data.table::rbindlist(combined_list, fill = TRUE)
      if (verbose) {
        message("Download completato: ", nrow(result), " righe, ",
                length(combined_list), " frequenze")
      }
      return(result)
    }
  }

  # Download failed - try cache fallback
  cached_data <- read_cached()
  if (!is.null(cached_data)) {
    warning("Download fallito. Uso dati in cache per: ", dataset_id,
            " (", nrow(cached_data), " righe)")
    return(cached_data)
  }

  stop("Download fallito e nessun dato in cache per: ", dataset_id)
}

# 5. Data processing functions -----

#' Apply codelist labels to data
#'
#' Applies labels from codelists to create columns with both codes and labels.
#' Automatically ensures codelists are available for the dataset before labeling.
#' If labeling fails due to missing codes, refreshes codelists and retries.
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

  # Get the dataset ID
  dataset_id <- data$id[1]

  # Ensure codelists are available for this dataset before labeling
  istatlab::ensure_codelists(dataset_id, verbose = FALSE)

  # Try labeling with current codelists, refresh and retry on failure
  result <- tryCatch({
    apply_labels_internal(data, codelists, dataset_id)
  }, error = function(e) {
    message("Labeling failed: ", e$message)
    message("Refreshing codelists for ", dataset_id, " and retrying...")

    # Force refresh codelists for this dataset
    fresh_codelists <- istatlab::download_codelists(dataset_id, force_update = TRUE)

    # Retry with fresh codelists
    apply_labels_internal(data, fresh_codelists, dataset_id)
  })

  return(result)
}

#' Internal function to apply labels (used by apply_codelist_labels)
#'
#' @param data data.table with raw ISTAT data
#' @param codelists List of codelists
#' @param dataset_id Dataset ID string
#'
#' @return data.table with labels applied
#' @keywords internal
apply_labels_internal <- function(data, codelists, dataset_id) {
  # Make a copy to avoid modifying original
  dt <- data.table::copy(data)

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

    # Find matching labels in the codelist - keep only first occurrence per code
    matching_labels <- cl[id_description %in% unique_codes, .(id_description, it_description)]
    matching_labels <- matching_labels[!duplicated(id_description)]

    if (nrow(matching_labels) > 0) {
      # Create the label column name
      label_col <- paste0(dim_col, "_label")

      # Create lookup table and merge (safer than vector indexing)
      lookup_dt <- data.table::copy(matching_labels)
      data.table::setnames(lookup_dt, c(dim_col, label_col))
      # Ensure join key types match
      lookup_dt[[dim_col]] <- as.character(lookup_dt[[dim_col]])
      dt[[dim_col]] <- as.character(dt[[dim_col]])
      dt <- merge(dt, lookup_dt, by = dim_col, all.x = TRUE)

      # Fill NA labels with the code itself
      dt[is.na(get(label_col)), (label_col) := get(dim_col)]

      message("  Added labels for: ", dim_col, " (", nrow(matching_labels), " mappings)")
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
