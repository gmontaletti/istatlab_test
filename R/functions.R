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
wait_for_api_connectivity <- function(
  max_hours = 12,
  check_interval_minutes = 15,
  verbose = TRUE
) {
  max_attempts <- ceiling((max_hours * 60) / check_interval_minutes)

  for (attempt in seq_len(max_attempts)) {
    result <- istatlab::test_endpoint_connectivity(
      "data",
      timeout = 30,
      verbose = FALSE
    )

    if (result$accessible[1]) {
      if (verbose) {
        message("API ISTAT raggiungibile dopo ", attempt, " tentativo/i")
      }
      return(TRUE)
    }

    if (verbose) {
      message(
        "API non raggiungibile. Tentativo ",
        attempt,
        "/",
        max_attempts,
        ". Prossimo tentativo tra ",
        check_interval_minutes,
        " minuti."
      )
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
random_rate_limit_delay <- function(
  min_seconds = 6,
  max_seconds = 300,
  verbose = TRUE
) {
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
  if (verbose) {
    message("Expanding dataset-frequency combinations...")
  }

  combinations <- lapply(dataset_ids, function(id) {
    freqs <- tryCatch(
      {
        istatlab::get_available_frequencies(id)
      },
      error = function(e) {
        if (verbose) {
          warning("Could not get frequencies for ", id, ": ", e$message)
        }
        NULL
      }
    )

    # Skip datasets with no available frequencies
    if (is.null(freqs) || length(freqs) == 0) {
      if (verbose) {
        message("  ", id, ": no frequencies found, skipping")
      }
      return(NULL)
    }

    if (verbose) {
      message("  ", id, ": ", paste(freqs, collapse = ", "))
    }

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
    message(
      "Total combinations: ",
      nrow(result),
      " (",
      length(unique(result$dataset_id)),
      " datasets)"
    )
  }

  return(result)
}

#' Get Dataset Frequency Combinations with Cache
#'
#' Returns cached dataset-frequency combinations if dataset_codes hasn't changed.
#' Otherwise queries the API, caches the result, and returns it.
#'
#' @param dataset_codes Character vector of dataset codes (original user input)
#' @param expand Logical; whether to expand root codes (default FALSE)
#' @param cache_file Path to cache file (default "meta/dataset_freq_cache.rds")
#' @param verbose Logical; print status messages (default TRUE)
#'
#' @return data.frame with columns: dataset_id, freq
get_cached_dataset_freq_combinations <- function(
  dataset_codes,
  expand = FALSE,
  cache_file = "meta/dataset_freq_cache.rds",
  verbose = TRUE
) {
  cache_dir <- dirname(cache_file)
  if (!dir.exists(cache_dir)) {
    dir.create(cache_dir, recursive = TRUE)
  }

  if (file.exists(cache_file)) {
    cache <- tryCatch(readRDS(cache_file), error = function(e) NULL)
    if (!is.null(cache)) {
      if (
        identical(sort(cache$dataset_codes), sort(dataset_codes)) &&
          identical(cache$expand_code, expand)
      ) {
        if (verbose) {
          message(
            "Using cached frequencies (",
            nrow(cache$combinations),
            " combinations)"
          )
        }
        return(cache$combinations)
      }
      if (verbose) message("Dataset codes changed, refreshing cache...")
    }
  }

  dataset_ids <- istatlab::expand_dataset_ids(dataset_codes, expand = expand)
  combinations <- expand_dataset_freq_combinations(
    dataset_ids,
    verbose = verbose
  )

  cache <- list(
    dataset_codes = dataset_codes,
    expand_code = expand,
    dataset_ids = dataset_ids,
    combinations = combinations,
    created = Sys.time()
  )
  tryCatch(saveRDS(cache, cache_file), error = function(e) {
    warning("Could not save cache: ", e$message)
  })

  combinations
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
  if (is.null(new_data) || nrow(new_data) == 0) {
    return(old_data)
  }
  if (is.null(old_data) || nrow(old_data) == 0) {
    return(new_data)
  }

  # Key columns = all dimension columns (exclude value and status columns)
  exclude_cols <- c("ObsValue", "CONF_STATUS", "OBS_STATUS")
  key_cols <- intersect(names(old_data), names(new_data))
  key_cols <- setdiff(key_cols, exclude_cols)

  # Anti-join: keep old rows NOT present in new data
  old_unique <- old_data[!new_data, on = key_cols]

  # Combine old unique + new data
  result <- data.table::rbindlist(
    list(old_unique, new_data),
    use.names = TRUE,
    fill = TRUE
  )
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
  constraints <- tryCatch(
    {
      istatlab::get_available_constraints(dataset_id)
    },
    error = function(e) {
      if (verbose) {
        warning("Errore query edizioni: ", e$message)
      }
      NULL
    }
  )

  if (is.null(constraints)) {
    return(NULL)
  }

  # Find edition dimension (case-insensitive)
  edition_col <- grep(
    "^edition$",
    names(constraints),
    ignore.case = TRUE,
    value = TRUE
  )

  if (length(edition_col) == 0) {
    return(NULL)
  }

  # Get available edition values and return the max
  editions <- unique(constraints[[edition_col[1]]])
  editions <- editions[!is.na(editions)]

  if (length(editions) == 0) {
    return(NULL)
  }

  # Return latest (max) edition - works for numeric or date-like strings
  latest <- max(editions, na.rm = TRUE)
  if (verbose) {
    message("Ultima edizione disponibile: ", latest)
  }

  return(as.character(latest))
}

#' Filter Data to Latest Edition
#'
#' Filters a data.table to keep only rows with the latest (maximum) edition value.
#' Edition column is detected case-insensitively.
#'
#' @param dt data.table with ISTAT data
#' @param verbose Logical; print status messages (default TRUE)
#'
#' @return data.table filtered to latest edition, or original data if no edition column
filter_latest_edition <- function(dt, verbose = TRUE) {
  if (is.null(dt) || nrow(dt) == 0) {
    return(dt)
  }

  # Find edition column (case-insensitive match)
  edition_col <- grep("^edition$", names(dt), ignore.case = TRUE, value = TRUE)

  if (length(edition_col) == 0) {
    if (verbose) {
      message("No edition column found, skipping edition filter")
    }
    return(dt)
  }
  edition_col <- edition_col[1] # Use first match if multiple

  # Get unique editions and find max
  editions <- unique(dt[[edition_col]])
  editions <- editions[!is.na(editions)]

  if (length(editions) <= 1) {
    if (verbose) {
      message("Single or no edition found, no filtering needed")
    }
    return(dt)
  }

  latest_edition <- max(editions, na.rm = TRUE)

  # Filter to latest edition
  original_rows <- nrow(dt)
  dt_filtered <- dt[get(edition_col) == latest_edition]

  if (verbose) {
    message(
      "Edition filter: ",
      original_rows,
      " -> ",
      nrow(dt_filtered),
      " rows (keeping edition ",
      latest_edition,
      ")"
    )
  }

  return(dt_filtered)
}

#' Filter Data to Latest Base Year
#'
#' Parses "base YYYY" pattern from DATA_TYPE_label column and filters
#' to keep only rows with the highest base year found.
#'
#' @param dt data.table with labeled ISTAT data (must have DATA_TYPE_label column)
#' @param label_col Name of the label column to parse (default "DATA_TYPE_label")
#' @param verbose Logical; print status messages (default TRUE)
#'
#' @return data.table filtered to latest base year, or original if no base year pattern found
filter_latest_base_year <- function(
  dt,
  label_col = "DATA_TYPE_label",
  verbose = TRUE
) {
  if (is.null(dt) || nrow(dt) == 0) {
    return(dt)
  }

  # Check if label column exists
  if (!label_col %in% names(dt)) {
    if (verbose) {
      message("Column ", label_col, " not found, skipping base year filter")
    }
    return(dt)
  }

  # Extract base year from labels using regex
  dt[,
    .base_year_temp := as.numeric(
      gsub(".*base\\s*([0-9]{4}).*", "\\1", get(label_col), perl = TRUE)
    )
  ]

  # Check for valid years

  valid_years <- dt[
    !is.na(.base_year_temp) & .base_year_temp >= 1900,
    .base_year_temp
  ]

  if (length(valid_years) == 0) {
    dt[, .base_year_temp := NULL]
    if (verbose) {
      message("No base year patterns found in ", label_col, ", skipping filter")
    }
    return(dt)
  }

  # Find max base year
  max_base_year <- max(valid_years, na.rm = TRUE)
  original_rows <- nrow(dt)

  # Keep rows with max base year OR no base year pattern (NA)
  dt_filtered <- dt[.base_year_temp == max_base_year | is.na(.base_year_temp)]

  # Clean up temporary column
  dt_filtered[, .base_year_temp := NULL]
  dt[, .base_year_temp := NULL]

  if (verbose) {
    message(
      "Base year filter: ",
      original_rows,
      " -> ",
      nrow(dt_filtered),
      " rows (keeping base ",
      max_base_year,
      ")"
    )
  }

  return(dt_filtered)
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
  if (is.null(data)) {
    return(FALSE)
  }
  if (!data.table::is.data.table(data)) {
    return(FALSE)
  }
  if (nrow(data) < min_rows) {
    return(FALSE)
  }
  # Check for required SDMX columns
  required <- c("ObsDimension", "ObsValue")
  if (!all(required %in% names(data))) {
    return(FALSE)
  }
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
download_dataset_safe <- function(
  dataset_id,
  start_time,
  api_status,
  targets_dir = "_targets/objects"
) {
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
      message(
        "API not accessible. Using cached data for: ",
        dataset_id,
        " (",
        nrow(cached_data),
        " rows)"
      )
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
    md5_info <- if (!is.na(result$md5)) {
      paste0(" (MD5: ", substr(result$md5, 1, 8), "...)")
    } else {
      ""
    }
    message(
      "Successfully downloaded ",
      nrow(result$data),
      " rows for dataset: ",
      dataset_id,
      md5_info
    )
    return(result$data)
  }

  # Download failed - distinguish between timeout and other errors
  if (result$is_timeout) {
    warning(
      "Timeout downloading ",
      dataset_id,
      " (exit code: ",
      result$exit_code,
      ")"
    )
  } else if (!result$success) {
    warning(
      "Download error for ",
      dataset_id,
      ": ",
      result$message,
      " (exit code: ",
      result$exit_code,
      ")"
    )
  } else {
    warning("Download returned invalid/empty data for: ", dataset_id)
  }

  # Try cache fallback
  cached_data <- read_cached()
  if (!is.null(cached_data)) {
    message(
      "Download failed. Preserving cached data for: ",
      dataset_id,
      " (",
      nrow(cached_data),
      " rows)"
    )
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
download_dataset_by_freq_safe <- function(
  dataset_id,
  start_time,
  check_update = TRUE,
  targets_dir = "_targets/objects",
  apply_delay = TRUE,
  delay_min = 6,
  delay_max = 300,
  verbose = TRUE
) {
  # Apply random delay for rate limiting
  if (apply_delay) {
    random_rate_limit_delay(
      min_seconds = delay_min,
      max_seconds = delay_max,
      verbose = verbose
    )
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
      last_update <- tryCatch(
        {
          istatlab::get_dataset_last_update(dataset_id)
        },
        error = function(e) NULL
      )

      # Get cached file modification time
      if (!is.null(last_update) && file.exists(cached_file)) {
        cache_mtime <- file.info(cached_file)$mtime

        if (last_update <= cache_mtime) {
          if (verbose) {
            message("Dataset ", dataset_id, " non aggiornato. Skip.")
          }
          return(cached_data)
        }

        # Update detected - check for edition column
        has_edition <- any(grepl(
          "^edition$",
          names(cached_data),
          ignore.case = TRUE
        ))

        if (has_edition) {
          # Dataset with editions - download only latest edition
          if (verbose) {
            message("Dataset con edizioni - download ultima edizione")
          }

          latest_edition <- get_latest_edition(dataset_id, verbose = verbose)

          if (!is.null(latest_edition)) {
            # Build filter for latest edition
            dims <- tryCatch(
              {
                istatlab::get_dataset_dimensions(dataset_id)
              },
              error = function(e) NULL
            )

            if (!is.null(dims)) {
              edition_pos <- which(tolower(dims) == "edition")

              if (length(edition_pos) > 0) {
                # Build filter with edition in correct position
                filter_parts <- rep("", length(dims))
                filter_parts[edition_pos] <- latest_edition
                edition_filter <- paste(filter_parts, collapse = ".")

                data_list <- tryCatch(
                  {
                    istatlab::download_istat_data_by_freq(
                      dataset_id = dataset_id,
                      filter = edition_filter,
                      start_time = start_time,
                      verbose = verbose
                    )
                  },
                  error = function(e) {
                    warning("Errore download edizione: ", e$message)
                    NULL
                  }
                )

                if (!is.null(data_list) && length(data_list) > 0) {
                  combined <- data.table::rbindlist(
                    lapply(names(data_list), function(f) {
                      dt <- data_list[[f]]
                      if (!is.null(dt) && nrow(dt) > 0) {
                        dt[, FREQ := f]
                      }
                      dt
                    }),
                    fill = TRUE
                  )
                  if (nrow(combined) > 0) {
                    if (verbose) {
                      message(
                        "Download edizione completato: ",
                        nrow(combined),
                        " righe"
                      )
                    }
                    return(combined)
                  }
                }
              }
            }
          }
          # Edition filter failed, continue to full download below
          if (verbose) {
            message("Filtro edizione fallito, provo download completo")
          }
        } else {
          # No edition - use incremental update
          incremental_date <- format(as.Date(cache_mtime), "%Y-%m-%d")
          if (verbose) {
            message("Aggiornamento incrementale da: ", incremental_date)
          }

          data_list <- tryCatch(
            {
              istatlab::download_istat_data_by_freq(
                dataset_id = dataset_id,
                incremental = incremental_date,
                verbose = verbose
              )
            },
            error = function(e) {
              warning("Errore download incrementale: ", e$message)
              NULL
            }
          )

          if (!is.null(data_list) && length(data_list) > 0) {
            combined <- data.table::rbindlist(
              lapply(names(data_list), function(f) {
                dt <- data_list[[f]]
                if (!is.null(dt) && nrow(dt) > 0) {
                  dt[, FREQ := f]
                }
                dt
              }),
              fill = TRUE
            )
            if (nrow(combined) > 0) {
              result <- merge_incremental_data(cached_data, combined)
              if (verbose) {
                message("Merge completato: ", nrow(result), " righe totali")
              }
              return(result)
            }
          }
          # Incremental failed, fall through to full download
          if (verbose) message("Incrementale fallito, provo download completo")
        }
      }
    }
  }

  if (verbose) {
    message("Download dataset: ", dataset_id, " (split per frequenza)")
  }

  # Attempt download split by frequency
  data_list <- tryCatch(
    {
      istatlab::download_istat_data_by_freq(
        dataset_id = dataset_id,
        start_time = start_time,
        verbose = verbose
      )
    },
    error = function(e) {
      warning("Errore download ", dataset_id, ": ", e$message)
      NULL
    }
  )

  # Process results
  if (!is.null(data_list) && length(data_list) > 0) {
    # Handle "ALL" fallback case - data already has FREQ column
    if ("ALL" %in% names(data_list) && length(data_list) == 1) {
      result <- data_list[["ALL"]]
      if (!is.null(result) && nrow(result) > 0) {
        if (verbose) {
          freqs <- if ("FREQ" %in% names(result)) {
            unique(result$FREQ)
          } else {
            "unknown"
          }
          message(
            "Download completato (from ALL): ",
            nrow(result),
            " righe, ",
            "frequenze: ",
            paste(freqs, collapse = ", ")
          )
        }
        return(result)
      }
    }

    # Normal case: add frequency column and combine
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
        message(
          "Download completato: ",
          nrow(result),
          " righe, ",
          length(combined_list),
          " frequenze"
        )
      }
      return(result)
    }
  }

  # Download failed - try cache fallback
  cached_data <- read_cached()
  if (!is.null(cached_data)) {
    warning(
      "Download fallito. Uso dati in cache per: ",
      dataset_id,
      " (",
      nrow(cached_data),
      " righe)"
    )
    return(cached_data)
  }

  stop("Download fallito e nessun dato in cache per: ", dataset_id)
}

#' Download Single Frequency Dataset with Cache Fallback
#'
#' Downloads a single frequency slice of a dataset. Uses per-frequency
#' caching for independent update tracking. Designed for use with
#' tar_map() two-level branching.
#'
#' @param dataset_id Character string with dataset ID
#' @param freq Character string with frequency code (M, Q, A)
#' @param start_time Character string with start date
#' @param check_update Logical; check LAST_UPDATE before downloading (default TRUE)
#' @param targets_dir Path to targets objects directory (default "_targets/objects")
#' @param apply_delay Logical; apply random delay before download (default TRUE)
#' @param delay_min Minimum delay in seconds (default 6)
#' @param delay_max Maximum delay in seconds (default 300)
#' @param verbose Logical; print status messages (default TRUE)
#'
#' @return data.table with single frequency data including FREQ column
download_dataset_single_freq_safe <- function(
  dataset_id,
  freq,
  start_time,
  check_update = TRUE,
  targets_dir = "_targets/objects",
  apply_delay = TRUE,
  delay_min = 6,
  delay_max = 300,
  verbose = TRUE
) {
  # Apply random delay for rate limiting
  if (apply_delay) {
    random_rate_limit_delay(
      min_seconds = delay_min,
      max_seconds = delay_max,
      verbose = verbose
    )
  }

  # Construct frequency-specific target filename
  target_name <- paste0("data_", dataset_id, "_", freq)
  cached_file <- file.path(targets_dir, target_name)

  # Helper to read cached data
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
      last_update <- tryCatch(
        {
          istatlab::get_dataset_last_update(dataset_id)
        },
        error = function(e) NULL
      )

      if (!is.null(last_update) && file.exists(cached_file)) {
        cache_mtime <- file.info(cached_file)$mtime

        if (last_update <= cache_mtime) {
          if (verbose) {
            message("Dataset ", dataset_id, "_", freq, " non aggiornato. Skip.")
          }
          return(cached_data)
        }

        # Update detected - try incremental update
        has_edition <- any(grepl(
          "^edition$",
          names(cached_data),
          ignore.case = TRUE
        ))

        if (!has_edition) {
          incremental_date <- format(as.Date(cache_mtime), "%Y-%m-%d")
          if (verbose) {
            message("Aggiornamento incrementale da: ", incremental_date)
          }

          incr_list <- tryCatch(
            {
              istatlab::download_istat_data_by_freq(
                dataset_id = dataset_id,
                incremental = incremental_date,
                verbose = verbose,
                freq = freq
              )
            },
            error = function(e) NULL
          )

          if (!is.null(incr_list)) {
            new_data <- NULL
            # Case 1: data already split by frequency
            if (freq %in% names(incr_list)) {
              new_data <- incr_list[[freq]]
            }
            # Case 2: fallback when frequency check failed - data returned as "ALL"
            if (
              is.null(new_data) &&
                "ALL" %in% names(incr_list) &&
                !is.null(incr_list[["ALL"]])
            ) {
              all_data <- incr_list[["ALL"]]
              if (nrow(all_data) > 0 && "FREQ" %in% names(all_data)) {
                new_data <- all_data[FREQ == freq]
              }
            }
            if (!is.null(new_data) && nrow(new_data) > 0) {
              new_data[, FREQ := freq]
              result <- merge_incremental_data(cached_data, new_data)
              if (verbose) {
                message("Merge completato: ", nrow(result), " righe")
              }
              return(result)
            }
          }
        }
      }
    }
  }

  if (verbose) {
    message("Download dataset: ", dataset_id, " freq: ", freq)
  }

  # Download using istatlab by_freq function (with specific frequency)
  data_list <- tryCatch(
    {
      istatlab::download_istat_data_by_freq(
        dataset_id = dataset_id,
        start_time = start_time,
        verbose = verbose,
        freq = freq
      )
    },
    error = function(e) {
      warning("Errore download ", dataset_id, ": ", e$message)
      NULL
    }
  )

  # Extract the requested frequency
  if (!is.null(data_list)) {
    # Case 1: data is already split by frequency
    if (freq %in% names(data_list)) {
      result <- data_list[[freq]]
      if (!is.null(result) && nrow(result) > 0) {
        result[, FREQ := freq]
        if (verbose) {
          message("Download completato: ", nrow(result), " righe per ", freq)
        }
        return(filter_latest_edition(result, verbose = verbose))
      }
    }
    # Case 2: fallback when frequency check failed - data returned as "ALL"
    # Filter by FREQ column in the data
    if ("ALL" %in% names(data_list) && !is.null(data_list[["ALL"]])) {
      all_data <- data_list[["ALL"]]
      if (nrow(all_data) > 0 && "FREQ" %in% names(all_data)) {
        result <- all_data[FREQ == freq]
        if (nrow(result) > 0) {
          if (verbose) {
            message(
              "Download completato (from ALL): ",
              nrow(result),
              " righe per ",
              freq
            )
          }
          return(filter_latest_edition(result, verbose = verbose))
        }
      }
    }
  }

  # Download failed - try cache fallback
  cached_data <- read_cached()
  if (!is.null(cached_data)) {
    warning(
      "Download fallito. Uso cache per: ",
      dataset_id,
      "_",
      freq,
      " (",
      nrow(cached_data),
      " righe)"
    )
    return(cached_data)
  }

  stop("Download fallito e nessun dato in cache per: ", dataset_id, "_", freq)
}

# 5. Data processing functions -----

#' Apply codelist labels to data
#'
#' Wrapper around istatlab::apply_labels() with error recovery.
#' Uses the package function which correctly maps each dimension to its
#' specific codelist, preventing label mixing.
#'
#' @param data data.table with raw ISTAT data
#' @param codelists Ignored (kept for backward compatibility, apply_labels loads from cache)
#'
#' @return data.table with label columns added
apply_codelist_labels <- function(data, codelists = NULL) {
  if (is.null(data) || nrow(data) == 0) {
    warning("No data to label")
    return(data)
  }

  dataset_id <- data$id[1]

  # Ensure codelists are available for this dataset before labeling
  istatlab::ensure_codelists(dataset_id, verbose = FALSE)

  # Try labeling with package function (uses dimension-specific codelist mapping)
  result <- tryCatch(
    {
      istatlab::apply_labels(data, verbose = FALSE)
    },
    error = function(e) {
      message("Labeling failed: ", e$message)
      message("Refreshing codelists for ", dataset_id, " and retrying...")

      # Force refresh codelists for this dataset
      istatlab::download_codelists(dataset_id, force_update = TRUE)

      # Retry with fresh cache
      istatlab::apply_labels(data, verbose = FALSE)
    }
  )

  return(result)
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
    if (is.null(dt)) {
      return(NULL)
    }

    data.table::data.table(
      dataset_id = dt$id[1],
      n_rows = nrow(dt),
      n_cols = ncol(dt),
      columns = paste(names(dt), collapse = ", ")
    )
  })

  data.table::rbindlist(summaries[!sapply(summaries, is.null)])
}

# 6. Plotting and forecasting functions -----

#' Identify Dimension Columns in ISTAT Data
#'
#' Returns column names that represent dimensions (excluding temporal,
#' value, and label columns).
#'
#' @param dt data.table with ISTAT data
#'
#' @return Character vector of dimension column names
get_dimension_columns <- function(dt) {
  all_cols <- names(dt)

  # Exclude common non-dimension columns
  exclude_patterns <- c(
    "^tempo",
    "^valore$",
    "^ObsValue$",
    "^ObsDimension$",
    "^CONF_STATUS$",
    "^OBS_STATUS$",
    "^FREQ$",
    "_label$",
    "^id$",
    "^type$",
    "^NOTE_" # Exclude note columns
  )

  exclude_cols <- unlist(lapply(exclude_patterns, function(p) {
    grep(p, all_cols, value = TRUE, ignore.case = TRUE)
  }))

  setdiff(all_cols, exclude_cols)
}

#' Compute Series Statistics
#'
#' Computes statistics for each unique time series in the data.
#'
#' @param dt data.table with labeled ISTAT data
#' @param value_col Name of value column (default "valore")
#' @param time_col Name of time column (default "tempo")
#'
#' @return data.table with per-series statistics
compute_series_statistics <- function(
  dt,
  value_col = "valore",
  time_col = "tempo"
) {
  if (!value_col %in% names(dt) || !time_col %in% names(dt)) {
    return(data.table::data.table())
  }

  dim_cols <- get_dimension_columns(dt)

  # If no dimension columns, treat as single series
  if (length(dim_cols) == 0) {
    return(data.table::data.table(
      series_id = "all",
      n_obs = nrow(dt),
      start_date = min(dt[[time_col]], na.rm = TRUE),
      end_date = max(dt[[time_col]], na.rm = TRUE),
      mean = mean(dt[[value_col]], na.rm = TRUE),
      sd = stats::sd(dt[[value_col]], na.rm = TRUE),
      min = min(dt[[value_col]], na.rm = TRUE),
      max = max(dt[[value_col]], na.rm = TRUE),
      n_missing = sum(is.na(dt[[value_col]]))
    ))
  }

  # Compute stats per unique series
  stats_dt <- dt[,
    .(
      n_obs = .N,
      start_date = min(get(time_col), na.rm = TRUE),
      end_date = max(get(time_col), na.rm = TRUE),
      mean = mean(get(value_col), na.rm = TRUE),
      sd = stats::sd(get(value_col), na.rm = TRUE),
      min = min(get(value_col), na.rm = TRUE),
      max = max(get(value_col), na.rm = TRUE),
      n_missing = sum(is.na(get(value_col)))
    ),
    by = dim_cols
  ]

  # Add series_id column
  stats_dt[, series_id := do.call(paste, c(.SD, sep = "_")), .SDcols = dim_cols]

  return(stats_dt)
}

#' Prepare Data for Plotting
#'
#' Prepares labeled ISTAT data for visualization. Computes per-series
#' statistics and returns structured output for ggplot integration.
#'
#' @param labeled_data data.table with labeled ISTAT data
#' @param value_col Name of value column (default "valore")
#' @param time_col Name of time column (default "tempo")
#'
#' @return List with: data (plot-ready), stats (per-series), dimensions, n_series
prepare_plot_data <- function(
  labeled_data,
  value_col = "valore",
  time_col = "tempo"
) {
  if (is.null(labeled_data) || nrow(labeled_data) == 0) {
    warning("No data to prepare for plotting")
    return(list(
      data = labeled_data,
      stats = data.table::data.table(),
      dimensions = character(),
      n_series = 0L
    ))
  }

  # Ensure time column is Date
  dt <- data.table::copy(labeled_data)
  if (!inherits(dt[[time_col]], "Date")) {
    dt[, (time_col) := as.Date(get(time_col))]
  }

  # Identify dimension columns
  dim_cols <- get_dimension_columns(dt)

  # Compute series statistics
  stats <- compute_series_statistics(dt, value_col, time_col)

  # Count unique series
  if (length(dim_cols) > 0) {
    n_series <- nrow(unique(dt[, ..dim_cols]))
  } else {
    n_series <- 1L
  }

  result <- list(
    data = dt,
    stats = stats,
    dimensions = dim_cols,
    n_series = n_series
  )

  class(result) <- c("istat_plot_ready", class(result))
  return(result)
}

#' Generate Forecasts for All Series in a Dataset
#'
#' Identifies unique time series in the dataset and generates forecasts
#' for each using istatlab::forecast_series().
#'
#' @param labeled_data data.table with labeled ISTAT data
#' @param horizon Forecast horizon (NULL = auto-detect 2 years based on frequency)
#' @param models Character vector of models to fit
#' @param value_col Name of value column (default "valore")
#' @param time_col Name of time column (default "tempo")
#' @param freq_col Name of frequency column (default "FREQ")
#' @param min_obs Minimum observations required for forecasting (default 12)
#' @param verbose Logical; print status messages (default TRUE)
#'
#' @return List with: forecasts (named list), n_series, n_success, dimension_cols
generate_dataset_forecasts <- function(
  labeled_data,
  horizon = NULL,
  models = c("auto.arima", "ets", "naive"),
  value_col = "valore",
  time_col = "tempo",
  freq_col = "FREQ",
  min_obs = 12L,
  n_cores = NULL,
  large_threshold = 500L,
  verbose = TRUE
) {
  if (is.null(labeled_data) || nrow(labeled_data) == 0) {
    warning("No data for forecasting")
    return(list(
      forecasts = list(),
      n_series = 0L,
      n_success = 0L,
      dimension_cols = character(),
      skipped = FALSE
    ))
  }

  # Get dimension columns (excluding _label columns for grouping)
  dim_cols <- get_dimension_columns(labeled_data)
  dim_cols_no_label <- dim_cols[!grepl("_label$", dim_cols)]

  if (length(dim_cols_no_label) == 0) {
    # Single series case
    series_list <- list(single_series = labeled_data)
    series_dims_list <- list(single_series = NULL)
  } else {
    # Split data by dimension combinations
    labeled_data[,
      .series_key := do.call(paste, c(.SD, sep = "_")),
      .SDcols = dim_cols_no_label
    ]

    if (verbose) {
      n_unique <- length(unique(labeled_data$.series_key))
      message("Found ", n_unique, " unique series to forecast")
      message("Splitting data by series...")
    }

    # Use data.table split (fast)
    series_list <- split(labeled_data, by = ".series_key", keep.by = FALSE)

    # Extract dimension values for each series
    series_dims_list <- lapply(series_list, function(dt) {
      dt[1, ..dim_cols_no_label]
    })

    # Clean up
    labeled_data[, .series_key := NULL]
  }

  n_series <- length(series_list)
  series_names <- names(series_list)

  # Always use all models (auto.arima, ets, naive)
  if (verbose) {
    message("Using models: ", paste(models, collapse = ", "))
  }

  # Always use parallel processing
  use_parallel <- TRUE
  if (is.null(n_cores)) {
    n_cores <- max(1L, parallel::detectCores() - 1L)
  }
  if (verbose) {
    message("Using parallel processing with ", n_cores, " cores")
  }

  # Function to forecast a single series (takes data directly)
  forecast_one_series <- function(series_data, series_dims) {
    if (nrow(series_data) < min_obs) {
      return(NULL)
    }

    tryCatch(
      {
        fc <- istatlab::forecast_series(
          series_data,
          time_col = time_col,
          value_col = value_col,
          freq_col = freq_col,
          horizon = horizon,
          models = models,
          verbose = FALSE
        )
        fc$series_dims <- series_dims
        fc
      },
      error = function(e) NULL
    )
  }

  # Run forecasts
  if (use_parallel) {
    if (verbose) {
      message("Starting parallel forecasting...")
    }
    forecasts <- parallel::mcmapply(
      forecast_one_series,
      series_list,
      series_dims_list,
      SIMPLIFY = FALSE,
      mc.cores = n_cores
    )
  } else {
    forecasts <- mapply(
      function(dt, dims, i) {
        if (verbose && (i %% 10 == 0 || i == 1)) {
          message("  Forecasting series ", i, "/", n_series)
        }
        forecast_one_series(dt, dims)
      },
      series_list,
      series_dims_list,
      seq_along(series_list),
      SIMPLIFY = FALSE
    )
  }

  names(forecasts) <- series_names

  # Remove NULLs
  forecasts <- forecasts[!sapply(forecasts, is.null)]

  n_success <- length(forecasts)
  if (verbose) {
    message("Successfully forecasted ", n_success, "/", n_series, " series")
  }

  list(
    forecasts = forecasts,
    n_series = n_series,
    n_success = n_success,
    dimension_cols = dim_cols_no_label
  )
}

#' Combine Historical and Forecast Data
#'
#' Merges labeled historical data with forecasts, adding a type column
#' to distinguish between observed and forecasted values.
#'
#' @param labeled_data data.table with labeled historical data
#' @param forecast_results Output from generate_dataset_forecasts()
#' @param value_col Name of value column (default "valore")
#' @param time_col Name of time column (default "tempo")
#'
#' @return data.table with combined historical + forecast data and type column
combine_historical_forecast <- function(
  labeled_data,
  forecast_results,
  value_col = "valore",
  time_col = "tempo"
) {
  if (is.null(labeled_data) || nrow(labeled_data) == 0) {
    warning("No historical data to combine")
    return(labeled_data)
  }

  # Add type column to historical data
  historical <- data.table::copy(labeled_data)
  historical[, type := "historical"]

  # Check if we have forecasts
  if (
    is.null(forecast_results) ||
      length(forecast_results$forecasts) == 0
  ) {
    warning("No forecasts to combine")
    return(historical)
  }

  dim_cols <- forecast_results$dimension_cols

  # Extract forecast data from each series
  forecast_list <- lapply(names(forecast_results$forecasts), function(fc_name) {
    fc <- forecast_results$forecasts[[fc_name]]

    if (is.null(fc) || is.null(fc$best_model)) {
      return(NULL)
    }

    # Get best model forecast data.table
    fc_dt <- data.table::copy(fc$best_model$forecast)

    # Rename columns to match historical
    if ("tempo" %in% names(fc_dt) && time_col != "tempo") {
      data.table::setnames(fc_dt, "tempo", time_col)
    }

    # Value column from forecast is "valore"
    if (value_col != "valore" && "valore" %in% names(fc_dt)) {
      data.table::setnames(fc_dt, "valore", value_col)
    }

    # Add type column
    fc_dt[, type := "forecast"]

    # Add dimension values if available
    if (!is.null(fc$series_dims) && length(dim_cols) > 0) {
      for (col in names(fc$series_dims)) {
        fc_dt[, (col) := fc$series_dims[[col]]]
      }
    }

    fc_dt
  })

  # Remove NULLs and combine
  forecast_list <- forecast_list[!sapply(forecast_list, is.null)]

  if (length(forecast_list) == 0) {
    warning("No valid forecast data to combine")
    return(historical)
  }

  all_forecasts <- data.table::rbindlist(forecast_list, fill = TRUE)

  # Find label columns in historical data (columns ending with _label)
  label_cols <- grep("_label$", names(historical), value = TRUE)

  # If we have label columns and dimension columns, join labels to forecasts
  if (length(label_cols) > 0 && length(dim_cols) > 0) {
    # Get unique dimension-to-label mappings from historical data
    cols_for_lookup <- c(dim_cols, label_cols)
    cols_for_lookup <- intersect(cols_for_lookup, names(historical))

    if (length(cols_for_lookup) > length(dim_cols)) {
      # Create lookup table with unique dimension combinations
      dim_cols_present <- intersect(dim_cols, names(historical))
      if (length(dim_cols_present) > 0) {
        label_lookup <- unique(historical[, ..cols_for_lookup])

        # Join labels to forecasts
        fc_dim_cols <- intersect(dim_cols_present, names(all_forecasts))
        if (length(fc_dim_cols) > 0) {
          all_forecasts <- merge(
            all_forecasts,
            label_lookup,
            by = fc_dim_cols,
            all.x = TRUE
          )
        }
      }
    }
  }

  # Combine historical + forecasts
  combined <- data.table::rbindlist(
    list(historical, all_forecasts),
    use.names = TRUE,
    fill = TRUE
  )

  # Sort by dimensions and time
  sort_cols <- c(dim_cols, time_col)
  sort_cols <- sort_cols[sort_cols %in% names(combined)]
  if (length(sort_cols) > 0) {
    data.table::setorderv(combined, sort_cols)
  }

  combined
}

# 8. Data optimization functions for deployment -----

#' Slim Dataset by Removing NULL and Constant Columns
#'
#' Removes columns that are entirely NA or have only one unique value,
#' reducing dataset size for faster loading.
#'
#' @param dt A data.table to slim down
#' @param keep_always Character vector of column names to never remove
#'
#' @return A slimmed copy of the data.table
slim_dataset <- function(
  dt,
  keep_always = c("FREQ", "dataset_id", "dataset_name")
) {
  dt <- data.table::copy(dt)

  # Remove all-NA columns
  na_cols <- names(dt)[vapply(dt, function(x) all(is.na(x)), logical(1))]
  if (length(na_cols) > 0) {
    dt[, (na_cols) := NULL]
  }

  # Remove constant columns (only one unique value)
  const_cols <- names(dt)[vapply(
    dt,
    function(x) {
      data.table::uniqueN(x, na.rm = TRUE) <= 1
    },
    logical(1)
  )]
  const_cols <- setdiff(const_cols, keep_always)
  if (length(const_cols) > 0) {
    dt[, (const_cols) := NULL]
  }

  dt
}

#' Partition Datasets to Individual qs Files
#'
#' Takes a named list of data.tables and saves each as a separate .qs file
#' with NULL/constant columns removed. Creates an index file with metadata.
#'
#' @param dataset_list Named list of data.tables
#' @param output_dir Directory to save partitioned files
#' @param preset qs compression preset: "fast", "balanced", "high" (default "fast")
#'
#' @return Path to output directory (for targets file format)
partition_datasets <- function(dataset_list, output_dir, preset = "fast") {
  if (!requireNamespace("qs", quietly = TRUE)) {
    stop(
      "Package 'qs' is required for partition_datasets(). Install with: install.packages('qs')"
    )
  }

  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

  index_data <- list()

  for (name in names(dataset_list)) {
    dt <- slim_dataset(dataset_list[[name]])
    file_path <- file.path(output_dir, paste0(name, ".qs"))
    qs::qsave(dt, file_path, preset = preset)

    index_data[[name]] <- list(
      dataset = name,
      rows = nrow(dt),
      columns = names(dt),
      size_kb = round(file.size(file_path) / 1024, 1)
    )
  }

  # Save index with metadata
  qs::qsave(index_data, file.path(output_dir, "index.qs"))

  message(
    "Partitioned ",
    length(dataset_list),
    " datasets to ",
    output_dir,
    " (",
    sum(vapply(index_data, function(x) x$size_kb, numeric(1))),
    " KB total)"
  )

  output_dir
}
