# R/prepare_deployment_data.R
# Script to extract targets data for shinyapps.io deployment
# Author: Giampaolo Montaletti

library(targets)
library(data.table)
library(qs)

# Source helper functions for slim_dataset and partition_datasets
source("R/functions.R")

# 1. Configuration -----
targets_store <- "_targets"
output_dir <- "deploy/data"

# Filter to last 10 years
cutoff_date <- Sys.Date() - (10 * 365.25)

# Helper function to filter data to last 10 years
filter_10_years <- function(dt) {
  if (!is.null(dt) && "tempo" %in% names(dt)) {
    if (!inherits(dt$tempo, "Date")) {
      dt[, tempo := as.Date(tempo)]
    }
    dt <- dt[tempo >= cutoff_date]
  }
  dt
}

# Create output directory
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# 2. Extract Quarterly Dashboard Data -----
message("Extracting quarterly dashboard data...")

quarterly_targets <- c(
  "combined_150_908_Q",
  "combined_150_915_Q",
  "combined_150_916_Q",
  "combined_150_938_Q",
  "combined_151_914_Q",
  "combined_151_929_Q",
  "combined_152_913_Q",
  "combined_152_928_Q",
  "combined_532_930_Q",
  "combined_534_1037_Q",
  "combined_534_1038_Q",
  "combined_154_373_Q"
)

quarterly_list <- lapply(quarterly_targets, function(tgt) {
  tryCatch(
    {
      dt <- tar_read_raw(tgt, store = targets_store)
      filter_10_years(dt)
    },
    error = function(e) {
      message("  Warning: Could not load ", tgt, " - ", e$message)
      NULL
    }
  )
})
names(quarterly_list) <- quarterly_targets

# Remove NULL entries
quarterly_list <- quarterly_list[!sapply(quarterly_list, is.null)]

if (length(quarterly_list) > 0) {
  # Partition to individual qs files (removes NULL columns, faster loading)
  partition_datasets(quarterly_list, file.path(output_dir, "quarterly"))
  # Also save combined RDS for backward compatibility
  saveRDS(quarterly_list, file.path(output_dir, "quarterly_data.rds"))
  message(
    "  Saved: quarterly/ (",
    length(quarterly_list),
    " partitioned datasets)"
  )
} else {
  message("  Warning: No quarterly data found")
}

# 3. Extract Vacancies Dashboard Data -----
message("Extracting vacancies dashboard data...")

vacancies_targets <- c("combined_534_50_M", "combined_534_49_M")

vacancies_list <- lapply(vacancies_targets, function(tgt) {
  tryCatch(
    {
      dt <- tar_read_raw(tgt, store = targets_store)
      filter_10_years(dt)
    },
    error = function(e) {
      message("  Warning: Could not load ", tgt, " - ", e$message)
      NULL
    }
  )
})
names(vacancies_list) <- vacancies_targets

# Remove NULL entries
vacancies_list <- vacancies_list[!sapply(vacancies_list, is.null)]

if (length(vacancies_list) > 0) {
  # Partition to individual qs files
  partition_datasets(vacancies_list, file.path(output_dir, "vacancies"))
  # Also save combined RDS for backward compatibility
  saveRDS(vacancies_list, file.path(output_dir, "vacancies_data.rds"))
  message(
    "  Saved: vacancies/ (",
    length(vacancies_list),
    " partitioned datasets)"
  )
} else {
  message("  Warning: No vacancies data found")
}

# 4. Extract Wages Dashboard Data -----
message("Extracting wages dashboard data...")

wages_targets <- c("combined_149_319_M", "combined_155_318_M")

wages_list <- lapply(wages_targets, function(tgt) {
  tryCatch(
    {
      dt <- tar_read_raw(tgt, store = targets_store)
      filter_10_years(dt)
    },
    error = function(e) {
      message("  Warning: Could not load ", tgt, " - ", e$message)
      NULL
    }
  )
})
names(wages_list) <- wages_targets

# Remove NULL entries
wages_list <- wages_list[!sapply(wages_list, is.null)]

if (length(wages_list) > 0) {
  # Partition to individual qs files
  partition_datasets(wages_list, file.path(output_dir, "wages"))
  # Also save combined RDS for backward compatibility
  saveRDS(wages_list, file.path(output_dir, "wages_data.rds"))
  message("  Saved: wages/ (", length(wages_list), " partitioned datasets)")
} else {
  message("  Warning: No wages data found")
}

# 5. Summary -----
message("\n", strrep("=", 50))
message("Data extraction complete!")

# List RDS files (backward compatibility)
rds_files <- list.files(output_dir, pattern = "\\.rds$", full.names = TRUE)
# List qs files (optimized)
qs_files <- list.files(
  output_dir,
  pattern = "\\.qs$",
  full.names = TRUE,
  recursive = TRUE
)

if (length(rds_files) > 0 || length(qs_files) > 0) {
  message("\nLegacy RDS files (backward compatibility):")
  if (length(rds_files) > 0) {
    rds_sizes <- file.info(rds_files)$size
    for (i in seq_along(rds_files)) {
      message(
        "  ",
        basename(rds_files[i]),
        " (",
        round(rds_sizes[i] / 1024 / 1024, 2),
        " MB)"
      )
    }
    message("  Total RDS: ", round(sum(rds_sizes) / 1024 / 1024, 2), " MB")
  }

  message("\nOptimized qs files (for lazy loading):")
  if (length(qs_files) > 0) {
    qs_sizes <- file.info(qs_files)$size
    for (i in seq_along(qs_files)) {
      rel_path <- sub(paste0(output_dir, "/"), "", qs_files[i])
      message("  ", rel_path, " (", round(qs_sizes[i] / 1024, 1), " KB)")
    }
    message("  Total qs: ", round(sum(qs_sizes) / 1024, 1), " KB")
  }
} else {
  message("No files were created. Make sure targets have been built.")
}
