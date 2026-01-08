# R/update_docker_data.R
# Script to extract targets data for Docker deployment
# Author: Giampaolo Montaletti <giampaolo.montaletti@gmail.com>
#
# Usage: Rscript R/update_docker_data.R
#
# This script extracts data from the targets store and saves it
# to both deploy/data/ (for shinyapps.io) and deploy/docker/data/
# (for Docker deployment).

library(targets)
library(data.table)

# 1. Configuration -----
targets_store <- "_targets"
output_dirs <- c("deploy/data", "deploy/docker/data")

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

# Create output directories
for (dir in output_dirs) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
    message("Created directory: ", dir)
  }
}

# 2. Helper function -----
save_to_all_dirs <- function(data, filename, dirs) {
  for (dir in dirs) {
    filepath <- file.path(dir, filename)
    saveRDS(data, filepath)
  }
}

# 3. Extract Quarterly Dashboard Data -----
message("\n", strrep("=", 50))
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
  save_to_all_dirs(quarterly_list, "quarterly_data.rds", output_dirs)
  message("  Saved: quarterly_data.rds (", length(quarterly_list), " datasets)")
} else {
  message("  Warning: No quarterly data found")
}

# 4. Extract Vacancies Dashboard Data -----
message("\nExtracting vacancies dashboard data...")

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
  save_to_all_dirs(vacancies_list, "vacancies_data.rds", output_dirs)
  message("  Saved: vacancies_data.rds (", length(vacancies_list), " datasets)")
} else {
  message("  Warning: No vacancies data found")
}

# 5. Extract Wages Dashboard Data -----
message("\nExtracting wages dashboard data...")

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
  save_to_all_dirs(wages_list, "wages_data.rds", output_dirs)
  message("  Saved: wages_data.rds (", length(wages_list), " datasets)")
} else {
  message("  Warning: No wages data found")
}

# 6. Summary -----
message("\n", strrep("=", 50))
message("Data extraction complete!")
message(strrep("=", 50))

for (dir in output_dirs) {
  message("\nDirectory: ", dir)
  files <- list.files(dir, pattern = "\\.rds$", full.names = TRUE)
  if (length(files) > 0) {
    sizes <- file.info(files)$size
    message("  Files: ", length(files))
    message("  Total size: ", round(sum(sizes) / 1024 / 1024, 2), " MB")
    for (i in seq_along(files)) {
      message(
        "    - ",
        basename(files[i]),
        " (",
        round(sizes[i] / 1024 / 1024, 2),
        " MB)"
      )
    }
  } else {
    message("  No files found")
  }
}

message("\nDocker deployment data is ready.")
message("Run the following to restart the container:")
message("  cd deploy/docker && docker-compose restart shiny-app")
