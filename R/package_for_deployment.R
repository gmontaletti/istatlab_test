# R/package_for_deployment.R
# Complete packaging script for shinyapps.io deployment
# Author: Giampaolo Montaletti
#
# Usage:
#   source("R/package_for_deployment.R")
#
# This script:
# 1. Extracts data from targets store
# 2. Verifies the unified dashboard exists
# 3. Reports deployment readiness

library(data.table)

# 1. Configuration -----
project_root <- getwd()
deploy_dir <- file.path(project_root, "deploy")
data_dir <- file.path(deploy_dir, "data")

message("\n", strrep("=", 60))
message("ISTAT Dashboard Deployment Packaging")
message(strrep("=", 60))

# 2. Verify deploy directory structure -----
message("\n[1/4] Checking directory structure...")

if (!dir.exists(deploy_dir)) {
  dir.create(deploy_dir, recursive = TRUE)
  message("  Created: deploy/")
}

if (!dir.exists(data_dir)) {
  dir.create(data_dir, recursive = TRUE)
  message("  Created: deploy/data/")
}

# 3. Extract data from targets -----
message("\n[2/4] Extracting data from targets store...")

data_script <- file.path(project_root, "R", "prepare_deployment_data.R")
if (file.exists(data_script)) {
  source(data_script)
} else {
  stop("Data extraction script not found: ", data_script)
}

# 4. Verify required files -----
message("\n[3/4] Verifying deployment files...")

required_files <- c(
  file.path(deploy_dir, "app.Rmd"),
  file.path(deploy_dir, "DESCRIPTION"),
  file.path(data_dir, "quarterly_data.rds"),
  file.path(data_dir, "vacancies_data.rds"),
  file.path(data_dir, "wages_data.rds")
)

missing_files <- required_files[!file.exists(required_files)]

if (length(missing_files) > 0) {
  message("  WARNING: Missing files:")
  for (f in missing_files) {
    message("    - ", basename(f))
  }
} else {
  message("  All required files present")
}

# 5. Summary -----
message("\n[4/4] Deployment summary...")

all_files <- list.files(deploy_dir, recursive = TRUE, full.names = TRUE)
total_size <- sum(file.info(all_files)$size, na.rm = TRUE) / 1024 / 1024

message("\n  Deployment directory: ", deploy_dir)
message("  Total files: ", length(all_files))
message("  Total size: ", round(total_size, 2), " MB")

message("\n  Files:")
for (f in list.files(deploy_dir, recursive = TRUE)) {
  fpath <- file.path(deploy_dir, f)
  fsize <- file.info(fpath)$size / 1024
  if (fsize > 1024) {
    message("    ", f, " (", round(fsize / 1024, 2), " MB)")
  } else {
    message("    ", f, " (", round(fsize, 1), " KB)")
  }
}

message("\n", strrep("=", 60))
message("DEPLOYMENT READY")
message(strrep("=", 60))

message("\nTo deploy to shinyapps.io:")
message("\n  # One-time account setup")
message("  rsconnect::setAccountInfo(")
message("    name = 'YOUR_ACCOUNT',")
message("    token = 'YOUR_TOKEN',")
message("    secret = 'YOUR_SECRET'")
message("  )")
message("\n  # Deploy")
message("  rsconnect::deployApp(")
message("    appDir = '", deploy_dir, "',")
message("    appName = 'istat-dashboard',")
message("    appTitle = 'ISTAT Dashboard'")
message("  )")

message("\nTo test locally:")
message("  rmarkdown::run('", file.path(deploy_dir, "app.Rmd"), "')")
message("")
