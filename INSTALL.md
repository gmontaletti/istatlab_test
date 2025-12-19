# Installation Instructions for istatlab_test Workflow

This document provides instructions for installing and running the istatlab_test workflow on a new machine.

## Prerequisites

- R (version 4.0 or higher recommended)
- RStudio (optional but recommended)
- Git

## Repository

The workflow is available at: https://github.com/gmontaletti/istatlab_test

## Installation Steps

### Step 1: Clone the repository

```bash
git clone https://github.com/gmontaletti/istatlab_test.git
cd istatlab_test
```

### Step 2: Open in RStudio (recommended)

Open `istatlab_test.Rproj` in RStudio. This will automatically activate the renv environment.

### Step 3: Restore dependencies

When you open the project, renv will bootstrap automatically. Then run:

```r
# Restore all packages from renv.lock
renv::restore()
```

This installs all dependencies including:

- `istatlab` package from GitHub
- `targets` for workflow management
- All other required packages

### Step 4: Run the workflow

```r
# Check workflow status
targets::tar_visnetwork()

# Run the pipeline
targets::tar_make()
```

## Alternative: Command Line Installation

If you prefer not to use RStudio:

```bash
# Clone
git clone https://github.com/gmontaletti/istatlab_test.git
cd istatlab_test

# Start R and restore dependencies
R -e "renv::restore()"

# Run workflow
R -e "targets::tar_make()"
```

## Troubleshooting

### renv does not activate automatically

If renv doesn't activate when opening the project:

```r
source("renv/activate.R")
renv::restore()
```

### Package installation fails

If a package fails to install, try:

```r
# Update renv itself
install.packages("renv")

# Then restore
renv::restore()
```

### istatlab package issues

If the istatlab package fails to install from the lockfile, install it manually:

```r
remotes::install_github("gmontaletti/istatlab")
```

## Project Structure

```
istatlab_test/
├── _targets.R      # Targets pipeline definition
├── R/              # R functions used in the pipeline
├── renv/           # renv configuration
├── renv.lock       # Locked package versions
├── run.R           # Script to run the workflow
└── doc/            # Output documents
```

## Running the Workflow

After installation, you can run the workflow using:

```r
# Interactive: visualize the pipeline
targets::tar_visnetwork()

# Run the full pipeline
targets::tar_make()

# Check outdated targets
targets::tar_outdated()

# Load a specific target result
targets::tar_read(target_name)
```

## Contact

For issues or questions, contact:

- Author: Giampaolo Montaletti
- Email: giampaolo.montaletti@gmail.com
- GitHub: https://github.com/gmontaletti
