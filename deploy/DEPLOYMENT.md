# ISTAT Dashboard - Deployment Instructions

## Overview

This folder contains a deployment-ready Shiny application for shinyapps.io. The dashboard provides interactive exploration of ISTAT statistical data with three integrated pages.

## Package Contents

```
deploy/
├── app.Rmd               # Unified multi-page dashboard
├── DESCRIPTION           # Package metadata for shinyapps.io
├── DEPLOYMENT.md         # This file
└── data/
    ├── quarterly_data.rds    # 12 quarterly datasets (16.7 MB)
    ├── vacancies_data.rds    # 2 monthly datasets (1.3 MB)
    └── wages_data.rds        # 2 monthly datasets (1.9 MB)
```

Total size: ~20 MB

## Dashboard Pages

The application includes three pages accessible via the "Dashboard" dropdown menu:

| Page | Description | Data |
|------|-------------|------|
| Dati Trimestrali | Quarterly labor force indicators | 12 datasets |
| Occupazione (Base 2021) | Monthly employment data | 2 datasets |
| Retribuzioni (Base 2021) | Monthly wages and contractual tension | 2 datasets |

## Deployment Steps

### 1. Test Locally

Before deploying, verify the dashboard works correctly:

```r
rmarkdown::run("deploy/app.Rmd")
```

### 2. Set Up shinyapps.io Account

If you don't have an account:
1. Go to https://www.shinyapps.io and create an account
2. Navigate to Account > Tokens
3. Click "Show" to reveal your token and secret

Configure rsconnect (one-time setup):

```r
# Install rsconnect if needed
install.packages("rsconnect")

# Configure account
rsconnect::setAccountInfo(
  name = "YOUR_ACCOUNT_NAME",
  token = "YOUR_TOKEN",
  secret = "YOUR_SECRET"
)
```

### 3. Deploy to shinyapps.io

```r
rsconnect::deployApp(
  appDir = "deploy",
  appName = "istat-dashboard",
  appTitle = "ISTAT Dashboard"
)
```

The dashboard will be available at:
`https://YOUR_ACCOUNT_NAME.shinyapps.io/istat-dashboard/`

### 4. Update Deployment

To update the dashboard with new data:

```r
# Re-extract data from targets
source("R/prepare_deployment_data.R")

# Redeploy
rsconnect::deployApp(
  appDir = "deploy",
  appName = "istat-dashboard",
  forceUpdate = TRUE
)
```

## Updating Data

To refresh the data snapshots after running the targets pipeline:

```r
# From project root
source("R/prepare_deployment_data.R")
```

Or use the complete packaging script:

```r
source("R/package_for_deployment.R")
```

## shinyapps.io Tier Considerations

| Tier | RAM | Apps | Hours/Month | Cost |
|------|-----|------|-------------|------|
| Free | 1 GB | 5 | 25 | $0 |
| Starter | 1 GB | 25 | 100 | ~$13/month |
| Basic | 8 GB | 100 | 500 | ~$49/month |

The dashboard (~20 MB data) should work on the Free tier for testing. For production use, consider the Starter tier for more active hours.

## Troubleshooting

### Dashboard doesn't load
- Verify all data files exist in `deploy/data/`
- Check that `app.Rmd` references correct file paths

### Memory errors on shinyapps.io
- The Free tier has 1 GB RAM limit
- Consider upgrading to Basic tier if memory issues occur

### PDF export fails
- shinyapps.io may not have Cairo installed
- The dashboard falls back to standard PDF device

### Data appears outdated
- Re-run `source("R/prepare_deployment_data.R")`
- Redeploy with `forceUpdate = TRUE`

## Author

Giampaolo Montaletti
Email: giampaolo.montaletti@gmail.com
GitHub: https://github.com/gmontaletti
