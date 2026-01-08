# ISTAT Dashboard Docker Container
# Maintainer: Giampaolo Montaletti <giampaolo.montaletti@gmail.com>

FROM rocker/shiny:4.4.0

LABEL maintainer="Giampaolo Montaletti <giampaolo.montaletti@gmail.com>"
LABEL description="ISTAT Statistical Dashboard - Interactive exploration of Italian labor statistics"
LABEL version="1.0.0"

# 1. Install system dependencies -----
RUN apt-get update && apt-get install -y --no-install-recommends \
    libcurl4-openssl-dev \
    libssl-dev \
    libxml2-dev \
    libfontconfig1-dev \
    libfreetype6-dev \
    libpng-dev \
    libtiff5-dev \
    libjpeg-dev \
    libharfbuzz-dev \
    libfribidi-dev \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Install R packages -----
RUN R -e "install.packages(c( \
    'data.table', \
    'scales', \
    'ggplot2', \
    'plotly', \
    'shiny', \
    'shinyWidgets', \
    'flexdashboard', \
    'rmarkdown', \
    'bslib', \
    'qs' \
), repos='https://cloud.r-project.org/', Ncpus=4)"

# 3. Configure Shiny Server -----
COPY shiny-server.conf /etc/shiny-server/shiny-server.conf

# 4. Create application directory -----
RUN mkdir -p /srv/shiny-server/istat-dashboard/data/quarterly \
    && mkdir -p /srv/shiny-server/istat-dashboard/data/vacancies \
    && mkdir -p /srv/shiny-server/istat-dashboard/data/wages

# 5. Copy application files -----
COPY app/app.Rmd /srv/shiny-server/istat-dashboard/

# Copy optimized qs files (lazy loading, faster startup)
COPY app/data/quarterly/*.qs /srv/shiny-server/istat-dashboard/data/quarterly/
COPY app/data/vacancies/*.qs /srv/shiny-server/istat-dashboard/data/vacancies/
COPY app/data/wages/*.qs /srv/shiny-server/istat-dashboard/data/wages/

# Copy legacy RDS files (backward compatibility)
COPY app/data/*.rds /srv/shiny-server/istat-dashboard/data/

# 6. Set permissions -----
RUN chown -R shiny:shiny /srv/shiny-server/istat-dashboard

# 7. Expose port -----
EXPOSE 3838

# 8. Health check -----
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:3838/istat-dashboard/ || exit 1

# 9. Start Shiny Server -----
CMD ["/usr/bin/shiny-server"]
