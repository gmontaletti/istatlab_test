# install.packages("rsdmx")
#
library(targets)
devtools::load_all("/Users/giampaolomontaletti/Documents/funzioni/istatlab")
tar_destroy()
1
tar_make()
targets::tar_meta(fields = warnings, complete_only = TRUE)
avvisi <- targets::tar_meta(fields = warnings, complete_only = TRUE)
targets::tar_visnetwork()
tar_objects()
dt <- tar_read(labeled_534_50)

tar_read_raw("labeled_data")
targets::tar_load_everything()
metda <- tar_read("metadata")

devtools::load_all("../../istatlab/", reset = T, export_all = T); library(istatlab)

# devtools::install_github("gmontaletti/istatlab", build_vignettes = TRUE, force = TRUE)


check_istat_api()


# Download metadata to explore available datasets ----
metadata <- download_metadata()
head(metadata)

# test dati ----
codici <- c(
    "150_873"
  , "150_881"
  , "161_267_DF_DCSP_SBSNAZ_11"
  , "613_936"
)

codici <- c("168_756_DF_DCSP_IPCATC1B2015_1")

prezzi <- download_multiple_datasets(codici)
list_prezzi <- download_codelists(codici)

prezzi <- download_multiple_datasets(codici)
mpr <- prezzi$`168_756_DF_DCSP_IPCATC1B2015_1`
processed_data <- apply_labels(mpr, codelists = list_prezzi$X168_756_DF_DCSP_IPCATC1B2015_1)

# Create a time series plot
create_time_series_plot(processed_data,
                        title = "Italian Labour Market Data",
                        subtitle = "Source: ISTAT")
