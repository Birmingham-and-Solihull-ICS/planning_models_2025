
library(data.table)
library(dplyr)

base_dir <- "output/activity_plans"

csv_files <- list.files(path = base_dir, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)

# Read all as a list
dt_list <- lapply(csv_files, fread)

# Bind and add id column for the list index
combined_dt <- rbindlist(dt_list, use.names = TRUE, fill = TRUE, idcol = "file_id")

# Build a lookup of file metadata
file_meta <- data.table(
    file_id       = seq_along(csv_files),
    source_folder = basename(dirname(csv_files))
)

# Add metadata to combined data
combined_dt <- file_meta[combined_dt, on = "file_id"]


future_dt <-
    combined_dt |>
    filter(as.Date(end_date) > as.Date("31/08/2025", "%d/%m/%Y"))



future_dt <-
    future_dt |>
    mutate(weeks = as.numeric(difftime(end_date, start_date, units = "days"))/7)



out <-
    future_dt |>
    mutate(capacity_for_period = ceiling(calc_capacity * weeks),
           modelled_demand_for_period = round(Referrals * weeks) )|>
    select(source_folder, Specialty, start_date, end_date,
           capacity_for_period, modelled_demand_for_period,
           Waiting.list.size_relief, target_wl, target)


fwrite(out, "output/activity_plans/model_wl_output.csv")
