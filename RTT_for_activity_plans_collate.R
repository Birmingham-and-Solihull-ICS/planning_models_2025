
library(data.table)
library(dplyr)

base_dir <- "output/activity_plans"

csv_files <- list.files(path = base_dir, pattern = "\\.csv$", recursive = TRUE, full.names = TRUE)
csv_files <- csv_files[1:10]

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
    mutate(capacity_relief = ceiling(calc_capacity * weeks),
           capacity_do_nothing = ceiling(Removals * weeks),
           predicted_demand = round(Referrals * weeks) ,
           ICB = ifelse(substr(source_folder, 1, 2) == "BC", "BC", "BSOL"),
           Provider = sub("^[^_]*_", "", source_folder) ) |>
    mutate(capacity_difference = capacity_relief - capacity_do_nothing,
           start_date = as.Date(start_date),
           end_date = as.Date(end_date)) |>

    select(ICB, Provider, Specialty, start_date, end_date,
           `Demand (predicted)` = predicted_demand,
           `Capacity (do nothing)` = capacity_do_nothing,
           `Waiting list size (do nothing)` = Waiting.list.size,
           `Waiting list compliance (do nothing)` = wl_performance_cur,
           `Capacity (relief)` = capacity_relief,
           `Waiting list size (relief)` = Waiting.list.size_relief,
           `Target (sustainable) WL size` = target_wl,
           `Target compliance with 18 weeks` = target,
           `Waiting list compliance (relief)` = wl_performance_rel,
           `Capacity difference` = capacity_difference)



fwrite(out, "output/activity_plans/model_wl_output_v3_6.csv")

