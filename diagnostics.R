library(tidyverse)
library(fuzzyjoin)
library(NHSRwaitinglist)
library(scales)
library(ggtext)
library(zoo)
source("monte_carlo_func.R")
source("utils.R")
library(purrr)
library(furrr)
library(future.apply)


# set ggplot theme
theme_set(
    theme_minimal() +
        theme(plot.title = element_text(size = 12),
              plot.subtitle = element_text(size = 9, face = "italic")
        )
)



######################## Targets & population growth ##############

#################
# 86% at 6 weeks 2026/27
# 80 25/26, 86 26/27, 93 27/28, 99 29/30
#################

last_data_date <- as.Date('01/09/2025', '%d/%m/%Y')

target_dts <-
    data.frame(
        startdate = as.Date(c('17/11/2025', '01/04/2026', '01/04/2027'
                              , '01/04/2028','01/04/2029','01/04/2030'), '%d/%m/%Y'),
        enddate = as.Date(c('31/03/2026', '31/03/2027', '31/03/2028'
                            , '31/03/2029', '31/03/2030', '31/03/2031'), '%d/%m/%Y'),
        value = c(0.8, 0.86, 0.93, 0.99, 0.99, 0.99),
        descr = c("diag80%", "diag86%", "diag93%", "diag99%", "diag99%", "diag99%")
    )

population_growth <-
    tibble::tribble(
        ~start_date,    ~end_date, ~ratio_increase, ~population,
        "17/11/2025", "31/03/2026",               1,     1590793,
        "01/04/2026", "31/03/2027",         1.00271, 1596056.425,
        "01/04/2027", "31/03/2028",        1.005589, 1601503.998,
        "01/04/2028", "31/03/2029",        1.009093, 1607815.611,
        "01/04/2029", "31/03/2030",        1.013369, 1615161.207,
        "01/04/2030", "31/03/2031",        1.017844, 1622704.257
    )

# 62 days is
62/7
8.857143



# Convert Year to Date
population_growth$start_date <- as.Date(population_growth$start_date, '%d/%m/%Y')
population_growth$end_date <- as.Date(population_growth$end_date, '%d/%m/%Y')



############################
# Load DIDs data
#############################

library(DBI)
con <- dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "MLCSU-BI-SQL",
                 Database = "EAT_Reporting_BSOL", Trusted_Connection = "True")

#sql2 <- read_file("./resident_dids_data.sql")

sql <-
"Select Descriptor,
DATEADD(day,-6, WeekEndingDate) WeekStartingDate,
WeekEndingDate,
sum(case when DATEDIFF(day, [ClockStartDate], WeekEndingDate) <=7 THEN 1 ELSE 0 END) as Referrals,
NULL as Removals,
count(*) as [Waiting.list.size]
from
[EAT_Reporting_BSOL].[DEVELOPMENT].[BSOL0057_Data_Resident] T1
LEFT JOIN [EAT_Reporting_BSOL].[Development].[BSOL0057_RefDiagMod] T2
ON Modality = t2.Code
Where WeekEndingDate >= {d '2024-04-01'} and Descriptor is not NULL
group by Descriptor,
WeekEndingDate
order by Descriptor, WeekEndingDate"

#dt_load <- dbGetQuery(con, sql2)

# write.csv(dt_load, "./data/dids_load.csv", row.names = FALSE)

library(readxl)
dt_load <- read_xlsx("./data/dids_load.xlsx", .name_repair = "universal_quiet" )


# Clear any float/double number formatting, as it trips rpois over.
dt_load$start_date <- as.Date(dt_load$start_date)
dt_load$end_date <- as.Date(dt_load$end_date)
dt_load$Referrals <- as.integer(dt_load$Referrals)
dt_load$Removals <- as.integer(dt_load$Removals)
dt_load$Waiting.list.size <- as.integer(dt_load$Waiting.list.size)



##################Prepare data###########################

############### Convert to list of data.frames per specialty#####################
# Split data frame by Specialty, into a list.
# Each slot in the list is a data.frame for a specialty
df_list <- dt_load |>  #test_input %>%
    mutate(ratio_increase = as.numeric(NA)) |>
    group_by(Descriptor) %>%
    group_split()

print(df_list[[1]], n = 92)

#write.csv(a, "check_removals.csv")

# # Fix the first value to median  -  Not required after CM calcualtion update.
# df_list <- map(df_list, function(.x) {
#     med <- as.integer(median(.x$Waiting.list.size, na.rm = TRUE))
#     .x$Waiting.list.size[1] <- med
#     .x
# })

# Chop last date off: 17/11/2025 - 23/11/2025
df_list <- map(df_list, function(.x) {
    .x <- .x[.x$start_date < as.Date("17/11/2025", "%d/%m/%Y"),]
    .x
})
.x <- df_list[[1]]
# Recalculate WL total, as SQL calc was not right
df_list <- map(df_list, function(.x) {
    sub <- .x[1,]
    .x$Waiting.list.size  <- data.table::shift(.x$Waiting.list.size, 1, type = "lag") + .x$Referrals - .x$Removals

    rbind(sub, .x[2:nrow(.x),])
})

df_list[[1]]

last_data_row <- nrow(df_list[[1]])

# Calculate coefficients of variation (how each list behaves)
# cv_demand and cv_capacity for each specialty
tf_summary <- map_dfr(df_list, function(.x) {
    #.x <- filter(.x, start_date < as.Date("2025-10-01"))
    cv_demand  <- sd(.x$Referrals, na.rm = TRUE) / mean(.x$Referrals, na.rm = TRUE)
    cv_capacity <- sd(.x$Removals, na.rm = TRUE) / mean(.x$Removals, na.rm = TRUE)

    tibble(
        Descriptor = first(.x$Descriptor),
        cv_demand = cv_demand,
        cv_capacity = cv_capacity

    )
})



# Append rows for population growth periods
df_append <- data.frame(Descriptor = NA,
                        start_date = as.Date(population_growth$start_date, '%d/%m/%Y'),
                        end_date = as.Date(population_growth$end_date, '%d/%m/%Y'),
                        Referrals = NA, Removals = NA, Waiting.list.size = NA,
                        ratio_increase = population_growth$ratio_increase)


# Apply to data.frames in list.
df_list <- map(df_list, ~ rbind(.x, df_append))

print(df_list[[1]], n = 92)

#### Populate values logically ####
######## Populate values ####

# fill in values, Descriptor
df_list <- map(df_list, function(.x) {
    dscr <- first(.x$Descriptor)
    .x$Descriptor[is.na(.x$Descriptor)] <- dscr
    .x
})
#.x <- df_list[[1]]
# median capacity for last 3 months fiscal year only
df_list <- map(df_list, function(.x) {
    sub <- filter(.x, start_date > as.Date("2025-08-31"))
    med <- as.integer(median(sub$Removals, na.rm = TRUE))
    .x$Removals[is.na(.x$Removals)] <- med
    .x
})


# Fill in referrals based on last known value and growth ratios
df_list <- map(df_list, function(.x) {
    .x %>%
        mutate(
            Referrals = {
                last_val <- last(Referrals[!is.na(Referrals)])
                new_vals <- Referrals
                for (i in seq_along(new_vals)) {
                    if (is.na(new_vals[i])) {
                        if (i == 1 || !is.na(new_vals[i - 1])) {
                            new_vals[i] <- last_val * ratio_increase[i]
                        } else {
                            new_vals[i] <- new_vals[i - 1] * ratio_increase[i]
                        }
                    }
                }
                as.integer(new_vals)
            }
        )
})



print(df_list[[1]], n = 92)
print(df_list[[2]], n = 92)



#### MC to end of 2025 ####
#Then can use to calculate target waiting lists

# Parallel plan
plan(multisession, workers = 5)

#library(future.mirai)
#plan(mirai_multisession, workers = 6)

# Your simulation function (unchanged)
df <- df_list[[1]]
#rm(df)
sim_func <- function(df) {
    current_wl <- data.frame(
        Referral = rep(as.Date(df$start_date[last_data_row])
                       , df$Waiting.list.size[last_data_row]),
        Removal = rep(as.Date(NA), df$Waiting.list.size[last_data_row])
    )

    sim <- wl_simulator_cpp(
        start_date = as.Date(df[last_data_row+1,]$start_date),
        end_date = as.Date(df[last_data_row+1,]$end_date),
        demand = df[last_data_row+1,]$Referrals,
        capacity = df[last_data_row+1,]$Removals, # project last point forward
        waiting_list = current_wl
    )

    data.frame(Descriptor = df$Descriptor[1], queue = tail(wl_queue_size(sim)[, 2],1),
               mean_wait = wl_stats(sim)$mean_wait)
}

# Apply to each specialty in df_list
results <- map(df_list, function(df) {
    # Run 50 simulations for this specialty
    sims <- future_replicate(20, sim_func(df), simplify = FALSE)
    # Combine into one data frame
    bind_rows(sims)
})

plan(sequential)

#saveRDs(results, "./data/results_didsinput2025.rds")

# Combine all specialties into one data frame
all_results <- bind_rows(results)

# Summarize mean and median queue per specialty
summary_results <- all_results %>%
    group_by(Descriptor) %>%
    summarise(
        mean_queue = mean(queue, na.rm = TRUE),
        median_queue = median(queue, na.rm = TRUE),
        .groups = "drop"
    )

summary_results

print(df_list[[1]], n = 92) # 15458 110
print(df_list[[2]], n = 92) # 163286 99999


# Update df_list last row plus 1 to get to end of 2025/26row 19 with median_queue
df_list <- map(df_list, function(df) {
    dscr <- df$Descriptor[1]
    median_val <- round(summary_results$median_queue[summary_results$Descriptor == dscr])
    df$Waiting.list.size[last_data_row+1] <- median_val
    df
})




# # add lagged 1 column for waiting list at start of period
# df_list <- map(df_list, ~ mutate(.x, Wl_start_lag = lag(Waiting.list.size, n = 1)))
# # Imput 1
#
#
# df_list <- map(df_list, function(.x) {
#     .x$Wl_start_lag[1] <- .x$Wl_start_lag[2] + .x$Referrals[1] - .x$Removals[2]
#     .x
# })



########## Sustainable WL calcs #####################################

# Add targets into data.frames in list
df_list <- map(df_list, function(.x) {

    .x <- fuzzy_left_join(.x, target_dts, by = c("start_date" = "startdate"
                                                 , "end_date" = "enddate")
                          , match_fun = list(`>=`, `<=`)) |>
        select(-startdate,-enddate,-descr) |>
        rename(target = value)
    .x
})

print(df_list[[1]], n = 92) # 15458 110
print(df_list[[2]], n = 92) # 163286 99999

# run NHSR waiting list functions over each data.frame in list (Descriptor)
df_list <- map(df_list, function(.x) {
    .x$target_wl <- calc_target_queue_size(
        demand = .x$Referrals,
        target_wait = 6,
        factor = qexp(.x$target)
    )

    dscr <- first(.x$Descriptor)

    cv_demand <- tf_summary %>% filter(Descriptor == dscr) %>% pull(cv_demand)
    cv_capacity <- tf_summary %>% filter(Descriptor == dscr) %>% pull(cv_capacity)

    # Assuming we meet target list size in each period
    .x$Waiting.list.size <- coalesce(.x$Waiting.list.size, .x$target_wl)


    .x$target_capacity <-
        calc_target_capacity(
            demand = .x$Referrals,
            target_wait = 6,
            factor = qexp(.x$target),
            cv_demand = cv_demand,
            cv_capacity = cv_capacity)

    .x$relief_capacity <-
        calc_relief_capacity(
            demand = .x$Referrals,
            queue_size = .x$Waiting.list.size,
            target_queue_size = .x$target_wl,
            time_to_target = 52,
            cv_demand = cv_demand
        )




    .x
})

print(df_list[[1]], n = 92)
print(df_list[[2]], n = 50)


write.csv(df_list[[1]], "audiology_check.csv")



########
# Needed to sim waiting list out as at present
###########


plan(multisession, workers = 5)  # Adjust workers to your CPU cores

#plan(mirai_multisession, workers = 6)
start_time <- Sys.time()

sim_results <- map(df_list, function(df) {
    #Rcpp::sourceCpp("wl_simulator.cpp")
    # Extract starting_wl from first row
    start_wl <- df[1, "Waiting.list.size", drop = TRUE]
    if (is.na(start_wl)) start_wl <- 0

    # Inner parallel map (optional)
    future_map(1:25, function(i) {
        #Rcpp::sourceCpp("wl_simulator.cpp")
        bsol_montecarlo_WL2(
            .data = df,
            run_id = i,
            start_date_name = "start_date",
            end_date_name = "end_date",
            adds_name = "Referrals",
            removes_name = "Removals",
            starting_wl = start_wl
        )
    }, .options = furrr_options(seed = NULL, globals = TRUE))
}
)

end_time <- Sys.time()



i = 87
sim_func <- function(df) {
    current_wl <- data.frame(
        Referral = rep(as.Date(df$start_date[last_data_row])
                       , df$Waiting.list.size[last_data_row]),
        Removal = rep(as.Date(NA), df$Waiting.list.size[last_data_row])
    )

    sim <- wl_simulator_cpp(
        start_date = as.Date(df[last_data_row+1,]$start_date),
        end_date = as.Date(df[last_data_row+1,]$end_date),
        demand = df[87,]$Referrals,
        capacity = df[last_data_row+1,]$Removals, # project last point forward
        waiting_list = current_wl
    )

    data.frame(Descriptor = df$Descriptor[1], queue = tail(wl_queue_size(sim)[, 2],1),
               mean_wait = wl_stats(sim)$mean_wait)
}

# Apply to each specialty in df_list
results2 <- map(df_list, function(df) {
    # Run 50 simulations for this specialty
    sims <- future_replicate(20, sim_func(df), simplify = FALSE)
    # Combine into one data frame
    bind_rows(sims)
})