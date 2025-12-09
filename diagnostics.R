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
        "10/11/2025", "31/03/2026",               1,     1590793,
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
dt_load <- read_xlsx("./data/dids_load.xlsx", .name_repair = "universal_quiet" , sheet = "Sheet 1")
dt_load <- read_xlsx("./data/dids_quarterly.xlsx", .name_repair = "universal_quiet" )


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

df_list <- map(df_list, function(.x) {
    sub <- .x[1,]
    .x$Waiting.list.size  <- data.table::shift(.x$Waiting.list.size, 1, type = "lag") + .x$Referrals - .x$Removals

    rbind(sub, .x[2:nrow(.x),])
})




# 1) Summarise each input df to 3-month groups
summarised_each[[1]] <- purrr::map(df_list, summarise_to_3m, start_month = 1)  # set to 4 for NHS fiscal quarters





# # Fix the first value to median  -  Not required after CM calcualtion update.
# df_list <- map(df_list, function(.x) {
#     med <- as.integer(median(.x$Waiting.list.size, na.rm = TRUE))
#     .x$Waiting.list.size[1] <- med
#     .x
# })

# Chop last date off from November: 17/11/2025 - 23/11/2025
# df_list <- map(df_list, function(.x) {
#     .x <- .x[.x$start_date < as.Date("10/11/2025", "%d/%m/%Y"),]
#     .x
# })
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


# Add targets into data.frames in list
df_list <- map(df_list, function(.x) {

    .x <- fuzzy_left_join(.x, target_dts, by = c("start_date" = "startdate"
                                                 , "end_date" = "enddate")
                          , match_fun = list(`>=`, `<=`)) |>
        select(-startdate,-enddate,-descr) |>
        rename(target = value)
    .x
})


print(df_list[[1]], n = 92)
print(df_list[[2]], n = 92)



#### MC to end of 2025 ####
#Then can use to calculate target waiting lists

# Parallel plan
#plan(multisession, workers = 5)

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
summary_results <-
    all_results %>%
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


write.csv(df_list[[1]], "audiology_check1.csv")



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

###################################
# Need to loop this now to build WL, then calculate targets, and sim next
i <- 86L
df <- df_list[[1]]
for (i in (last_data_row + 1):nrow(df_list[[1]])) {

    sim_func_it <- function(df) {
        current_wl <- data.frame(
            Referral = rep(as.Date(df$start_date[i-1])
                           , df$Waiting.list.size[i-1]),
            Removal = rep(as.Date(NA), df$Waiting.list.size[i-1])
        )

        sim <- wl_simulator_cpp(
            start_date = as.Date(df[i,]$start_date),
            end_date = as.Date(df[i,]$end_date),
            demand = df[i,]$Referrals,
            capacity = df[i,]$Removals, # project last point forward
            waiting_list = current_wl
        )

        data.frame(Descriptor = df$Descriptor[1], queue = tail(wl_queue_size(sim)[, 2],1),
                   mean_wait = wl_stats(sim)$mean_wait)
    }

    # Apply to each specialty in df_list
    results_it <- map(df_list, function(df) {
        # Run 50 simulations for this specialty
        sims <- future_replicate(20, sim_func(df), simplify = FALSE)
        # Combine into one data frame
        bind_rows(sims)
    })

    #plan(sequential)

    #saveRDs(results, "./data/results_didsinput2025.rds")

    # Combine all specialties into one data frame
    all_results_it <- bind_rows(results_it)

    # Summarize mean and median queue per Descriptor
    summary_results_it <- all_results_it %>%
        group_by(Descriptor) %>%
        summarise(
            mean_queue = mean(queue, na.rm = TRUE),
            median_queue = median(queue, na.rm = TRUE),
            .groups = "drop"
    )

    #summary_results

    #print(df_list[[1]], n = 92) # 15458 110
    #print(df_list[[2]], n = 92) # 163286 99999


    # Update df_list last row plus 1 to get to end of 2025/26row 19 with median_queue
    df_list <- map(df_list, function(df) {
        dscr <- df$Descriptor[1]
        median_val <- round(summary_results_it$median_queue[summary_results_it$Descriptor == dscr])
        df$Waiting.list.size[i] <- median_val
        df
    })

# run NHSR waiting list functions over each data.frame in list (Descriptor)
df_list <- map(df_list, function(.x) {
    .x$target_wl[i] <- calc_target_queue_size(
        demand = .x$Referrals[i],
        target_wait = 6,
        factor = qexp(.x$target[i])
    )

    dscr <- first(.x$Descriptor)

    cv_demand <- tf_summary %>% filter(Descriptor == dscr) %>% pull(cv_demand)
    cv_capacity <- tf_summary %>% filter(Descriptor == dscr) %>% pull(cv_capacity)

    # Assuming we meet target list size in each period
    #.x$Waiting.list.size <- coalesce(.x$Waiting.list.size, .x$target_wl)


    .x$target_capacity[i] <-
        calc_target_capacity(
            demand = .x$Referrals[i],
            target_wait = 6,
            factor = qexp(.x$target[i]),
            cv_demand = cv_demand,
            cv_capacity = cv_capacity)

    .x$relief_capacity[i] <-
        calc_relief_capacity(
            demand = .x$Referrals[i],
            queue_size = .x$Waiting.list.size[i],
            target_queue_size = .x$target_wl[i],
            time_to_target = 52,
            cv_demand = cv_demand
        )



    .x
})


}


.x <- df_list[[1]]

# Need to loop this now to build WL, then calculate targets, and sim next
i <- 86L
df <- df_list[[1]]
for (i in (last_data_row + 1):nrow(df_list[[1]])) {

    sim_func_it <- function(df) {
        current_wl <- data.frame(
            Referral = rep(as.Date(df$start_date[i-1])
                           , df$Waiting.list.size[i-1]),
            Removal = rep(as.Date(NA), df$Waiting.list.size[i-1])
        )

        sim <- wl_simulator_cpp(
            start_date = as.Date(df[i,]$start_date),
            end_date = as.Date(df[i,]$end_date),
            demand = df[i,]$Referrals,
            capacity = df[i,]$Removals, # project last point forward
            waiting_list = current_wl
        )

        data.frame(Descriptor = df$Descriptor[1], queue = tail(wl_queue_size(sim)[, 2],1),
                   mean_wait = wl_stats(sim)$mean_wait)
    }

    # Apply to each specialty in df_list
    results_it <- map(df_list, function(df) {
        # Run 50 simulations for this specialty
        sims <- future_replicate(20, sim_func(df), simplify = FALSE)
        # Combine into one data frame
        bind_rows(sims)
    })

    #plan(sequential)

    #saveRDs(results, "./data/results_didsinput2025.rds")

    # Combine all specialties into one data frame
    all_results_it <- bind_rows(results_it)

    # Summarize mean and median queue per Descriptor
    summary_results_it <- all_results_it %>%
        group_by(Descriptor) %>%
        summarise(
            mean_queue = mean(queue, na.rm = TRUE),
            median_queue = median(queue, na.rm = TRUE),
            .groups = "drop"
        )

    #summary_results

    #print(df_list[[1]], n = 92) # 15458 110
    #print(df_list[[2]], n = 92) # 163286 99999


    # Update df_list last row plus 1 to get to end of 2025/26row 19 with median_queue
    df_list <- map(df_list, function(df) {
        dscr <- df$Descriptor[1]
        median_val <- round(summary_results_it$median_queue[summary_results_it$Descriptor == dscr])
        df$Waiting.list.size[i] <- median_val
        df
    })

    # run NHSR waiting list functions over each data.frame in list (Descriptor)
    df_list <- map(df_list, function(.x) {
        .x$target_wl[i] <- calc_target_queue_size(
            demand = .x$Referrals[i],
            target_wait = 6,
            factor = qexp(.x$target[i])
        )

        dscr <- first(.x$Descriptor)

        cv_demand <- tf_summary %>% filter(Descriptor == dscr) %>% pull(cv_demand)
        cv_capacity <- tf_summary %>% filter(Descriptor == dscr) %>% pull(cv_capacity)

        # Assuming we meet target list size in each period
        #.x$Waiting.list.size <- coalesce(.x$Waiting.list.size, .x$target_wl)


        .x$target_capacity[i] <-
            calc_target_capacity(
                demand = .x$Referrals[i],
                target_wait = 6,
                factor = qexp(.x$target[i]),
                cv_demand = cv_demand,
                cv_capacity = cv_capacity)

        .x$relief_capacity[i] <-
            calc_relief_capacity(
                demand = .x$Referrals[i],
                queue_size = .x$Waiting.list.size[i],
                target_queue_size = .x$target_wl[i],
                time_to_target = 52,
                cv_demand = cv_demand
            )



        .x
    })


}




######### Now add new capacity column #########################

df_list <- map(df_list, function(.x) {

    .x$calc_capacity <- round(ifelse(.x$start_date < target_dts[1,]$startdate,
                                     .x$Removals, # Actual capacity
                                     ifelse(.x$start_date > target_dts[4,]$enddate,
                                            .x$Referrals, # peg at demand to maintain
                                     .x$relief_capacity))) # Calcualted relief capacity


    .x
})


print(df_list[[1]], n=90)

################################################################

library(furrr)
#library(future.mirai)

df_list2 <- df_list[1]

print(df_list2[[1]], n = 50)
# Set up parallel plan (multisession works on most OS)
plan(multisession, workers = 5)  # Adjust workers to your CPU cores

#plan(mirai_multisession, workers = 6)
start_time <- Sys.time()
df_list2 <- df_list[1]
sim_results <- map(df_list, function(df) {

    df <- df[df$start_date >= target_dts$startdate[1],]
    #Rcpp::sourceCpp("wl_simulator.cpp")
    # Extract starting_wl from first row
    start_wl <- df[1, "Waiting.list.size", drop = TRUE]
    if (is.na(start_wl)) start_wl <- 0

    # Inner parallel map (optional)
    future_map(1:25, function(i) {
        #Rcpp::sourceCpp("wl_simulator.cpp")
        bsol_montecarlo_WL(
            .data = df,
            run_id = i,
            start_date_name = "start_date",
            end_date_name = "end_date",
            adds_name = "Referrals",
            removes_name = "calc_capacity",
            starting_wl = start_wl
        )
    }, .options = furrr_options(seed = NULL, globals = TRUE))
}
)



end_time <- Sys.time()

#saveRDS(sim_results, "./data/bsol_sims.rds")

end_time - start_time
#inner_sequential <- end_time - start_time

# end parallel sessions
plan(sequential)


tail(sim_results[[1]][[2]])
# Bind each run together within first level of list, per speciality (each list slot is specialty)
mc_bind <-  map(sim_results,  function(.x) {do.call("rbind", .x)})

# Aggregate each function within list slot (each list slot is specialty)
mc_agg <- map(mc_bind ,  function(.x) {
    aggregate(
        queue_size ~ dates
        , data = .x
        , FUN = \(x) {
            c(mean_q = mean(x),
              median_q = median(x),
              lower_95CI = mean(x) -  (1.96 * (sd(x) / sqrt(length(x)))),
              upper_95CI = mean(x) +  (1.96 * (sd(x) / sqrt(length(x)))),
              q_25 = quantile(x, .025, names = FALSE),
              q_75 = quantile(x, .975, names = FALSE))
        }
    )
})

mc_agg[[1]]

# Rename funk column names form nest list in aggregate step
mc_agg <- map(mc_agg, ~ data.frame(
    dates = as.Date(.x$dates),
    unlist(.x$queue_size)
)
)

chck <- mc_aggb[[1]] |>
    as.data.frame()


# Testing plot
ggplot(as.data.frame(mc_bind[[1]]), aes(x = dates)) +
    geom_line(aes(y = queue_size, group = run_id), alpha = 0.5, col = "grey") +
    geom_ribbon(aes(y = mean_q, ymin = lower_95CI, ymax = upper_95CI)
                , alpha = 0.5, data = mc_agg[[1]], fill = "red") +
    geom_line(aes(y = mean_q), data = mc_agg[[1]], col = "black") +
    geom_hline(yintercept = as.numeric(df_list[[1]][90,]$target_wl), col = "blue") +
    annotate("text", x = as.Date("01/01/2026", "%d/%m/%Y"), y = as.numeric(df_list[[1]][90,]$target_wl)*1.01,
             label = "Target waiting list for 92% at 18 weeks 2028/29", col = "blue",
             hjust = 0.1, vjust = 0.1) +
    scale_y_continuous(labels = comma) +
    labs(y = "Queue Size", x = "Date"
         , title = "Simulated BSOL waiting list after raising capacity"
         , subtitle = "Average WL over 25 runs, with 95% point-wise confidence interval")


# Testing plot
ggplot(as.data.frame(mc_bind[[3]]), aes(x = dates)) +
    geom_line(aes(y = queue_size, group = run_id), alpha = 0.5, col = "grey") +
    geom_ribbon(aes(y = mean_q, ymin = lower_95CI, ymax = upper_95CI)
                , alpha = 0.5, data = mc_agg[[3]], fill = "red") +
    geom_line(aes(y = mean_q), data = mc_agg[[3]], col = "black") +
    geom_hline(yintercept = as.numeric(df_list[[3]][90,]$target_wl), col = "blue") +
    labs(y = "Queue Size", x = "Date"
         , title = "Simulated waiting list after raising capacity"
         , subtitle = "Average WL over 25 runs, with 95% confidence interval")


#Need to take out the waiting list size at last point of data, next point would be sustainable 65%,
#then next year would be sustainable 92%, then same each year.
# Need target capacity calc., then add extr capacity in capacity and sim.



# install.packages("writexl")
library(writexl)

# Ensure list elements are named (used as sheet names)
names(df_list) <- dt_load |> distinct(Descriptor) |> pull()

write_xlsx(df_list, path = "./output/dids.xlsx")



###############################################
# Snip out the last queue at the end of each year
###############################################

select_march_year_end <- function(df) {
    df %>%
        mutate(dates = as.Date(dates)) %>%              # ensure Date type
        filter(month(dates) == 3) %>%                   # keep only March
        mutate(year_end = year(dates)) %>%              # FY ends in this calendar year
        group_by(year_end) %>%
        slice_max(order_by = dates, n = 1, with_ties = FALSE) %>%  # last date in March
        ungroup() %>%
        select(dates, mean_q,lower_95CI, upper_95CI)
}

# Apply to the list
march_ends_list <- map(mc_agg, select_march_year_end)


##########################################
# Add into df_list
############################################

# If your march_ends_list currently uses 'dates' (from your earlier function),
# standardize its key column to 'end_date' first:
march_ends_list2 <- map(march_ends_list, ~ rename(.x, end_date = dates))

# Define which columns from the March list to bring over (excluding the key)
cols_to_add <- c("mean_q", "lower_95CI", "upper_95CI")  # adjust as needed

# Perform the join per-pair of data.frames
updated_df_list[[1]] <- map2(
    df_list,
    march_ends_list2,
    ~ .x %>%
        mutate(end_date = as.Date(end_date)) %>%                                # ensure Date
        left_join(
            .y %>%
                mutate(end_date = as.Date(end_date)) %>%
                distinct(end_date, .keep_all = TRUE) %>%                            # guard against duplicates
                select(end_date, any_of(cols_to_add)),                              # only bring chosen cols
            by = "end_date"
        )
)

# Result: updated_df_list is the same length as df_list, with added columns populated
# where end_date matches the FY March endpoints; NA elsewhere.



library(dplyr)
library(purrr)

# 0) Basic sanity checks
stopifnot(length(df_list) == length(march_ends_list))

# If your march_ends_list still has `dates` as the March endpoint, rename to `end_date`
march_ends_list2 <- map(march_ends_list, ~ {
    y <- .x
    if ("dates" %in% names(y)) y <- rename(y, end_date = dates)
    y
})

# 1) Normalize types and guard against duplicate keys on the March list
normalize_march <- function(df) {
    df %>%
        mutate(end_date = as.Date(end_date)) %>%       # ensure Date for the key
        distinct(end_date, .keep_all = TRUE)           # one row per end_date
}

normalize_df <- function(df) {
    df %>%
        mutate(end_date = as.Date(end_date))           # ensure Date for the key
}

march_ends_list2 <- map(march_ends_list2, normalize_march)
df_list2         <- map(df_list,         normalize_df)

# 2) Columns you want to add from the March endpoints
cols_to_add <- c("mean_q", "lower_95CI", "upper_95CI")  # adjust if needed

# 3) Pairwise join by position (i-th with i-th)
updated_df_list <- map2(
    df_list2,
    march_ends_list2,
    ~ {
        add_cols <- select(.y, end_date, any_of(cols_to_add))
        # If March list element is empty, add NA columns and return
        if (nrow(add_cols) == 0) {
            out <- .x
            for (col in cols_to_add) if (!col %in% names(out)) out[[col]] <- NA_real_
            return(out)
        }
        left_join(.x, add_cols, by = "end_date")
    }
)



write_xlsx(updated_df_list[[1]], path = "./output/dids2.xlsx")

print(updated_df_list[[1]], n = 92)

print(march_ends_list2[[1]], n = 92)


mc_agg[[1]]
