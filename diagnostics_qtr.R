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
        startdate = as.Date(c('01/11/2025', '01/04/2026', '01/04/2027'
                              , '01/04/2028','01/04/2029','01/04/2030'), '%d/%m/%Y'),
        enddate = as.Date(c('31/03/2026', '31/03/2027', '31/03/2028'
                            , '31/03/2029', '31/03/2030', '31/03/2031'), '%d/%m/%Y'),
        value = c(0.8, 0.86, 0.93, 0.99, 0.99, 0.99),
        descr = c("diag80%", "diag86%", "diag93%", "diag99%", "diag99%", "diag99%")
    )

population_growth <-
    tibble::tribble(
        ~start_date,    ~end_date, ~ratio_increase, ~population,
        "01/11/2025", "31/03/2026",               1,     1590793,
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
#
# library(DBI)
# con <- dbConnect(odbc::odbc(), Driver = "SQL Server", Server = "MLCSU-BI-SQL",
#                  Database = "EAT_Reporting_BSOL", Trusted_Connection = "True")
#
# #sql2 <- read_file("./resident_dids_data.sql")
#
# sql <-
# "Select Descriptor,
# DATEADD(day,-6, WeekEndingDate) WeekStartingDate,
# WeekEndingDate,
# sum(case when DATEDIFF(day, [ClockStartDate], WeekEndingDate) <=7 THEN 1 ELSE 0 END) as Referrals,
# NULL as Removals,
# count(*) as [Waiting.list.size]
# from
# [EAT_Reporting_BSOL].[DEVELOPMENT].[BSOL0057_Data_Resident] T1
# LEFT JOIN [EAT_Reporting_BSOL].[Development].[BSOL0057_RefDiagMod] T2
# ON Modality = t2.Code
# Where WeekEndingDate >= {d '2024-04-01'} and Descriptor is not NULL
# group by Descriptor,
# WeekEndingDate
# order by Descriptor, WeekEndingDate"

#dt_load <- dbGetQuery(con, sql2)

# write.csv(dt_load, "./data/dids_load.csv", row.names = FALSE)

library(readxl)
dt_load <- read_xlsx("./data/dids_load.xlsx", .name_repair = "universal_quiet" , sheet = "Sheet1")
#dt_load <- read_xlsx("./data/dids_quarterly.xlsx", .name_repair = "universal_quiet" )


# Clear any float/double number formatting, as it trips rpois over.
dt_load$start_date <- as.Date(dt_load$start_date)
dt_load$end_date <- as.Date(dt_load$end_date)
dt_load$Referrals <- as.integer(dt_load$Referrals)
dt_load$Removals <- as.integer(dt_load$Removals)
dt_load$Waiting.list.size <- as.integer(dt_load$Waiting.list.size)


# remove Barium enema, as it's too small numbers - leave barium in a sworking now.
# dt_load <- dt_load[dt_load$Descriptor == "Barium Enema",]

##################Prepare data###########################

############### Convert to list of data.frames per specialty#####################
# Split data frame by Specialty, into a list.
# Each slot in the list is a data.frame for a specialty
df_list <- dt_load |>  #test_input %>%
    mutate(ratio_increase = as.numeric(NA)) |>
    group_by(Descriptor) %>%
    group_split()

print(df_list[[1]], n = 92)

# Correct waiting list size
df_list <- map(df_list, function(.x) {
    sub <- .x[1,]
    .x$Waiting.list.size  <- data.table::shift(.x$Waiting.list.size, 1, type = "lag") + .x$Referrals - .x$Removals

    rbind(sub, .x[2:nrow(.x),])
})

# Chop November as it's wonky
df_list <- map(df_list, function(.x) {
    .x <- .x[.x$end_date < as.Date("01/11/2025", "%d/%m/%Y"),]
    .x
})




# 1) Summarise each input df to 3-month groups
df_list2 <- map(df_list, function(.x) {
    .x <-
        .x |>
        mutate(new_start = case_when(
            month(end_date) < 4 ~ as.Date(paste0(as.character(year(end_date)), "-01-01" )),
            month(end_date) < 7 ~ as.Date(paste0(as.character(year(end_date)), "-04-01" )),
            month(end_date) < 10 ~ as.Date(paste0(as.character(year(end_date)), "-07-01" )),
            month(end_date) < 13 ~ as.Date(paste0(as.character(year(end_date)), "-10-01" )),
            .default = NA
        ),
        new_end = case_when(
            month(end_date) < 4 ~ as.Date(paste0(as.character(year(end_date)), "-03-31" )),
            month(end_date) < 7 ~ as.Date(paste0(as.character(year(end_date)), "-06-30" )),
            month(end_date) < 10 ~ as.Date(paste0(as.character(year(end_date)), "-09-30" )),
            month(end_date) < 13 ~ as.Date(paste0(as.character(year(end_date)), "-12-31" )),
            .default = NA)
        ) |>
        group_by(Descriptor, start_date = new_start, end_date = new_end) |>
        summarise(Referrals = round(mean(Referrals)),
                  #Referrals_md = median(Referrals),
                  Removals = round(mean(Removals)),
                  #Removals_md = median(Removals),
                  Waiting.list.size = last(Waiting.list.size))
    .x
})

# Manually fix short period

df_list2 <- map(df_list2, function(.x) {
    .x <-
        .x |>
        mutate(end_date = as.Date(ifelse(end_date == as.Date("2025-12-31"), "2025-10-31", end_date)))

    .x
})



df_list2[[1]]

# # Fix the first value to median  -  Not required after CM calcualtion update.
# df_list <- map(df_list, function(.x) {
#     med <- as.integer(median(.x$Waiting.list.size, na.rm = TRUE))
#     .x$Waiting.list.size[1] <- med
#     .x
# })


last_data_row <- nrow(df_list2[[1]])

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
df_list2 <- map(df_list2, ~ rbind(.x, df_append))

print(df_list2[[1]], n = 92)

#### Populate values logically ####
######## Populate values ####

# fill in values, Descriptor
df_list2 <- map(df_list2, function(.x) {
    dscr <- first(.x$Descriptor)
    .x$Descriptor[is.na(.x$Descriptor)] <- dscr
    .x
})
#.x <- df_list[[1]]
# median capacity for last 3 months fiscal year only
df_list2 <- map(df_list2, function(.x) {
    sub <- filter(.x, start_date > as.Date("2025-08-31"))
    med <- as.integer(median(sub$Removals, na.rm = TRUE))
    .x$Removals[is.na(.x$Removals)] <- med
    .x
})

#.x <- df_list2[[1]]
# Fill in referrals based on last known value and growth ratios
df_list2 <- map(df_list2, function(.x) {
    .x$Referrals = {
                last_val <- last(.x$Referrals[!is.na(.x$Referrals)])
                new_vals <- .x$Referrals
                for (i in seq_along(new_vals)) {
                    if (is.na(new_vals[i])) {
                        if (i == 1 || !is.na(new_vals[i - 1])) {
                            new_vals[i] <- last_val * .x$ratio_increase[i]
                        } else {
                            new_vals[i] <- new_vals[i - 1] * .x$ratio_increase[i]
                        }
                    }
                }
                as.integer(new_vals)
            }

    .x
})


# Add targets into data.frames in list
df_list2 <- map(df_list2, function(.x) {

    .x <- fuzzy_left_join(.x, target_dts, by = c("start_date" = "startdate"
                                                 , "end_date" = "enddate")
                          , match_fun = list(`>=`, `<=`)) |>
        select(-startdate,-enddate,-descr) |>
        rename(target = value)
    .x
})

# Initialize columns for later
df_list2 <- map(df_list2, function(.x) {

    .x$target_wl <- NA
    .x$target_capacity <- NA
    .x$relief_capacity_cur <- NA
    .x$relief_capacity_rel <- NA
    .x$Waiting.list.size_relief <- NA

    # Copy over right waiting list size as easier than using 2 columns in sim function.
    .x$Waiting.list.size_relief[last_data_row] <- .x$Waiting.list.size[last_data_row]

    .x
})


print(df_list2[[1]], n = 92)
print(df_list2[[2]], n = 92)



df_list2[[1]]

# Need to loop this now to build WL, then calculate targets, and sim next
#i <- 86L
#df <- df_list2[[1]]
#i = 8

for (i in (last_data_row + 1):nrow(df_list2[[1]])) {


    # run NHSR waiting list functions over each data.frame in list (Descriptor)
    ##.x <- df_list2[[1]]
    df_list2 <- map(df_list2, function(.x) {
        .x$target_wl[i] <- floor(calc_target_queue_size(
            demand = .x$Referrals[i],
            target_wait = 6,
            factor = qexp(.x$target[i])
        ))


        dscr <- first(.x$Descriptor)

        cv_demand <- tf_summary %>% filter(Descriptor == dscr) %>% pull(cv_demand)
        cv_capacity <- tf_summary %>% filter(Descriptor == dscr) %>% pull(cv_capacity)

        # Assuming we meet target list size in each period
        #.x$Waiting.list.size <- coalesce(.x$Waiting.list.size, .x$target_wl)


        .x$target_capacity[i] <-
            ceiling(calc_target_capacity(
                demand = .x$Referrals[i],
                target_wait = 6,
                factor = qexp(.x$target[i]),
                cv_demand = cv_demand,
                cv_capacity = cv_capacity))


        .x$relief_capacity_cur[i] <-
            ceiling(calc_relief_capacity(
                demand = .x$Referrals[i],
                queue_size = .x$Waiting.list.size[i - 1],
                target_queue_size = .x$target_wl[i],
                time_to_target = 52, # set to 45 to give 7 weeks per year grace
                cv_demand = cv_demand
            ))

        .x$relief_capacity_rel[i] <-
            ceiling(calc_relief_capacity(
                demand = .x$Referrals[i],
                queue_size = .x$Waiting.list.size_relief[i - 1],
                target_queue_size = .x$target_wl[i],
                time_to_target = 52, # set to 45 to give 7 weeks per year grace
                cv_demand = cv_demand

            ))



        .x
    })

    # Sim with current capacity projected forward
    sim_func_cur <- function(df) {
        current_wl <- data.frame(
            Referral = rep(as.Date(df$start_date[i]-1)
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
    results_cur <- map(df_list2, function(df) {
        # Run 50 simulations for this specialty
        sims <- future_replicate(25, sim_func_cur(df), simplify = FALSE)
        # Combine into one data frame
        bind_rows(sims)
    })

    #plan(sequential)

    #saveRDs(results, "./data/results_didsinput2025.rds")

    # Combine all specialties into one data frame
    all_results_cur <- bind_rows(results_cur)

    # Summarize mean and median queue per Descriptor
    summary_results_cur <- all_results_cur %>%
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
    df_list2 <- map(df_list2, function(df) {
        dscr <- df$Descriptor[1]
        mean_val <- round(summary_results_cur$mean_queue[summary_results_cur$Descriptor == dscr])
        df$Waiting.list.size[i] <- mean_val
        df
    })


    # Now using relief capacity instead
    sim_func_rel <- function(df) {
        current_wl <- data.frame(
            Referral = rep(as.Date(df$start_date[i]-1)
                           , df$Waiting.list.size_relief[i-1]),
            Removal = rep(as.Date(NA), df$Waiting.list.size_relief[i-1])
        )

        sim_rel <- wl_simulator_cpp(
            start_date = as.Date(df[i,]$start_date),
            end_date = as.Date(df[i,]$end_date),
            demand = df[i,]$Referrals,
            capacity = df[i,]$relief_capacity_rel, # project last point forward
            waiting_list = current_wl
        )

        data.frame(Descriptor = df$Descriptor[1], queue = tail(wl_queue_size(sim_rel)[, 2],1),
                   mean_wait = wl_stats(sim_rel)$mean_wait)
    }

    # Apply to each specialty in df_list
    results_rel <- map(df_list2, function(df) {
        # Run 50 simulations for this specialty
        sims_rel <- future_replicate(25, sim_func_rel(df), simplify = FALSE)
        # Combine into one data frame
        bind_rows(sims_rel)
    })

    #plan(sequential)

    #saveRDs(results, "./data/results_didsinput2025.rds")

    # Combine all specialties into one data frame
    all_results_rel <- bind_rows(results_rel)

    # Summarize mean and median queue per Descriptor
    summary_results_rel <- all_results_rel %>%
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
    df_list2 <- map(df_list2, function(df) {
        dscr <- df$Descriptor[1]
        mean_val <- round(summary_results_rel$mean_queue[summary_results_rel$Descriptor == dscr])
        df$Waiting.list.size_relief[i] <- mean_val
        df
    })




}

52 * (544-529)

1650 - 780


df_list2[[1]]

######### Now add new capacity column #########################

df_list2 <- map(df_list2, function(.x) {

    .x$calc_capacity <- ceiling(ifelse(.x$start_date < target_dts[1,]$startdate,
                                     .x$Removals, # Actual capacity
                                     ifelse(.x$start_date > target_dts[4,]$enddate,
                                           .x$target_capacity, # peg at target capacity for
                                     .x$relief_capacity_rel)) # Calcualted relief capacity
                                    )

    .x
})


print(df_list2[[1]], n=90)

################################################################

library(furrr)
#library(future.mirai)

df_list2 <- df_list[1]

print(df_list2[[1]], n = 50)
# Set up parallel plan (multisession works on most OS)
plan(multisession, workers = 5)  # Adjust workers to your CPU cores

#plan(mirai_multisession, workers = 6)
start_time <- Sys.time()
#df_list2 <- df_list[1]
#df <- df_list[[1]]
sim_results_rel <- map(df_list2, function(df) {


    #Rcpp::sourceCpp("wl_simulator.cpp")
    # Extract starting_wl from first row
    start_wl <- df[last_data_row, "Waiting.list.size_relief", drop = TRUE]
    if (is.na(start_wl)) start_wl <- 0

    df <- df[df$start_date >= target_dts$startdate[1],]

    # Inner parallel map (optional)
    future_map(1:50, function(i) {
        #Rcpp::sourceCpp("wl_simulator.cpp")
        bsol_montecarlo_WL3(
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
#plan(sequential)


#tail(sim_results[[1]][[2]])
# Bind each run together within first level of list, per speciality (each list slot is specialty)
mc_bind_rel <-  map(sim_results_rel,  function(.x) {do.call("rbind", .x)})

# Aggregate each function within list slot (each list slot is specialty)
mc_agg_rel <- map(mc_bind_rel,  function(.x) {
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

#mc_agg_rel[[1]]

# Rename funky column names form nest list in aggregate step
mc_agg_rel <- map(mc_agg_rel, ~ data.frame(
    dates = as.Date(.x$dates),
    unlist(.x$queue_size)
)
)


#plan(mirai_multisession, workers = 6)
start_time <- Sys.time()
#df_list2 <- df_list[1]
#df <- df_list2[[1]]
sim_results_cur <- map(df_list2, function(df) {


    #Rcpp::sourceCpp("wl_simulator.cpp")
    # Extract starting_wl from first row
    start_wl <- df[last_data_row, "Waiting.list.size", drop = TRUE]
    if (is.na(start_wl)) start_wl <- 0

    df <- df[df$start_date >= target_dts$startdate[1],]

    # Inner parallel map (optional)
    future_map(1:50, function(i) {
        #Rcpp::sourceCpp("wl_simulator.cpp")
        bsol_montecarlo_WL3(
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


# ggplot(b, aes(y=queue_size, x=dates, col = run_id))+
#     geom_line()

end_time <- Sys.time()

#saveRDS(sim_results, "./data/bsol_sims.rds")

end_time - start_time
#inner_sequential <- end_time - start_time

# end parallel sessions
#plan(sequential)


# Bind each run together within first level of list, per speciality (each list slot is specialty)
mc_bind_cur <-  map(sim_results_cur,  function(.x) {do.call("rbind", .x)})

# Aggregate each function within list slot (each list slot is specialty)
mc_agg_cur <- map(mc_bind_cur,  function(.x) {
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

#mc_agg_cur[[1]]

# Rename funk column names form nest list in aggregate step
mc_agg_cur <- map(mc_agg_cur, ~ data.frame(
    dates = as.Date(.x$dates),
    unlist(.x$queue_size)
)
)


write.csv(df_list2[[1]], "barium_check.csv")

brewer_pal(type = "div", palette = "Paired")

# Testing plot
ggplot() +
    geom_line(aes(x = end_date, y = Waiting.list.size), col = "black"
              , data = df_list2[[1]][df_list2[[1]]$start_date < target_dts$startdate[1],]) +
    geom_line(aes(x = dates, y = queue_size, group = run_id), alpha = 0.4, col = "#A6CEE3"
              , data = as.data.frame(mc_bind_cur[[1]])) +
    geom_ribbon(aes(x = dates, y = mean_q, ymin = lower_95CI, ymax = upper_95CI)
                , alpha = 0.5, data = mc_agg_cur[[1]], fill = "#1F78B4") +
    geom_line(aes(x = dates, y = mean_q), data = mc_agg_cur[[1]], col = "#1F78B4") +
    geom_line(aes(x = dates, y = queue_size, group = run_id), alpha = 0.3, col =  "#B2DF8A"
              , data = as.data.frame(mc_bind_rel[[1]])) +
    geom_ribbon(aes(x = dates, y = mean_q, ymin = lower_95CI, ymax = upper_95CI)
                , alpha = 0.5, data = mc_agg_rel[[1]], fill = "#33A02C" ) +
    geom_line(aes(x = dates, y = mean_q), data = mc_agg_rel[[1]], col = "#33A02C" ) +



    geom_hline(yintercept = as.numeric(df_list2[[1]][11,]$target_wl), col = "#FF7F00") +
    annotate("text", x = as.Date("01/01/2029", "%d/%m/%Y")
             , y = as.numeric(df_list2[[1]][11,]$target_wl)*4,
             label = "Target waiting list for 92%\nat 18 weeks 2028/29", col = "#FF7F00",
             hjust = 0.1, vjust = 0.1) +
    scale_y_continuous(labels = comma, breaks = seq(0,500,50)) +
    scale_x_date(date_breaks = "6 months", date_labels = "%b-%y", date_minor_breaks = "3 months",
                 limits = as.Date(c("2024-04-01", "2031-04-01"), "%Y-%m-%d"), expand = 0,
                 ) +
    guides(x = guide_axis(check.overlap = TRUE, n.dodge = 2)) +
    labs(y = "Queue Size", x = "Date"
         , title = "Simulated BSOL waiting list for Barium Enema"
         , subtitle = "Average WL over 50 runs, with 95% point-wise confidence interval") +
    theme(axis.text.x = element_text(angle = 0),
          axis.line = element_line(color = "grey"),
          axis.ticks = element_line(color = "grey"),
          plot.margin = unit(c(2,5,2,2),"mm"))


df_list2[[1]]



#Need to take out the waiting list size at last point of data, next point would be sustainable 65%,
#then next year would be sustainable 92%, then same each year.
# Need target capacity calc., then add extr capacity in capacity and sim.



# install.packages("writexl")
library(writexl)

# Ensure list elements are named (used as sheet names)
as.data.frame(names(df_list2)) <- dt_load |> distinct(Descriptor) |> pull()

write_xlsx(df_list2, path = "./output/dids.xlsx")



