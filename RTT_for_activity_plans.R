library(tidyverse)
library(fuzzyjoin)
library(NHSRwaitinglist)
library(scales)
library(ggtext)
library(zoo)
#source("monte_carlo_func.R")
source("utils.R")
library(purrr)
library(furrr)
library(future.apply)
library(parallel)
library(BSOLwaitinglist)


# set ggplot theme
theme_set(
    theme_minimal() +
        theme(plot.title = element_text(size = 12),
              plot.subtitle = element_text(size = 9, face = "italic")
        )
)


######################## Targets & population growth ##############

last_data_date <- as.Date('01/09/2025', '%d/%m/%Y')

target_dts <-
    data.frame(
        startdate = as.Date(c('01/04/2026', '01/04/2027', "01/04/2029"), '%d/%m/%Y'),
        enddate = as.Date(c('31/03/2027', '31/03/2029', "31/03/2031"), '%d/%m/%Y'),
        value = c(0.65, 0.92, 0.92),
        descr = c("RTT65%", "RTT92%", "RTT92%")
    )

# # Bsol
# population_growth <-
#     tibble::tribble(
#         ~start_date,    ~end_date, ~ratio_increase, ~population,
#         "01/09/2025", "31/03/2026",               1,     1590793,
#         "01/04/2026", "31/03/2027",        1.003309, 1596056.425,
#         "01/04/2027", "31/03/2028",        1.006733, 1601503.998,
#         "01/04/2028", "31/03/2029",        1.010701, 1607815.611,
#         "01/04/2029", "31/03/2030",        1.015318, 1615161.207,
#         "01/04/2030", "31/03/2031",        1.020006, 1622704.257
#     )

#bc
population_growth <-
    tibble::tribble(
          ~start_date,    ~end_date, ~ratio_increase, ~population,
         "01/09/2025", "31/03/2026",               1,     1254329,
         "01/04/2026", "31/03/2027",     1.005597424,     1261350,
         "01/04/2027", "31/03/2028",     1.011238882,     1268426,
         "01/04/2028", "31/03/2029",     1.016872582,     1275493,
         "01/04/2029", "31/03/2030",      1.02287851,     1283026,
         "01/04/2030", "31/03/2031",     1.028903111,     1290583
     )



# Convert Year to Date
population_growth$start_date <- as.Date(population_growth$start_date, '%d/%m/%Y')
population_growth$end_date <- as.Date(population_growth$end_date, '%d/%m/%Y')



##################Load data#############################

library(readxl)

in_bc <- read_excel("data/20260107_RTT_bc.xlsx", .name_repair = "universal_quiet")
#in_bsol <- read_excel("data/20251229_RTT_bsol.xlsx", .name_repair = "universal_quiet")

#in_bsol |>  group_by(Org_Code, Org_Name) |> count()

# Rename and remove WL column so don't have to upate later code
in_bc$Waiting.list.size <- as.integer(in_bc$Waiting_List_Size)
in_bc$Waiting_List_Size <- NULL

#in_bsol$Waiting.list.size <- as.integer(in_bsol$Waiting_List_Size)
#in_bsol$Waiting_List_Size <- NULL

# Drop prior to 2024/25
in_bc <-
    in_bc |>
    filter(start_date > as.Date("2024-03-31", "%Y-%m-%d"))

# # Drop prior to 2024/25
# in_bsol <-
#     in_bsol |>
#     filter(start_date > as.Date("2024-03-31", "%Y-%m-%d"))


# # Drop X03 as numbers too small
# in_bsol <-
#     in_bsol |>
#     filter(Specialty != 'X03')

in_bc <-
    in_bc |>
    filter(Specialty != 'X03')

# Remove several DQ issues:
# single 300 in RBK:

in_bc <-
   in_bc |>
   filter(!(Specialty == '300' & Org_code == "RBK"))

# 170 at rxk.  0 referrals and removal smost weeks

in_bc <-
   in_bc |>
   filter(!(Specialty == '170' & Org_code == "RXK"))

# 160 is small numbers and a bit odd
in_bc <-
   in_bc |>
   filter(!(Specialty == '160' & Org_code == "RXK"))

# # Rq3 -
# # Drop 100, 130, X05 due to very small numbers
# in_bsol <-
#    in_bsol |>
#    filter(!(Specialty == '100' & Org_Code == "RQ3"))
#
# in_bsol <-
#     in_bsol |>
#     filter(!(Specialty == '130' & Org_Code == "RQ3"))
#
# in_bsol <-
#     in_bsol |>
#     filter(!(Specialty == 'X05' & Org_Code == "RQ3"))
#
#
# in_bsol <-
#     in_bsol |>
#     filter(!(Specialty == '150' & Org_Code == "RRJ"))
#
#
# in_bsol <-
#     in_bsol |>
#     filter(!(Specialty == 'X04' & Org_Code == "RYW"))



# # Split into feeder df for each org. Ideally needs a wrapper to apply to each, but will manually run for now.
bc_total <- filter(in_bc, Org_code == "D2P2L") |> select(-Org_code, -Org_Name)
bc_rbk <- filter(in_bc, Org_code == "RBK") |> select(-Org_code, -Org_Name)
bc_rl4 <- filter(in_bc, Org_code == "RL4") |> select(-Org_code, -Org_Name)
bc_rna <- filter(in_bc, Org_code == "RNA") |> select(-Org_code, -Org_Name)
bc_rxk <- filter(in_bc, Org_code == "RXK") |> select(-Org_code, -Org_Name)

# bsol_total <- filter(in_bsol, Org_Code == "15E") |> select(-Org_Code, -Org_Name)
# bsol_rrk <- filter(in_bsol, Org_Code == "RRK") |> select(-Org_Code, -Org_Name)
# bsol_rq3 <- filter(in_bsol, Org_Code == "RQ3") |> select(-Org_Code, -Org_Name)
# bsol_rrj <- filter(in_bsol, Org_Code == "RRJ") |> select(-Org_Code, -Org_Name)
# bsol_ryw <- filter(in_bsol, Org_Code == "RYW") |> select(-Org_Code, -Org_Name)


#
#monthly_bsol$adj_removals <- NULL
#monthly_bsol$Est_renage <- NULL
#monthly_bsol$Waiting.list.size <- as.integer(monthly_bsol$Waiting.list.size)



##################Prepare data###########################

############### Convert to list of data.frames per specialty#####################
# Split data frame by Specialty, into a list.
# Each slot in the list is a data.frame for a specialty
df_list <- bc_total |>  #test_input %>%
    mutate(ratio_increase = as.numeric(NA)) |>
    group_by(Specialty) %>%
    group_split()


#.x <- df_list[[1]]

# Correct removals to balance waiting list
df_list <- map(df_list, function(.x) {
    sub <- .x[1,, drop = FALSE]

    .x$Removals  <- data.table::shift(.x$Waiting.list.size, 1, type = "lag") + .x$Referrals - .x$Waiting.list.size

    .x <- rbind(sub, .x[2:nrow(.x),, drop = FALSE])

    .x$Referrals <- as.integer(.x$Referrals / 4.33) # divide by 4.33 to turn monthly to weekly, needs to be done here after balance
    .x$Removals <- as.integer(.x$Removals / 4.33) # divide by 4.33 to turn monthly to weekly

    .x

})

# Now make same month-to-week adjustment and formatting outside look, for later plotting
bc_total$Referrals <- as.integer(bc_total$Referrals/ 4.33) # divide by 4.33 to turn monthly to weekly
bc_total$Removals <- as.integer(bc_total$Removals / 4.33) # divide by 4.33 to turn monthly to weekly


# Chop September as it's wonky due to PAS implementation
df_list <- map(df_list, function(.x) {
    .x <- .x[.x$end_date < as.Date("01/09/2025", "%d/%m/%Y"),]
    .x
})


# Last row based on data for iteration elements. Another taken after extending later for full data + simualtion period.
last_data_row <- nrow(df_list[[1]])
#last_data_row <- map(df_list, nrow)


# Calculate coefficients of variation (how each list behaves)
# cv_demand and cv_capacity for each specialty
tf_summary <- map_dfr(df_list, function(.x) {
    .x <- filter(.x, start_date < as.Date("2025-10-01"))
    cv_demand  <- sd(.x$Referrals, na.rm = TRUE) / mean(.x$Referrals, na.rm = TRUE)
    cv_capacity <- sd(.x$Removals, na.rm = TRUE) / mean(.x$Removals, na.rm = TRUE)

    tibble(
        Specialty = first(.x$Specialty),
        cv_demand = cv_demand,
        cv_capacity = cv_capacity
    )
})



# Append rows for population growth periods
df_append <- data.frame(Specialty = NA,
                        start_date = as.Date(population_growth$start_date, '%d/%m/%Y'),
                        end_date = as.Date(population_growth$end_date, '%d/%m/%Y'),
                        Referrals = NA, Removals = NA, Waiting.list.size = NA,
                        ratio_increase = population_growth$ratio_increase)


# Apply to data.frames in list.
df_list <- map(df_list, ~ rbind(.x, df_append))

print(df_list[[2]], n = 50)

last_row <- map(df_list, nrow)

#### Populate values logically ####
######## Populate values ####

# fill in values, specialty
df_list <- map(df_list, function(.x) {
    spec <- first(.x$Specialty)
    .x$Specialty[is.na(.x$Specialty)] <- spec
    .x
})

# median capacity over last 3 months. Longer seems a bit extreme
#j <- 1
df_list <- map(df_list, function(.x) {

    med <- as.integer(median(.x[(last_data_row - 11):last_data_row,]$Removals, na.rm = TRUE))

    .x$Removals[is.na(.x$Removals)] <- med
#
    #j <<- j + 1

    .x
})

#
# # Fill in referrals based on last known value and growth ratios
# df_list <- map(df_list, function(.x) {
#
#     .x %>%
#         mutate(
#             Referrals = {
#                 last_val <- last(Referrals[!is.na(Referrals)])
#                 new_vals <- Referrals
#                 for (i in seq_along(new_vals)) {
#                     if (is.na(new_vals[i])) {
#                         if (i == 1 || !is.na(new_vals[i - 1])) {
#                             new_vals[i] <- last_val * ratio_increase[i]
#                         } else {
#                             new_vals[i] <- new_vals[i - 1] * ratio_increase[i]
#                         }
#                     }
#                 }
#                 as.integer(new_vals)
#             }
#         )
# })

#.x <- df_list[[1]]
# Fill in referrals based on last known value and growth ratios
#j <- 1
df_list <- map(df_list, function(.x) {

        .x %>%
        mutate(
            Referrals = {
                last_val <- as.integer(median(.x[(last_data_row - 11):last_data_row,]$Referrals, na.rm = TRUE))
                new_vals <- .x$Referrals
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

#View(df_list[[1]])



print(df_list[[1]], n = 50)
#print(df_list[[2]], n = 50)

#.x <- df_list[[1]]

# Add targets into data.frames in list
df_list <- map(df_list, function(.x) {

    .x <- fuzzy_left_join(.x, target_dts, by = c("start_date" = "startdate", "end_date" = "enddate")
                          , match_fun = list(`>=`, `<=`)) |>
        mutate(time_to_target =  floor(as.numeric(difftime(enddate, start_date, units = "weeks")))) |>
        select(-startdate,-enddate,-descr) |>
        rename(target = value)
    .x
})
#.x<-df_list[[1]]
# Initialize columns for later
#j <- 1
df_list <- map(df_list, function(.x) {

    .x$target_wl <- NA
    .x$target_capacity <- NA
    .x$relief_capacity_cur <- NA
    .x$relief_capacity_rel <- NA
    .x$Waiting.list.size_relief <- NA
    .x$wl_performance_cur <- NA
    .x$wl_performance_rel <- NA

    # Copy over right waiting list size as easier than using 2 columns in sim function.
    .x$Waiting.list.size_relief[last_data_row] <- .x$Waiting.list.size[last_data_row]

    #j <<- j + 1
    .x
})


print(df_list[[2]], n = 92)
#print(df_list[[2]], n = 92)

#####

# Need to loop this now to build WL, then calculate targets, and sim next
#i <- 86L
#df <- df_list[[1]]
#i = 20

#.x <- df
plan(sequential)

if (exists("cl")){
    stopCluster(cl)
    rm(cl)
    gc()
}

# Create workers and pre-load Rcpp compilation on each
cl <- future::makeClusterPSOCK(workers = 6)

clusterEvalQ(cl, {
    library(BSOLwaitinglist)

})


#Optional sanity check: should be TRUE on each worker
parallel::clusterEvalQ(cl, exists("bsol_montecarlo_WL3", mode = "function"))


# Use the cluster in your plan
plan(cluster, workers = cl)

#j <- 2
for (i in (last_data_row + 1):nrow(df_list[[1]])) {

    # run NHSR waiting list functions over each data.frame in list (Specialty)
    ##.x <- df_list2[[1]]
    df_list <- map(df_list, function(.x) {
        .x$target_wl[i] <- floor(calc_target_queue_size(
            demand = .x$Referrals[i],
            target_wait = 18,
            factor = qexp(.x$target[i])
        ))


        dscr <- first(.x$Specialty)

        cv_demand <- tf_summary %>% filter(Specialty == dscr) %>% pull(cv_demand)
        cv_capacity <- tf_summary %>% filter(Specialty == dscr) %>% pull(cv_capacity)

        # Assuming we meet target list size in each period
        #.x$Waiting.list.size <- coalesce(.x$Waiting.list.size, .x$target_wl)


        .x$target_capacity[i] <-
            ceiling(calc_target_capacity(
                demand = .x$Referrals[i],
                target_wait = 18,
                factor = qexp(.x$target[i]),
                cv_demand = 1,
                cv_capacity = 1))


        .x$relief_capacity_cur[i] <-
            ceiling(calc_relief_capacity(
                demand = .x$Referrals[i],
                queue_size = .x$Waiting.list.size[i - 1],
                target_queue_size = .x$target_wl[i],
                time_to_target = .x$time_to_target[i],
                cv_demand = 1
            ))

        .x$relief_capacity_rel[i] <-
            ceiling(calc_relief_capacity(
                demand = .x$Referrals[i],
                queue_size = .x$Waiting.list.size_relief[i - 1],
                target_queue_size = .x$target_wl[i],
                time_to_target = .x$time_to_target[i],
                cv_demand = 1

            ))


        # manual correction for 65% target year, if capacity already higher, dont' reduce.
        .x$relief_capacity_rel[i] <- ifelse(
            .x$start_date[i] >= target_dts$startdate[1] &
            .x$end_date[i] <= target_dts$enddate[1] &
            .x$relief_capacity_rel[i] < .x$Removals[i],
            .x$Removals[i],
            .x$relief_capacity_rel[i]


            )


        .x
    })

    # Sim with current capacity projected forward
    sim_func_cur <- function(df) {
        current_wl <- data.frame(
            Referral = rep(as.Date(df$start_date[i] - 1)
                           , df$Waiting.list.size[i - 1]),
            Removal = rep(as.Date(NA), df$Waiting.list.size[i - 1])
        )

        sim <- wl_simulator_cpp(
            start_date = as.Date(df[i,]$start_date),
            end_date = as.Date(df[i,]$end_date),
            demand = df[i,]$Referrals,
            capacity = df[i,]$Removals, # project last point forward
            waiting_list = current_wl
        )

        data.frame(Specialty = df$Specialty[1], queue = tail(wl_queue_size(sim)[, 2],1),
                   mean_wait = wl_stats(sim)$mean_wait)
    }

    # Apply to each specialty in df_list
    results_cur <- map(df_list, function(df) {
        # Run 50 simulations for this specialty
        sims <- future_replicate(50, sim_func_cur(df), simplify = FALSE)
        # Combine into one data frame
        bind_rows(sims)
    })

    #plan(sequential)

    #saveRDs(results, "./data/results_didsinput2025.rds")

    # Combine all specialties into one data frame
    all_results_cur <- bind_rows(results_cur)

    # Summarize mean and median queue per Specialty
    summary_results_cur <- all_results_cur %>%
        group_by(Specialty) %>%
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
        dscr <- df$Specialty[1]
        mean_val <- round(summary_results_cur$mean_queue[summary_results_cur$Specialty == dscr])
        df$Waiting.list.size[i] <- mean_val

        df$wl_performance_cur[i] <- est_wait_performance(df$Referrals[i], df$Waiting.list.size[i], 18)

        df
    })


    # Now using relief capacity instead
    sim_func_rel <- function(df) {
        current_wl <- data.frame(
            Referral = rep(as.Date(df$start_date[i] - 1)
                           , df$Waiting.list.size_relief[i - 1]),
            Removal = rep(as.Date(NA), df$Waiting.list.size_relief[i - 1])
        )

        sim_rel <- wl_simulator_cpp(
            start_date = as.Date(df[i,]$start_date),
            end_date = as.Date(df[i,]$end_date),
            demand = df[i,]$Referrals,
            capacity = coalesce(df[i,]$relief_capacity_rel, df[i,]$Removals), # Coalesce added here to counter against NA's is targets dont' start at beginning of projection period.
            waiting_list = current_wl
        )

        data.frame(Specialty = df$Specialty[1], queue = tail(wl_queue_size(sim_rel)[, 2],1),
                   mean_wait = wl_stats(sim_rel)$mean_wait)
    }

    # Apply to each specialty in df_list
    results_rel <- map(df_list, function(df) {
        # Run 50 simulations for this specialty
        sims_rel <- future_replicate(50, sim_func_rel(df), simplify = FALSE)
        # Combine into one data frame
        bind_rows(sims_rel)
    })

    #plan(sequential)

    #saveRDs(results, "./data/results_didsinput2025.rds")

    # Combine all specialties into one data frame
    all_results_rel <- bind_rows(results_rel)

    # Summarize mean and median queue per Specialty
    summary_results_rel <- all_results_rel %>%
        group_by(Specialty) %>%
        summarise(
            mean_queue = mean(queue, na.rm = TRUE),
            median_queue = median(queue, na.rm = TRUE),
            .groups = "drop"
        )

    #summary_results

    #print(df_list[[1]], n = 92) # 15458 110
    #print(df_list[[2]], n = 92) # 163286 99999

    #j <<- j + 1

    # Update df_list last row plus 1 to get to end of 2025/26row 19 with median_queue
    df_list <- map(df_list, function(df) {
        dscr <- df$Specialty[1]
        mean_val <- round(summary_results_rel$mean_queue[summary_results_rel$Specialty == dscr])
        df$Waiting.list.size_relief[i] <- mean_val

        df$wl_performance_rel[i] <- est_wait_performance(df$Referrals[i], df$Waiting.list.size_relief[i], 18)
        df
    })



}

parallel::stopCluster(cl)
gc()

plan(sequential)

#502, x02,

print(df_list[[1]], n = 90)

#.x <- df_list[[1]]

######### Now add new capacity column #########################

df_list <- map(df_list, function(.x) {

    # .x$calc_capacity <- round(ifelse(.x$start_date >= target_dts[1,]$startdate & .x$start_date < target_dts[1,]$enddate,
    #                            ifelse(.x$relief_capacity_65 < .x$Removals, .x$Removals, .x$relief_capacity_65),
    #                            ifelse(.x$start_date >= target_dts[2,]$startdate & .x$start_date < target_dts[2,]$enddate,
    #                                   .x$relief_capacity_92,
    #                            ifelse(.x$start_date < target_dts[1,]$startdate, .x$Removals,
    #                            .x$Referrals)))) # have to keep pace with demand
    #
    # .x


    .x$calc_capacity <- ceiling(ifelse(.x$start_date < target_dts[1,]$startdate,
                                       .x$Removals, # Actual capacity
                                       ifelse(.x$start_date > target_dts[2,]$enddate,
                                              .x$target_capacity, # peg at target capacity for
                                             # ifelse(.x$start_date >= tail(target_dts,1)$enddate,
                                              #       .x$target_capacity,
                                              .x$relief_capacity_rel))#) # Calcualted relief capacity
    )

    .x
})



print(df_list[[5]], n = 90)

#a <- df_list[[1]]

################################################################

#library(furrr)
#library(future.mirai)




# Create workers and pre-load Rcpp compilation on each
cl <- future::makeClusterPSOCK(workers = 5)
clusterEvalQ(cl, {
    library(BSOLwaitinglist)

})


# Does the Rcpp wrapper exist on workers?
parallel::clusterEvalQ(cl, exists("bsol_montecarlo_WL3", mode = "function"))


# Use the cluster in your plan
plan(cluster, workers = cl)

#plan(sequential)


#plansequential()#plan(mirai_multisession, workers = 6)
start_time <- Sys.time()
#df_list2 <- df_list[1]
#df <- df_list[[1]]
sim_results_rel <- map(df_list, function(df) {


    #Rcpp::sourceCpp("wl_simulator.cpp")
    # Extract starting_wl from first row
    start_wl <- df[last_data_row+1, "Waiting.list.size_relief", drop = TRUE]
    if (is.na(start_wl)) start_wl <- 0

    df <- df[df$start_date >= target_dts$startdate[1],]
    #df <- df[df$start_date >= last_data_date,]

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
    }, .options = furrr_options(seed = NULL, globals = TRUE
                                , packages = c("Rcpp", "NHSRwaitinglist", "BSOLwaitinglist")
    )
    )
})


end_time <- Sys.time()

#saveRDS(sim_results, "./data/bsol_sims.rds")

end_time - start_time
#inner_sequential <- end_time - start_time

# end parallel sessions
#plan(sequential)


#tail(sim_results[[1]][[2]])
# Bind each run together within first level of list, per speciality (each list slot is specialty)
mc_bind_rel <- map(sim_results_rel,  function(.x) {do.call("rbind", .x)})

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
sim_results_cur <- map(df_list, function(df) {


    #Rcpp::sourceCpp("wl_simulator.cpp")
    # Extract starting_wl from first row
    start_wl <- df[last_data_row+1, "Waiting.list.size", drop = TRUE]
    if (is.na(start_wl)) start_wl <- 0

    df <- df[df$start_date >= target_dts$startdate[1],]
    #df <- df[df$start_date >= last_data_date,]

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
plan(sequential)
stopCluster(cl)


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

gc()

#write.csv(df_list2[[1]], "barium_check.csv")

#brewer_pal(type = "div", palette = "Paired")

# Testing plot
ggplot() +
    geom_line(aes(x = end_date, y = Waiting.list.size), col = "black"
              , data = df_list[[1]][df_list[[1]]$start_date < target_dts$startdate[1],]) +
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



    geom_hline(yintercept = as.numeric(df_list[[1]][21,]$target_wl), col = "#FF7F00") +
    annotate("text", x = as.Date("01/01/2025", "%d/%m/%Y")
             , y = as.numeric(df_list[[1]][21,]$target_wl)*0.6,
             label = "Target waiting list for 92%\nat 18 weeks 2028/29", col = "#FF7F00",
             hjust = 0.1, vjust = 0.1) +
    scale_y_continuous(labels = comma, breaks = seq(0,20000,2000), expand = 0) +
    scale_x_date(date_breaks = "6 months", date_labels = "%b-%y", date_minor_breaks = "3 months",
                 limits = as.Date(c("2024-04-01", "2031-04-01"), "%Y-%m-%d"), expand = 0,
    ) +
    guides(x = guide_axis(check.overlap = TRUE, n.dodge = 2)) +
    labs(y = "Queue Size", x = "Date"
         , title = "Simulated BC RTT waiting list for treatment specialty 100"
         , subtitle = "Average WL over 50 runs, with 95% point-wise confidence interval") +
    theme(axis.text.x = element_text(angle = 0),
          axis.line = element_line(color = "grey"),
          axis.ticks = element_line(color = "grey"),
          plot.margin = unit(c(2,5,2,2),"mm"))


#df_list2[[1]]



#Need to take out the waiting list size at last point of data, next point would be sustainable 65%,
#then next year would be sustainable 92%, then same each year.
# Need target capacity calc., then add extr capacity in capacity and sim.



# install.packages("writexl")
library(writexl)

# Ensure list elements are named (used as sheet names)
names(df_list) <- bc_total |> distinct(Specialty) |> pull()

write_xlsx(df_list, path = "./output/activity_plans/BC_total/bc_total.xlsx")


out_long <- bind_rows(df_list, .id = "source")

write_csv(out_long, "./output/activity_plans/BC_total/bc_total_long.csv")


