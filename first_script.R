#Generate artificial Gastro scenario using real data control parameters
library(tidyverse)
library(NHSRwaitinglist)
library(scales)
library(ggtext)
library(zoo)
source("monte_carlo_func.R")
source("utils.R")
library(purrr)
library(furrr)
library(progressr)


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
        startdate = as.Date(c('01/04/2026', '01/04/2028'), '%d/%m/%Y'),
        enddate = as.Date(c('31/03/2027', '01/04/2029'), '%d/%m/%Y'),
        descr = c("RTT65%", "RTT92%")
    )

population_growth <-
tibble::tribble(
          ~start_date,    ~end_date, ~ratio_increase, ~population,
         "01/10/2025", "31/03/2026",               1,     1590793,
         "01/04/2026", "31/03/2027",         1.00271, 1596056.425,
         "01/04/2027", "31/03/2028",        1.005589, 1601503.998,
         "01/04/2028", "31/03/2029",        1.009093, 1607815.611,
         "01/04/2029", "31/03/2030",        1.013369, 1615161.207,
         "01/04/2030", "31/03/2031",        1.017844, 1622704.257
         )



# Convert Year to Date
population_growth$start_date <- as.Date(population_growth$start_date, '%d/%m/%Y')
population_growth$end_date <- as.Date(population_growth$end_date, '%d/%m/%Y')




######## This bit is not relevant now, but leaving in#################################
# Generate full monthly sequence
full_months <- tibble(start_date = seq(last_data_date %m+% months(1),
                                 max(population_growth$start_date) %m+% months(12) - 1,
                                 by = "month"))



# Join and fill values forward
extended_df <- full_months %>%
    left_join(population_growth, by = "start_date") %>%
    fill(ratio_increase, population, .direction = "down")

# Manual correction as don't have time to sort logic
extended_df[1:3,]$ratio_increase <- 1.00
extended_df[1:3,]$population <-  1590793

print(extended_df, n = 66)

#################################################################################


##################Load data#############################

library(readxl)
# Load dummy data
test_input <- read_excel("data/test_input.xlsx", .name_repair = "universal_quiet")
test_input <- read_excel("data/total_RTT.xlsx", .name_repair = "universal_quiet")
monthly_bsol <- read_excel("data/monthly_bsol.xlsx", .name_repair = "universal_quiet")

# Clear any float/double number formatting, as it trips rpois over.
test_input$Referrals <- as.integer(test_input$Referrals)
test_input$Removals <- as.integer(test_input$Removals)
test_input$Waiting.list.size <- as.integer(test_input$Waiting.list.size)

# Clear any float/double number formatting, as it trips rpois over.
monthly_bsol$Referrals <- as.integer(monthly_bsol$Referrals)
monthly_bsol$Removals <- as.integer(monthly_bsol$Removals)
monthly_bsol$Waiting.list.size <- as.integer(monthly_bsol$Waiting.list.size)


##################Prepare data###########################

############### Convert to list of data.frames per specialty#####################
# Split data frame by Specialty, into a list.
# Each slot in the list is a data.frame for a specialty
df_list <- monthly_bsol |>  #test_input %>%
    mutate(ratio_increase = as.numeric(NA)) |>
    group_by(Specialty) %>%
    group_split()


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

print(df_list[[1]], n = 50)

#### Populate values logically ####
######## Populate values ####

# fill in values, specialty
df_list <- map(df_list, function(.x) {
    spec <- first(.x$Specialty)
    .x$Specialty[is.na(.x$Specialty)] <- spec
    .x
})

# median capacity
df_list <- map(df_list, function(.x) {
    med <- as.integer(median(.x$Removals, na.rm = TRUE))
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



print(df_list[[1]], n = 50)
print(df_list[[2]], n = 50)


#### MC to end of 2025 ####
#Then can use to calculate target waiting lists

library(furrr)
library(future.apply)

# Parallel plan
plan(multisession, workers = 10)

#library(future.mirai)
#plan(mirai_multisession, workers = 6)

# Your simulation function (unchanged)
df <- df_list[[2]]
#rm(df)
sim_func <- function(df) {
    current_wl <- data.frame(
        Referral = rep(as.Date(df$start_date[18]), df$Waiting.list.size[18]),
        Removal = rep(as.Date(NA), df$Waiting.list.size[18])
    )

    sim <- wl_simulator(
        start_date = as.Date(df[19,]$start_date),
        end_date = as.Date(df[19,]$end_date),
        demand = df[19,]$Referrals,
        capacity = df[18,]$Removals, # project last point forward
        waiting_list = current_wl
    )

    data.frame(specialty = df$Specialty[1], queue = wl_queue_size(sim)[212, 2],
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

saveRDs(results, "./data/results_input2025_bsol_all.rds")

# Combine all specialties into one data frame
all_results <- bind_rows(results)

# Summarize mean and median queue per specialty
summary_results <- all_results %>%
    group_by(specialty) %>%
    summarise(
        mean_queue = mean(queue),
        median_queue = median(queue),
        .groups = "drop"
    )

summary_results

print(df_list[[1]], n = 50) # 15458 110
print(df_list[[2]], n = 50) # 163286 99999

# Update df_list row 19 with median_queue
df_list <- map(df_list, function(df) {
    spec <- df$Specialty[1]
    median_val <- round(summary_results$median_queue[summary_results$specialty == spec])
    df$Waiting.list.size[19] <- median_val
    df
})




# add lagged 1 column for waiting list at start of period
df_list <- map(df_list, ~ mutate(.x, Wl_start_lag = lag(Waiting.list.size, n = 1)))
# Imput 1


df_list <- map(df_list, function(.x) {
    .x$Wl_start_lag[1] <- .x$Wl_start_lag[2] + .x$Referrals[1] - .x$Removals[2]
    .x
})


########## Sustainable WL calcs #####################################

# run NHSR waiting list functions over each data.frame in list (specialty)
df_list <- map(df_list, function(.x) {
    .x$target_wl_65 <- calc_target_queue_size(
        demand = .x$Referrals,
        target_wait = 18,
        factor = qexp(0.65)
    )

    .x$target_wl_65or6 <- .x$target_wl_65 * 0.94

    .x$target_wl_92 <- calc_target_queue_size(
        demand = .x$Referrals,
        target_wait = 18,
        factor = qexp(0.92)
    )
    .x
})

print(df_list[[1]], n = 50)
print(df_list[[2]], n = 50)

# target capacity from NHSRwaitinglist
df_list <- map(df_list, function(.x) {

    spec <- first(.x$Specialty)

    cv_demand <- tf_summary %>% filter(Specialty == spec) %>% pull(cv_demand)
    cv_capacity <- tf_summary %>% filter(Specialty == spec) %>% pull(cv_capacity)

    .x$target_capacity_65 <-
        calc_target_capacity(
            demand = .x$Referrals,
            target_wait = 18,
            factor = qexp(0.65),
            cv_demand = cv_demand,
            cv_capacity = cv_capacity)

    .x$target_capacity_92 <-
        calc_target_capacity(
            demand = .x$Referrals,
            target_wait = 18,
            factor = qexp(0.92),
            cv_demand = cv_demand,
            cv_capacity = cv_capacity)

    .x
})


# target capacity from NHSRwaitinglist
# rel_capacity <- map_dfr
df_list <- map(df_list, function(.x) {

    spec <- first(.x$Specialty)

    cv_demand <- tf_summary %>% filter(Specialty == spec) %>% pull(cv_demand)
    cv_capacity <- tf_summary %>% filter(Specialty == spec) %>% pull(cv_capacity)

    # .x$relief_capacity_65 <-
    #     calc_relief_capacity(
    #         demand = .x$Referrals,
    #         queue_size = .x$Waiting.list.size[19],
    #         target_queue_size = .x$target_wl_65[20],
    #         time_to_target = 12,
    #         cv_demand = cv_demand
    #         )
    #
    # .x$relief_capacity_65or6 <-
    #     calc_relief_capacity(
    #         demand = .x$Referrals,
    #         queue_size = .x$Waiting.list.size[19],
    #         target_queue_size = .x$target_wl_65or6[20],
    #         time_to_target = 12,
    #         cv_demand = cv_demand
    #     )

    .x$relief_capacity_92 <-
        calc_relief_capacity2(
            demand = .x$Referrals,
            queue_size = .x$Wl_start_lag[20],
            target_queue_size = .x$target_wl_92[22],
            time_to_target = 104,
            num_referrals = round(.x$Referrals/4.33),
            cv_demand = cv_demand
        )

    .x
})





######### Now add new capacity column #########################

df_list <- map(df_list, function(.x) {

    # .x$calc_capacity <- ifelse(.x$start_date >= target_dts[2,]$startdate, .x$relief_capacity_92,
    #                            ifelse(.x$start_date >= target_dts[1,]$startdate & .x$relief_capacity_65 >= .x$Removals,
    #                                   .x$relief_capacity_65,
    #                                   .x$Removals))
    #
    # .x$calc_capacity <- ifelse(.x$start_date > target_dts[1,]$enddate & .x$start_date <= target_dts[2,]$enddate,
    #                            .x$relief_capacity_92,
    #                             ifelse(.x$start_date >= target_dts[1,]$startdate & .x$start_date <= target_dts[6,]$enddate >= .x$Removals,
    #                                    .x$relief_capacity_65or6,
    #                            .x$Removals))
    #
    .x$calc_capacity <- round(ifelse(.x$start_date >= target_dts[1,]$startdate & .x$start_date < target_dts[2,]$enddate,
                               .x$relief_capacity_92,
                               ifelse(.x$start_date < target_dts[1,]$startdate, .x$Removals,
                               .x$Referrals))) # have to keep pace with demand

    .x
})

View(df_list[[1]])

target_dts[1,]$startdate

write.csv(df_list[[2]], "./verify2.csv")

# probably need to feed first one with end of 25/26 values, and second one with end of 2026/27 values.
# need then to move each year, so is it a lagged variable?


################### target for 2026/27################
# Row 18 has last data


targets_17 <- map_dfr(df_list, function(df){
    df[21,c(1,8,9)]
})



########## Relief capacity at last point ######################################
# # NOt working at present
# test_input$relief_cap <-
#     calc_relief_capacity(
#         demand = test_input$Referrals,
#         queue_size = test_input$`Waiting list size`,
#         time_to_target = 12,
#         target_queue_size = test_input$target_wl_92
#     )
################################################################################


#### Monte Carlo simulation for WLs#############################################


# Simulations
df <- df_list[[1]]

library(furrr)
library(future.mirai)

df_list2 <- df_list[2]
# Set up parallel plan (multisession works on most OS)
plan(multisession, workers = 6)  # Adjust workers to your CPU cores
#plan(mirai_multisession, workers = 6)
start_time <- Sys.time()

sim_results <- map(df_list2, function(df) {
    # Extract starting_wl from first row
    start_wl <- df[1, "Waiting.list.size", drop = TRUE]
    if (is.na(start_wl)) start_wl <- 0

    # Inner parallel map (optional)
    future_map(1:1, function(i) {
        bsol_montecarlo_WL(
            .data = df,
            run_id = i,
            start_date_name = "start_date",
            end_date_name = "end_date",
            adds_name = "Referrals",
            removes_name = "calc_capacity",
            starting_wl = start_wl
        )
    }, .options = furrr_options(seed = NULL))
}
)

end_time <- Sys.time()

saveRDS(sim_results, "./data/bsol_sims.rds")

inner_parallel <- end_time - start_time
#inner_sequential <- end_time - start_time

# end parallel sessions
plan(sequential)


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


# Testing plot
ggplot(as.data.frame(mc_bind[[1]]), aes(x = dates)) +
    geom_line(aes(y = queue_size, group = run_id), alpha = 0.5, col = "grey") +
    geom_ribbon(aes(y = mean_q, ymin = lower_95CI, ymax = upper_95CI)
                , alpha = 0.5, data = mc_agg[[1]], fill = "red") +
    geom_line(aes(y = mean_q), data = mc_agg[[1]], col = "black") +
    geom_hline(yintercept = as.numeric(df_list[[1]][24,]$target_wl_92), col = "blue") +
    labs(y = "Queue Size", x = "Date"
         , title = "Simulated waiting list after raising capacity"
         , subtitle = "Average WL over 50 runs, with 95% confidence interval")


# Testing plot
ggplot(as.data.frame(mc_bind[[2]]), aes(x = dates)) +
    geom_line(aes(y = queue_size, group = run_id), alpha = 0.5, col = "grey") +
    geom_ribbon(aes(y = mean_q, ymin = lower_95CI, ymax = upper_95CI)
                , alpha = 0.5, data = mc_agg[[2]], fill = "red") +
    geom_line(aes(y = mean_q), data = mc_agg[[2]], col = "black") +
    geom_hline(yintercept = as.numeric(df_list[[2]][24,]$target_wl_92), col = "blue") +
    labs(y = "Queue Size", x = "Date"
         , title = "Simulated waiting list after raising capacity"
         , subtitle = "Average WL over 50 runs, with 95% confidence interval")


#Need to take out the waiting list size at last point of data, next point would be sustainable 65%,
#then next year would be sustainable 92%, then same each year.
# Need target capacity calc., then add extr capacity in capacity and sim.
df[,1]

map(df_list, function(df){
    df[df$start_date == as.Date("2025-10-01", "%Y-%m-%d")]
})


cv_demand  <- sd(monthly_bsol$Referrals, na.rm = TRUE) / mean(monthly_bsol$Referrals, na.rm = TRUE)
cv_capacity <- sd(monthly_bsol$Removals, na.rm = TRUE) / mean(monthly_bsol$Removals, na.rm = TRUE)

calc_target_capacity(demand = monthly_bsol[18,]$Referrals,
                     target_wait = 18,
                     factor = qexp(0.92),
                     cv_demand = cv_demand,
                     cv_capacity = cv_demand
                    )

target_q_65 <-
    calc_target_queue_size(demand = (monthly_bsol[18,]$Referrals/4.33) * 1.01
                           , target_wait = 18
                           , factor = qexp(0.65))

target_q_92 <-
    calc_target_queue_size(demand = (monthly_bsol[18,]$Referrals/4.33) * 1.03
                           , target_wait = 18
                           , factor = qexp(0.92))



calc_relief_capacity(
    demand = (monthly_bsol[18,]$Referrals/4.33),
    queue_size = monthly_bsol[18,]$Waiting.list.size,
    time_to_target = 130,
    target_queue_size = target_q,
    cv_demand = cv_demand
)


