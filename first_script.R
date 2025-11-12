#Generate artificial Gastro scenario using real data control parameters
library(tidyverse)
library(NHSRwaitinglist)
library(scales)
library(ggtext)
library(zoo)
source("monte_carlo_func.R")
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
        startdate = as.Date(c('01/04/2026', '01/04/2027'), '%d/%m/%Y'),
        enddate = as.Date(c('31/03/2026', '01/04/2031'), '%d/%m/%Y'),
        descr = c("RTT 85%", "RTT95%")
    )

population_growth <-
tibble::tribble(
          ~start_date,    ~end_date, ~ratio_increase, ~population,
         "01/10/2025", "31/12/2025",               1,     1590793,
         "01/01/2026", "31/12/2026",         1.00271, 1596056.425,
         "01/01/2027", "31/12/2027",        1.005589, 1601503.998,
         "01/01/2028", "31/12/2028",        1.009093, 1607815.611,
         "01/01/2029", "31/12/2029",        1.013369, 1615161.207,
         "01/01/2030", "31/12/2030",        1.017844, 1622704.257
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

extended_df

#################################################################################


##################Load data#############################

library(readxl)
# Load dummy data
test_input <- read_excel("data/test_input.xlsx", .name_repair = "universal_quiet")

# Clear any float/double number formatting, as it trips rpois over.
test_input$Referrals <- as.integer(test_input$Referrals)
test_input$Removals <- as.integer(test_input$Removals)
test_input$Waiting.list.size <- as.integer(test_input$Waiting.list.size)



##################Prepare data###########################

############### Convert to list of data.frames per specialty#####################
# Split data frame by Specialty, into a list.
# Each slot in the list is a data.frame for a specialty
df_list <- test_input %>%
    mutate(ratio_increase = as.numeric(NA)) |>
    group_by(Specialty) %>%
    group_split()


# Calculate coefficients of variation (how each list behaves)
# cv_demand and cv_capacity for each specialty
tf_summary <- map_dfr(df_list, function(.x) {
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

#### Popualte values ####

# fill in values, median capacity
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



###################WL stats#############################
# No working at present
#######################################################
summary_figures <-
    test_input |>
    group_by(Specialty) |>
    summarise(
        median_referrals = median(Referrals),
        median_removals = median(Removals),
        median_wl = median(Waiting.list.size),
        last_referrals = last(Referrals),
        last_removals = last(Removals),
        last_wl = last(Waiting.list.size)
    )


summary_stats <-
    wl_stats(summary_figures[1,],
             target = 18)


########## Sustainable WL calcs #####################################

# run NHSR waiting list functions over each data.frame in list (specialty)
df_list <- map(df_list, function(.x) {
    .x$target_wl_65 <- calc_target_queue_size(
        demand = .x$Referrals,
        target_wait = 18,
        factor = qexp(0.65)
    )

    .x$target_wl_92 <- calc_target_queue_size(
        demand = .x$Referrals,
        target_wait = 18,
        factor = qexp(0.92)
    )
    .x
})



# target capacity friom NHSRwaitinglist
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




# probably need to feed first one with end of 25/26 values, and second one with end of 2026/27 values.
# need then to move each year, so is it a lagged variable?


########## Relief capacity at last point ######################################
# NOt working at present
test_input$relief_cap <-
    calc_relief_capacity(
        demand = test_input$Referrals,
        queue_size = test_input$`Waiting list size`,
        time_to_target = 12,
        target_queue_size = test_input$target_wl_92
    )
################################################################################


#### Monte Carlo simulation for WLs#############################################


# Simulations
df <- df_list[[1]]

sim_results <- map(df_list, function(df) {
    # Take starting_wl from first row of the data frame
    start_wl <- df[1, "Waiting.list.size", drop = TRUE]  # Adjust column name if needed

    map(1:1, function(i) {
             bsol_montecarlo_WL(
            .data = df,
            run_id = i,                # Different run_id
            start_date_name = "start_date",
            end_date_name = "end_date",
            adds_name = "Referrals",
            removes_name = "Removals",
            starting_wl = start_wl   # Use the first row value
        )
    })
})


library(furrr)

# Set up parallel plan (multisession works on most OS)
plan(multisession, workers = 6)  # Adjust workers to your CPU cores
start_time <- Sys.time()
sim_results <- future_map(df_list, function(df) {
    # Extract starting_wl from first row
    start_wl <- df[1, "Waiting.list.size", drop = TRUE]
    if (is.na(start_wl)) start_wl <- 0

    # Inner parallel map (optional)
    future_map(1:10, function(i) {
        bsol_montecarlo_WL(
            .data = df,
            run_id = i,
            start_date_name = "start_date",
            end_date_name = "end_date",
            adds_name = "Referrals",
            removes_name = "Removals",
            starting_wl = start_wl
        )
    })
}, .progress = TRUE
, .options = furrr_options(seed = NULL))

end_time <- Sys.time()

#inner_parallel <- end_time - start_time
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
    labs(y = "Queue Size", x = "Date"
         , title = "Simulated waiting list after raising capacity"
         , subtitle = "Average WL over 50 runs, with 95% confidence interval")
