library(Rcpp)

calc_relief_capacity2 <-
    function(
        demand,
        queue_size,
        target_queue_size,
        time_to_target = 26,
        num_referrals = 0,
        cv_demand = 0
    ) {
        NHSRwaitinglist:::check_class(demand, queue_size, target_queue_size, time_to_target)

        # Compute adjustment only where num_referrals > 0
        adjust <- ifelse(num_referrals > 0,
                         2 * demand * cv_demand / sqrt(num_referrals),
                         NA_real_)

        # Apply adjustment conditionally
        demand <- ifelse(num_referrals > 0 & adjust < 1,
                         demand / (1 - adjust),
                         demand)

        # Calculate relief capacity (vectorized)
        rel_cap <- demand + (queue_size - target_queue_size) / time_to_target
        return(rel_cap)
    }


elapsed_months <- function(end_date, start_date) {
    ed <- as.POSIXlt(end_date)
    sd <- as.POSIXlt(start_date)
    12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}


est_wait_performance <- function(demand, queue_size, target_wait){
    # Returns the compliance against waiting list target implied by demand, target_wait, and queue_length
    # Vectorized; will return Inf if target_queue_length == 0 and numerator > 0
    # and NaN/NA as per R rules for 0/0 or NA inputs.
    factor <- (demand * target_wait) / queue_size
    pexp(factor)

}

#
# wl_simulator <- function(
#         start_date = NULL,
#         end_date = NULL,
#         demand = 10,
#         capacity = 11,
#         waiting_list = NULL
# ) {
#     # Validate inputs
#     NHSRwaitinglist:::check_date(start_date, end_date, .allow_null = TRUE)
#     NHSRwaitinglist:::check_class(demand, capacity, .expected_class = "numeric")
#     if (!is.null(waiting_list)) NHSRwaitinglist:::check_wl(waiting_list)
#
#     # Set default dates
#     if (is.null(start_date)) start_date <- Sys.Date()
#     if (is.null(end_date)) end_date <- start_date + 31
#
#     start_date <- as.Date(start_date)
#     end_date <- as.Date(end_date)
#     number_of_days <- as.integer(end_date - start_date)
#
#     # Compute demand and capacity
#     total_demand <- demand * number_of_days / 7
#     daily_capacity <- capacity / 7
#
#     # Realized demand and referral dates
#     realized_demand <- stats::rpois(1, total_demand)
#     days_seq <- seq(start_date, end_date, by = "day")
#     referral <- sort(sample(days_seq, realized_demand, replace = TRUE))
#
#     # Initialize removal column
#     removal <- as.Date(rep(NA_integer_, length(referral)), origin = "1970-01-01")
#
#     # # Handle withdrawal
#     # if (is.na(withdrawal_prob)) withdrawal_prob <- 0.1
#     # withdrawal <- referral + rgeom(length(referral), prob = withdrawal_prob) + 1
#     # withdrawal[withdrawal > end_date] <- NA
#
#     # Build simulated waiting list
#     wl_simulated <- data.frame(
#         Referral = referral,
#         Removal = removal#,
#         # Withdrawal = withdrawal
#     )
#
#     # Merge with existing waiting list if provided
#     if (!is.null(waiting_list)) {
#         wl_simulated <- wl_join(waiting_list, wl_simulated)
#     }
#
#     # Create operating schedule if capacity > 0
#     if (daily_capacity > 0) {
#         schedule <- sim_schedule(number_of_days, start_date, daily_capacity)
#         wl_simulated <- wl_schedule(wl_simulated, schedule)
#     }
#
#     return(wl_simulated)
# }
#
#
#
# wl_simulator_fast <- function(
#         start_date,
#         end_date,
#         demand = 10,
#         capacity = 11,
#         waiting_list = NULL
# ) {
#     # Assume start_date and end_date are Date objects (convert once outside)
#     number_of_days <- as.integer(end_date - start_date)
#     if (number_of_days <= 0) return(waiting_list)
#
#     # Compute demand and capacity
#     total_demand <- demand * number_of_days / 7
#     daily_capacity <- capacity / 7
#
#     # Realized demand and referral dates
#     realized_demand <- stats::rpois(1, total_demand)
#     if (realized_demand > 0) {
#         # Faster sampling: sample positions, then map to days
#         days_seq <- seq.int(0, number_of_days)  # offsets
#         referral_offsets <- sample.int(length(days_seq), realized_demand, replace = TRUE)
#         referral <- start_date + referral_offsets
#     } else {
#         referral <- as.Date(integer(0), origin = "1970-01-01")
#     }
#
#     # Removal column preallocated
#     removal <- rep(NA, length(referral))
#
#     # Build simulated waiting list
#     wl_simulated <- data.frame(Referral = referral, Removal = removal)
#
#     # Merge with existing waiting list if provided
#     if (!is.null(waiting_list)) {
#         wl_simulated <- wl_join(waiting_list, wl_simulated)  # optimize wl_join separately
#     }
#
#     # Apply schedule if capacity > 0
#     if (daily_capacity > 0 && nrow(wl_simulated) > 0) {
#         schedule <- sim_schedule(number_of_days, start_date, daily_capacity)
#         wl_simulated <- wl_schedule(wl_simulated, schedule)  # optimize wl_schedule separately
#     }
#
#     wl_simulated
# }


# Build Rcpp function
#Rcpp::sourceCpp("wl_simulator2.cpp")



