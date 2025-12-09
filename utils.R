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
Rcpp::sourceCpp("wl_simulator2.cpp")





#' Mean wait required for a desired exceedance proportion at a given threshold
#'
#' For exponential waiting times, returns the mean wait Wbar such that
#' P(W > T) = p_exceed. Uses Wbar = T / ln(1 / p_exceed).
#'
#' @param threshold numeric (>0). The waiting time threshold T (e.g., weeks).
#' @param p_exceed numeric in (0,1). Desired proportion exceeding T.
#' @return numeric. The mean wait Wbar.
#' @examples
#' # P4 example: target T = 52 weeks
#' mean_wait_for_exceedance(52, p_exceed = 0.018)  # ~12.94 weeks (≈ a quarter of target)
#' mean_wait_for_exceedance(52, p_exceed = 0.002)  # ~8.37 weeks (≈ a sixth of target)
#' mean_wait_for_exceedance(52, p_exceed = 0.05)   # ~17.35 weeks
#'
#' # Vectorised input:
#' mean_wait_for_exceedance(52, p_exceed = c(0.10, 0.05, 0.018))
mean_wait_for_exceedance <- function(threshold, p_exceed) {
    if (any(!is.finite(threshold)) || any(threshold <= 0))
        stop("`threshold` must be a finite, positive numeric value (or vector).")
    if (any(!is.finite(p_exceed)) || any(p_exceed <= 0 | p_exceed >= 1))
        stop("`p_exceed` must be a finite numeric in the open interval (0, 1).")
    threshold / log(1 / p_exceed)  # log() is natural log in R
}


mean_wait_for_meeting <- function(threshold, p_meeting) {
    if (any(!is.finite(threshold)) || any(threshold <= 0))
        stop("`threshold` must be a finite, positive numeric value (or vector).")
    if (any(!is.finite(p_meeting)) || any(p_meeting <= 0 | p_meeting >= 1))
        stop("`p_exceed` must be a finite numeric in the open interval (0, 1).")
    threshold / log(1 / (1 - p_meeting))  # log() is natural log in R
}


# -- Optional companion functions ---------------------------------------------

# Given mean wait and desired exceedance proportion, return the threshold T
# using T = - Wbar * ln(p_exceed).
threshold_for_exceedance <- function(mean_wait, p_exceed) {
    if (any(!is.finite(mean_wait)) || any(mean_wait <= 0))
        stop("`mean_wait` must be a finite, positive numeric value.")
    if (any(!is.finite(p_exceed)) || any(p_exceed <= 0 | p_exceed >= 1))
        stop("`p_exceed` must be in (0, 1).")
    - mean_wait * log(p_exceed)
}

threshold_for_meeting <- function(mean_wait, p_meeting) {
    if (any(!is.finite(mean_wait)) || any(mean_wait <= 0))
        stop("`mean_wait` must be a finite, positive numeric value.")
    if (any(!is.finite(p_meeting)) || any(p_meeting <= 0 | p_meeting >= 1))
        stop("`p_exceed` must be in (0, 1).")
    - mean_wait * log(1-p_meeting)
}


# Given mean wait and threshold, return exceedance proportion p = P(W > T)
# using p = exp(- T / Wbar).
exceedance_prob <- function(mean_wait, threshold) {
    if (any(!is.finite(mean_wait)) || any(mean_wait <= 0))
        stop("`mean_wait` must be a finite, positive numeric value.")
    if (any(!is.finite(threshold)) || any(threshold <= 0))
        stop("`threshold` must be a finite, positive numeric value.")
    exp(-threshold / mean_wait)
}

exceedance_prob(8.2, threshold = 18)


# The "factor" k = T / mean = ln(1/p)
factor_for_exceedance <- function(p_exceed) {
    if (any(!is.finite(p_exceed)) || any(p_exceed <= 0 | p_exceed >= 1)) stop("p_exceed in (0,1)")
    log(1 / p_exceed)
}

factor_for_meeting <- function(p_meeting) {
    if (any(!is.finite(p_meeting)) || any(p_meeting <= 0 | p_meeting >= 1)) stop("p_exceed in (0,1)")
    log(1 / (1 - p_meeting))
}

# Convenience: mean as fraction of target = 1 / k
mean_fraction_of_target <- function(p_exceed) 1 / factor_for_exceedance(p_exceed)

