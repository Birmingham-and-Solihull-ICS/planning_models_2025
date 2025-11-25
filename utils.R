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




wl_simulator <- function(
        start_date = NULL,
        end_date = NULL,
        demand = 10,
        capacity = 11,
        waiting_list = NULL
) {
    # Validate inputs
    NHSRwaitinglist:::check_date(start_date, end_date, .allow_null = TRUE)
    NHSRwaitinglist:::check_class(demand, capacity, .expected_class = "numeric")
    if (!is.null(waiting_list)) NHSRwaitinglist:::check_wl(waiting_list)

    # Set default dates
    if (is.null(start_date)) start_date <- Sys.Date()
    if (is.null(end_date)) end_date <- start_date + 31

    start_date <- as.Date(start_date)
    end_date <- as.Date(end_date)
    number_of_days <- as.integer(end_date - start_date)

    # Compute demand and capacity
    total_demand <- demand * number_of_days / 7
    daily_capacity <- capacity / 7

    # Realized demand and referral dates
    realized_demand <- stats::rpois(1, total_demand)
    days_seq <- seq(start_date, end_date, by = "day")
    referral <- sort(sample(days_seq, realized_demand, replace = TRUE))

    # Initialize removal column
    removal <- as.Date(rep(NA_integer_, length(referral)), origin = "1970-01-01")

    # # Handle withdrawal
    # if (is.na(withdrawal_prob)) withdrawal_prob <- 0.1
    # withdrawal <- referral + rgeom(length(referral), prob = withdrawal_prob) + 1
    # withdrawal[withdrawal > end_date] <- NA

    # Build simulated waiting list
    wl_simulated <- data.frame(
        Referral = referral,
        Removal = removal#,
        # Withdrawal = withdrawal
    )

    # Merge with existing waiting list if provided
    if (!is.null(waiting_list)) {
        wl_simulated <- wl_join(waiting_list, wl_simulated)
    }

    # Create operating schedule if capacity > 0
    if (daily_capacity > 0) {
        schedule <- sim_schedule(number_of_days, start_date, daily_capacity)
        wl_simulated <- wl_schedule(wl_simulated, schedule)
    }

    return(wl_simulated)
}

