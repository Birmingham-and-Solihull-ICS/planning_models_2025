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
