NHSRwaitinglist::wl_simulator <-
function(
        start_date = NULL,
        end_date = NULL,
        demand = 10,
        capacity = 11,
        waiting_list = NULL,
        withdrawal_prob = NA_real_,
        detailed_sim = FALSE
) {

    check_date(start_date, end_date, .allow_null = TRUE)
    check_class(demand, capacity, withdrawal_prob, .expected_class = "numeric")
    check_class(detailed_sim, .expected_class = "logical")
    if (!is.null(waiting_list)) check_wl(waiting_list)

    # Fix Start and End Dates
    if (is.null(start_date)) {
        start_date <- Sys.Date()
    }
    if (is.null(end_date)) {
        end_date <- start_date + 31
    }

    start_date <- as.Date(start_date)
    end_date <- as.Date(end_date)
    number_of_days <- as.numeric(end_date) - as.numeric(start_date)

    total_demand <- demand * number_of_days / 7
    daily_capacity <- capacity / 7

    # allowing for fluctuations in predicted demand give a arrival list
    realized_demand <- stats::rpois(1, total_demand)
    referral <-
        sample(
            seq(as.Date(start_date), as.Date(end_date), by = "day"),
            realized_demand,
            replace = TRUE
        )

    referral <- referral[order(referral)]
    removal <- rep(as.Date(NA), length(referral))

    if (!detailed_sim) {
        if (is.na(withdrawal_prob)) {
            wl_simulated <- data.frame(
                "Referral" = referral,
                "Removal" = removal
            )
        } else {
            withdrawal <-
                referral + rgeom(length(referral), prob = withdrawal_prob) + 1
            withdrawal[withdrawal > end_date] <- NA
            wl_simulated <- data.frame(
                "Referral" = referral,
                "Removal" = removal,
                "Withdrawal" = withdrawal
            )
        }

        if (!is.null(waiting_list)) {
            wl_simulated <- wl_join(waiting_list, wl_simulated)
        }
    }
    if (detailed_sim) {
        if (is.na(withdrawal_prob)) {
            withdrawal_prob <- 0.1
        }
        withdrawal <- referral + rgeom(length(referral), prob = withdrawal_prob) + 1
        withdrawal[withdrawal > end_date] <- NA
        wl_simulated <- sim_patients(length(referral), start_date)
        wl_simulated$Referral <- referral
        wl_simulated$Withdrawal <- withdrawal
    }

    # create an operating schedule
    if (daily_capacity > 0) {
        schedule <- sim_schedule(number_of_days, start_date, daily_capacity)

        wl_simulated <- wl_schedule(wl_simulated, schedule)
    }

    return(wl_simulated)
}


NHSRwaitinglist::wl_schedule <-
function(
        waiting_list,
        schedule,
        referral_index = 1,
        removal_index = 2,
        unscheduled = FALSE
) {

    # Error handle
    check_wl(waiting_list, referral_index, removal_index)
    check_date(schedule)
    check_class(unscheduled, .expected_class = "logical")

    if (!inherits(schedule, "Date")) {
        schedule <- as.Date(schedule)
    }

    # split waiters and removed
    wl <- waiting_list[is.na(waiting_list[, removal_index]), ]
    wl_removed <- waiting_list[!(is.na(waiting_list[, removal_index])), ]
    rownames(wl) <- NULL

    # schedule
    if (!unscheduled) {
        i <- 1
        for (op in as.list(schedule)) {
            if (op > wl[i, referral_index] && i <= nrow(wl)) {
                wl[i, removal_index] <- op
                i <- i + 1
            }
        }

        # Ensure date format
        #wl$Removal <- as.Date(wl$Removal)
        wl[, removal_index] <- as.Date(wl[, removal_index])

        # recombine to update list
        updated_list <- rbind(wl_removed, wl)
        updated_list <- updated_list[order(updated_list[, referral_index]), ]
        return(updated_list)
    } else {
        scheduled <- data.frame(
            schedule = schedule,
            scheduled = rep(0, length(schedule))
        )
        i <- 1
        j <- 0
        for (op in as.list(schedule)) {
            j <- j + 1
            if (op > wl[i, referral_index] && i <= nrow(wl)) {
                wl[i, removal_index] <- op
                i <- i + 1
                scheduled[j, 2] <- 1
            }
        }



        # Ensure date format
        #wl$Removal <- as.Date(wl$Removal)
        wl[, removal_index] <- as.Date(wl[, removal_index])

        # recombine to update list
        updated_list <- rbind(wl_removed, wl)
        updated_list <- updated_list[order(updated_list[, referral_index]), ]

        # scheduled[scheduled$scheduled = 1, 1]

        return(list(updated_list, scheduled))
    }
}

NHSRwaitinglist::wl_join <-
function(wl_1, wl_2, referral_index = 1) {
    check_wl(wl_1, referral_index)
    check_wl(wl_2, referral_index)

    # combine and sort to update list
    updated_list <- rbind(wl_1, wl_2)
    updated_list <- updated_list[order(updated_list[, referral_index]), ]
    rownames(updated_list) <- NULL
    return(updated_list)
}