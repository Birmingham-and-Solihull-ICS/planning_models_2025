wl_simulator2 <-
    function(
        start_date = NULL,
        end_date = NULL,
        demand = 10,
        capacity = 11,
        waiting_list = NULL,
        withdrawal_prob = NA_real_,
        detailed_sim = FALSE
    ) {

        NHSRwaitinglist:::check_date(start_date, end_date, .allow_null = TRUE)
        NHSRwaitinglist:::check_class(demand, capacity, withdrawal_prob, .expected_class = "numeric")
        NHSRwaitinglist:::check_class(detailed_sim, .expected_class = "logical")
        if (!is.null(waiting_list)) NHSRwaitinglist:::check_wl(waiting_list)

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
                wl_simulated <- wl_join2(waiting_list, wl_simulated)
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
            schedule <- sim_schedule2(number_of_days, start_date, daily_capacity)

            wl_simulated <- wl_schedule2(wl_simulated, schedule)
        }

        return(wl_simulated)
    }

wl_simulated2 <- as.data.frame(wl_simulated2)

identical(wl_simulated_1, wl_simulated_2)


library(data.table)




wl_schedule2 <- function(
        waiting_list,
        schedule,
        referral_index = 1,
        removal_index = 2,
        unscheduled = FALSE,
        restore_types = TRUE  # Toggle strict type restoration
) {
    # Error handle
    NHSRwaitinglist:::check_wl(waiting_list, referral_index, removal_index)
    NHSRwaitinglist:::check_date(schedule)
    NHSRwaitinglist:::check_class(unscheduled, .expected_class = "logical")

    if (!inherits(schedule, "Date")) {
        schedule <- as.Date(schedule)
    }

    # Precompute column names
    referral_col <- names(waiting_list)[referral_index]
    removal_col  <- names(waiting_list)[removal_index]

    # Split waiters and removed
    wl <- waiting_list[is.na(waiting_list[[removal_col]]), ]
    wl_removed <- waiting_list[!is.na(waiting_list[[removal_col]]), ]
    rownames(wl) <- NULL

    if (!unscheduled) {
        # Local copies for faster in-loop access
        ref <- wl[[referral_col]]
        rem <- wl[[removal_col]]
        n_wl <- length(ref)

        i <- 1L
        for (op in schedule) {
            if (op > ref[i] & i <= n_wl) {
                rem[i] <- op
                i <- i + 1L
            }
        }

        # Only convert if needed
        if (!inherits(rem, "Date")) rem <- as.Date(rem)
        wl[[removal_col]] <- rem

        # Fast combine + sort
        library(data.table)
        updated_dt <- rbindlist(list(as.data.table(wl_removed), as.data.table(wl)), use.names = TRUE)
        setorderv(updated_dt, referral_col)

        updated_df <- as.data.frame(updated_dt, stringsAsFactors = FALSE)
        rownames(updated_df) <- seq_len(nrow(updated_df))

        # Optional strict type restoration
        if (restore_types) {
            for (col in seq_along(updated_df)) {
                if (is.factor(waiting_list[[col]])) {
                    updated_df[[col]] <- factor(updated_df[[col]], levels = levels(waiting_list[[col]]))
                } else {
                    class(updated_df[[col]]) <- class(waiting_list[[col]])
                }
            }
        }

        return(updated_df)

    } else {
        scheduled <- data.frame(schedule = schedule, scheduled = integer(length(schedule)))

        ref <- wl[[referral_col]]
        rem <- wl[[removal_col]]
        n_wl <- length(ref)

        i <- 1L
        j <- 0L
        for (op in schedule) {
            j <- j + 1L
            if (op > ref[i] & i <= n_wl) {
                rem[i] <- op
                i <- i + 1L
                scheduled[j, 2] <- 1L
            }
        }

        if (!inherits(rem, "Date")) rem <- as.Date(rem)
        wl[[removal_col]] <- rem

        updated_dt <- rbindlist(list(as.data.table(wl_removed), as.data.table(wl)), use.names = TRUE)
        setorderv(updated_dt, referral_col)

        updated_df <- as.data.frame(updated_dt, stringsAsFactors = FALSE)
        rownames(updated_df) <- seq_len(nrow(updated_df))

        if (restore_types) {
            for (col in seq_along(updated_df)) {
                if (is.factor(waiting_list[[col]])) {
                    updated_df[[col]] <- factor(updated_df[[col]], levels = levels(waiting_list[[col]]))
                } else {
                    class(updated_df[[col]]) <- class(waiting_list[[col]])
                }
            }
        }

        return(list(updated_df, scheduled))
    }
}





library(data.table)

wl_join2 <- function(wl_1, wl_2, referral_index = 1) {
    require(data.table)
    # Convert to data.table
    wl_1 <- as.data.table(wl_1)
    wl_2 <- as.data.table(wl_2)

    # Combine
    updated_list <- rbindlist(list(wl_1, wl_2), use.names = TRUE, fill = TRUE)

    # Get column name from position if referral_index is numeric
    if (is.numeric(referral_index)) {
        referral_index <- names(updated_list)[referral_index]
    }

    # Sort in place
    setorderv(updated_list, cols = referral_index)

    return(updated_list)
}


sim_schedule2 <- function(n_rows = 10, start_date = NULL, daily_capacity = 1) {
    if (is.null(start_date)) start_date <- Sys.Date()

    # Original logic: ceiling(seq(0, n_rows - 1, by = 1 / daily_capacity))
    # Vectorized equivalent:
    offsets <- ceiling(seq(0, n_rows - 1, by = 1 / daily_capacity))

    # Add offsets to start_date
    schedule <- start_date + offsets

    return(schedule)
}
