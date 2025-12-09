bsol_montecarlo_WL <-
    function(.data, seed, start_date_name = "start_date"
             , end_date_name = "end_date", adds_name = "added", removes_name="removed"
             , starting_wl = 0, run_id
    ){


        # Make sure date columns are right format
        if (!is(object = .data[[start_date_name]], "Date")) {
            .data[[start_date_name]] <- as.Date(.data[[start_date_name]], format = "%d/%m/%Y")
        }

        if (!is(object = .data[[end_date_name]], "Date")) {
            .data[[end_date_name]] <- as.Date(.data[[end_date_name]], format = "%d/%m/%Y")
        }


        # rows to iterate over in control table
        sim_period_n <- nrow(.data)

        if (!missing(seed)) { set.seed(seed) }

        sims <- vector("list", sim_period_n)

        seq_periods <-

            for (i in seq(1, sim_period_n)) {
                if (i == 1) {

                    #inpt <- unlist(.data[1,])

                    # handle starting waiting list
                    if (starting_wl > 0) { #Dump current waiting list figure into the day before the modelling period
                        #WL warm-up
                        wl_date <- as.POSIXlt(.data[[start_date_name]][i])
                        wl_date$mon <- wl_date$mon - 1
                        wl_date <- as.character(wl_date)

                        # wl_date <- as.POSIXlt(start_date)
                        # wl_date <- wl_date-86400 # number of seconds in a day
                        # wl_date <- as.character(wl_date)
                        #

                        # current_wl <-NHSRwaitinglist::wl_simulator(start_date = wl_date,
                        #                                            end_date = start_date,
                        #                                            demand = waiting_list_current,
                        #                                            capacity = waiting_list_current * 0.8)
                        #
                        current_wl <- data.frame(Referral = rep(as.Date(wl_date), starting_wl)
                                                 , Removal = rep(as.Date(NA), starting_wl)
                        )



                        sims[[i]] <- wl_simulator_cpp(as.Date(.data[[start_date_name]][i], format = "%d/%m/%Y")
                                                                   , as.Date(.data[[end_date_name]][i], format = "%d/%m/%Y")
                                                                   , as.integer(.data[[adds_name]][i])
                                                                   , as.integer(.data[[removes_name]][i])
                                                                   , waiting_list = current_wl)
                    } else {

                        sims[[i]] <- wl_simulator_cpp(as.Date(.data[[start_date_name]][i], format = "%d/%m/%Y")
                                                                   , as.Date(.data[[end_date_name]][i], format = "%d/%m/%Y")
                                                                   , as.integer(.data[[adds_name]][i])
                                                                   , as.integer(.data[[removes_name]][i])
                        )
                    }
                } else {

                   # if (!missing(seed)) {set.seed(seed)}

                    carry_over <- sims[[i - 1L]]
                    carry_over <- carry_over[is.na(carry_over$Removal), , drop = FALSE]


                    sims[[i]] <- wl_simulator_cpp(as.Date(.data[[start_date_name]][i], format = "%d/%m/%Y")
                                                               , as.Date(.data[[end_date_name]][i], format = "%d/%m/%Y")
                                                               , as.integer(.data[[adds_name]][i])
                                                               , as.integer(.data[[removes_name]][i])
                                                               ,  waiting_list =  sims[[i - 1L]]
                    )



                }
            }


            return(cbind(NHSRwaitinglist::wl_queue_size(sims[[NROW(.data)]]), run_id = run_id))

    }



bsol_montecarlo_WL2 <- function(.data, seed, start_date_name = "start_date",
                               end_date_name = "end_date", adds_name = "added",
                               removes_name = "removed", starting_wl = 0,
                               reference_date, run_id) {

    # Ensure date columns are Date objects
    if (!inherits(.data[[start_date_name]], "Date")) {
        .data[[start_date_name]] <- as.Date(.data[[start_date_name]], format = "%d/%m/%Y")
    }
    if (!inherits(.data[[end_date_name]], "Date")) {
        .data[[end_date_name]] <- as.Date(.data[[end_date_name]], format = "%d/%m/%Y")
    }

    sim_period_n <- nrow(.data)
    if (!missing(seed)) set.seed(seed)

    sims <- vector("list", sim_period_n)

    for (i in seq_len(sim_period_n)) {
        start_date_i <- .data[[start_date_name]][i]
        end_date_i <- .data[[end_date_name]][i]
        adds_i <- as.integer(.data[[adds_name]][i])
        removes_i <- as.integer(.data[[removes_name]][i])

        # For first iteration, handle starting waiting list
        if (i == 1 && starting_wl > 0) {
            wl_date <- start_date_i - 30  # approx 1 month before start
            current_wl <- data.frame(
                Referral = rep(wl_date, starting_wl),
                Removal = rep(as.Date(NA), starting_wl)
            )

            sims[[i]] <- wl_simulator_cpp(
                start_date = start_date_i,
                end_date = end_date_i,
                demand = adds_i,
                capacity = removes_i,
                waiting_list = current_wl
            )
        } else if (i == 1) {
            sims[[i]] <- wl_simulator_cpp(
                start_date = start_date_i,
                end_date = end_date_i,
                demand = adds_i,
                capacity = removes_i
            )
        } else {
            if (!missing(seed)) set.seed(seed)

            sims[[i]] <- wl_simulator_cpp(
                start_date = start_date_i,
                end_date = end_date_i,
                demand = adds_i,
                capacity = removes_i,
                waiting_list = sims[[i - 1]]
            )
        }
    }

    last_wl <- sims[[sim_period_n]]

    if (!missing(reference_date)) {
        referral_after_t3 <- ifelse(last_wl$Referral < reference_date, 0, 1)

        t4_date <- tryCatch({
            last_wl[referral_after_t3 == 0, ] |>
                dplyr::arrange(desc(Removal)) |>
                dplyr::slice_head(n = 1) |>
                dplyr::pull(Removal)
        }, error = function(e) NA)

        return(cbind(
            NHSRwaitinglist::wl_queue_size(last_wl),
            t4_date = as.Date(t4_date, origin = "1970-01-01"),
            run_id = run_id
        ))
    } else {
        return(cbind(
            NHSRwaitinglist::wl_queue_size(last_wl),
            run_id = run_id
        ))
    }
}




bsol_montecarlo_WL3 <-
    function(.data, seed,
             start_date_name = "start_date",
             end_date_name   = "end_date",
             adds_name       = "added",    # <-- set to "Referrals" for your data
             removes_name    = "removed",  # <-- set to "Removals"  for your data
             starting_wl     = 0L,
             run_id,
             # If TRUE, set the last day of each period’s size to the actual carry-over
             force_boundary_alignment = TRUE,
             # Burn-in window for initial backlog dispersion
             burn_in_days = 90L) {

        # -- Ensure date columns are Date --
        if (!inherits(.data[[start_date_name]], "Date")) {
            .data[[start_date_name]] <- as.Date(.data[[start_date_name]])
        }
        if (!inherits(.data[[end_date_name]], "Date")) {
            .data[[end_date_name]] <- as.Date(.data[[end_date_name]])
        }

        sim_period_n <- nrow(.data)

        # === A. Seed only once ===
        if (!missing(seed)) set.seed(seed)

        sims       <- vector("list", sim_period_n)
        sizes_list <- vector("list", sim_period_n)

        # --- Robust column pickers based on types (no name assumptions) ---
        get_date_col_index <- function(df) {
            idx <- which(vapply(df, function(x) inherits(x, "Date"), logical(1)))
            if (length(idx) == 0L) stop("wl_queue_size output: no Date-like column found.")
            idx[1]
        }
        get_size_col_index <- function(df, date_idx) {
            idx <- which(vapply(df, is.numeric, logical(1)))
            idx <- setdiff(idx, date_idx)  # exclude the Date column if numeric under the hood
            if (length(idx) == 0L) stop("wl_queue_size output: no numeric size column found.")
            idx[1]
        }

        for (i in seq_len(sim_period_n)) {
            start_i   <- .data[[start_date_name]][i]
            end_i     <- .data[[end_date_name]][i]
            adds_i    <- as.integer(.data[[adds_name]][i])
            removes_i <- as.integer(.data[[removes_name]][i])

            if (i == 1L) {
                # === C. Spread initial backlog over a pre-period window (burn-in) ===
                if (starting_wl > 0L) {
                    base_dates <- seq.Date(from = start_i - burn_in_days, to = start_i - 1L, by = "day")
                    current_wl <- data.frame(
                        Referral = sample(base_dates, starting_wl, replace = TRUE),
                        Removal  = as.Date(NA)
                    )
                    sims[[i]] <- wl_simulator_cpp(start_i, end_i, adds_i, removes_i, waiting_list = current_wl)
                } else {
                    sims[[i]] <- wl_simulator_cpp(start_i, end_i, adds_i, removes_i)
                }

                # Queue sizes for period i, trimmed to [start_i, end_i] (removes burn-in from output)
                sizes_i <- NHSRwaitinglist::wl_queue_size(sims[[i]])
                d_idx   <- get_date_col_index(sizes_i)
                s_idx   <- get_size_col_index(sizes_i, d_idx)
                in_rng  <- sizes_i[[d_idx]] >= start_i & sizes_i[[d_idx]] <= end_i
                sizes_i <- sizes_i[in_rng, , drop = FALSE]
                sizes_list[[i]] <- sizes_i

            } else {
                # Previous period simulation result
                prev <- sims[[i - 1L]]
                stopifnot(is.data.frame(prev), all(c("Referral","Removal") %in% names(prev)))

                # === B. Carry forward ONLY unresolved cases as input to next period ===
                carry_over <- prev[is.na(prev$Removal), c("Referral","Removal"), drop = FALSE]

                # Optional: align the end-of-period size to carry-over to visually avoid a boundary jump
                if (force_boundary_alignment) {
                    prev_sizes <- sizes_list[[i - 1L]]
                    if (!is.null(prev_sizes) && nrow(prev_sizes) > 0L) {
                        pd_idx <- get_date_col_index(prev_sizes)
                        ps_idx <- get_size_col_index(prev_sizes, pd_idx)
                        prev_sizes[nrow(prev_sizes), ps_idx] <- nrow(carry_over)
                        sizes_list[[i - 1L]] <- prev_sizes
                    }
                }

                # Simulate current period with carried backlog
                sims[[i]] <- wl_simulator_cpp(start_i, end_i, adds_i, removes_i, waiting_list = carry_over)

                # Queue sizes for current period, trimmed to [start_i, end_i]
                sizes_i <- NHSRwaitinglist::wl_queue_size(sims[[i]])
                d_idx   <- get_date_col_index(sizes_i)
                s_idx   <- get_size_col_index(sizes_i, d_idx)
                in_rng  <- sizes_i[[d_idx]] >= start_i & sizes_i[[d_idx]] <= end_i
                sizes_i <- sizes_i[in_rng, , drop = FALSE]
                sizes_list[[i]] <- sizes_i
            }
        }

        # Build continuous series by binding all trimmed period outputs.
        # (Boundary day is retained; if alignment is on, last day equals next carry-over.)
        sizes_all <- do.call(rbind, sizes_list)
        sizes_all$run_id <- run_id
        return(sizes_all)
    }

