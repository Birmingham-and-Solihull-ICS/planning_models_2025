
# Packages
library(ggplot2)
library(dplyr)
library(purrr)
library(scales)
library(glue)

# --- Sanity checks: all lists are the same length ---
n <- length(df_list)
stopifnot(
    length(mc_bind_cur) == n,
    length(mc_agg_cur)  == n,
    length(mc_bind_rel) == n,
    length(mc_agg_rel)  == n
)

# --- Output folder ---
out_dir <- "output/activity_plans/BC_total"
dir.create(out_dir, showWarnings = FALSE)

# Optional: annotation text per plot (or just keep single string)
ann_labels <- rep("Target waiting list for 92%\nat 18 weeks 2028/29", n)

# --- Helpers for rounded y-scale ---
round_up <- function(x, to) ceiling(x / to) * to

choose_step <- function(ymax) {
    # Choose a “nice” step in 100s or 1000s based on magnitude
    if (is.na(ymax) || ymax <= 0) return(100)
    if (ymax <= 1000)      return(100)
    else if (ymax <= 2000) return(250)
    else if (ymax <= 6000) return(500)
    else if (ymax <= 12000) return(1000)
    else if (ymax <= 30000) return(2000)
    else if (ymax <= 100000) return(5000)
    else return(10000)
}

# --- Plotting function ---

make_plot <- function(
        i,
        target_row = NULL,
        target_date = NULL,
        target_date_fmt = "%d/%m/%Y",        # set to NULL if target_date is already Date
        ann_label = ann_labels[i]
) {
    df        <- df_list[[i]]
    cur_bind  <- mc_bind_cur[[i]]
    cur_agg   <- mc_agg_cur[[i]]
    rel_bind  <- mc_bind_rel[[i]]
    rel_agg   <- mc_agg_rel[[i]]

    # --- Coerce x variables to Date (robust to character/POSIXct) ---
    coerce_date <- function(x, fmt = NULL) {
        if (inherits(x, "Date")) return(x)
        if (inherits(x, "POSIXt")) return(as.Date(x))
        if (is.character(x)) {
            if (!is.null(fmt)) return(as.Date(x, fmt))
            # Fallback: try ISO then UK
            out <- suppressWarnings(as.Date(x))                    # ISO
            if (any(is.na(out))) out <- suppressWarnings(as.Date(x, "%d/%m/%Y")) # UK
            return(out)
        }
        # As last resort
        suppressWarnings(as.Date(x))
    }

    df$start_date <- coerce_date(df$start_date)
    df$end_date   <- coerce_date(df$end_date)

    cur_bind$dates <- coerce_date(cur_bind$dates)
    cur_agg$dates  <- coerce_date(cur_agg$dates)
    rel_bind$dates <- coerce_date(rel_bind$dates)
    rel_agg$dates  <- coerce_date(rel_agg$dates)

    # Ensure aggregated frames are sorted (ribbon likes ordered x)
    cur_agg <- dplyr::arrange(cur_agg, dates)
    rel_agg <- dplyr::arrange(rel_agg, dates)

    # --- Target row/date selection ---
    idx <- NA_integer_
    if (!is.null(target_row)) {
        idx <- target_row
    } else if (!is.null(target_date)) {
        td <- if (inherits(target_date, "Date")) target_date
        else coerce_date(target_date, fmt = target_date_fmt)
        idx <- match(as.Date(td), as.Date(df$start_date))
    }
    if (is.na(idx) || idx < 1 || idx > nrow(df)) {
        warning(glue::glue("Plot {i}: target row/date not found; defaulting to row 21."))
        idx <- min(21L, nrow(df))
    }

    hline <- suppressWarnings(as.numeric(df$target_wl[idx]))
    cutoff_date <- as.Date('2026-04-01', "%Y-%m-%d")
        #if (!is.null(target_date)) coerce_date(target_date, fmt = target_date_fmt)
        #else df$start_date[idx]
    ann_x <- if (!is.null(target_date)) coerce_date(target_date, fmt = target_date_fmt)
    else as.Date("2025-01-01")

    # --- Dynamic y-axis ---
    round_up <- function(x, to) ceiling(x / to) * to
    choose_step <- function(ymax) {
        if (is.na(ymax) || ymax <= 0) return(100)
        if (ymax <= 100)       20 else
        if (ymax <= 500)       50 else
          if (ymax <= 1500)       100 else
            if (ymax <= 6000)       500 else
                if (ymax <= 12000)       1000 else
                if (ymax <= 30000)     2000 else
                    if (ymax <= 100000)    5000 else 10000
    }

    ymax_raw <- suppressWarnings(max(
        df$Waiting.list.size,
        as.numeric(cur_bind$queue_size),
        as.numeric(rel_bind$queue_size),
        as.numeric(cur_agg$upper_95CI),
        as.numeric(rel_agg$upper_95CI),
        hline,
        na.rm = TRUE
    ))
    step  <- choose_step(ymax_raw)
    y_top <- round_up(ymax_raw, step)
    ann_y <- pmin(hline * 0.6, y_top * 0.9)

    # Title with Specialty
    spec <- dplyr::coalesce(
        df$Specialty[1] %||% NA
        #df$specialty[1] %||% NA
    )
    title_base <- "Simulated BC - total waiting list"
    title_txt  <- if (!is.na(spec)) paste0(title_base, " for treatment specialty ", spec) else title_base

    ggplot() +
        geom_line(
            aes(x = end_date, y = Waiting.list.size),
            col = "black",
            data = dplyr::filter(df, start_date < cutoff_date)
        ) +
        geom_line(
            aes(x = dates, y = queue_size, group = run_id),
            alpha = 0.4, col = "#A6CEE3", data = as.data.frame(cur_bind)
        ) +
        geom_ribbon(
            aes(x = dates, y = mean_q, ymin = lower_95CI, ymax = upper_95CI),
            alpha = 0.5, data = cur_agg, fill = "#1F78B4"
        ) +
        geom_line(aes(x = dates, y = mean_q), data = cur_agg, col = "#1F78B4") +
        geom_line(
            aes(x = dates, y = queue_size, group = run_id),
            alpha = 0.3, col = "#B2DF8A", data = as.data.frame(rel_bind)
        ) +
        geom_ribbon(
            aes(x = dates, y = mean_q, ymin = lower_95CI, ymax = upper_95CI),
            alpha = 0.5, data = rel_agg, fill = "#33A02C"
        ) +
        geom_line(aes(x = dates, y = mean_q), data = rel_agg, col = "#33A02C") +
        geom_hline(yintercept = hline, col = "#FF7F00") +
        annotate("text", x = ann_x, y = ann_y, label = ann_label,
                 col = "#FF7F00", hjust = 0.1, vjust = 0.1) +
        scale_y_continuous(
            labels = scales::comma,
            breaks = seq(0, y_top, by = step),
            limits = c(0, y_top),
            expand = c(0, 0)
        ) +
        # Keep your overall limits; all x's are now Date
        scale_x_date(
            date_breaks = "6 months", date_labels = "%b-%y",
            date_minor_breaks = "3 months",
            limits = as.Date(c("2024-04-01", "2031-04-01")),
            expand = c(0, 0)
        ) +
        guides(x = guide_axis(check.overlap = TRUE, n.dodge = 2)) +
        labs(
            y = "Queue Size", x = "Date",
            title = title_txt,
            subtitle = "Average WL over 50 runs, with 95% point-wise confidence interval"
        ) +
        theme(
            axis.text.x = element_text(angle = 0),
            axis.line = element_line(color = "grey"),
            axis.ticks = element_line(color = "grey"),
            plot.margin = unit(c(2, 5, 2, 2), "mm")
        )
}

# --- Choose how to drive the target per plot ---

## Option A: provide an index per plot
 target_rows <- rep(21, n)  # one row index per dataset

## Option B: provide a date per plot (e.g., from existing target_dts$startdate)
# target_dates <- target_dts$startdate[seq_len(n)]

# Build plots:
# A) If using row indices:
p_list <- map2(seq_len(n), target_rows, ~ make_plot(.x, target_row = .y))

# B) If using dates:
# p_list <- map2(seq_len(n), target_dates, ~ make_plot(.x, target_date = .y))

# C) If neither supplied, defaults to row 22:
p_list <- map(seq_len(n), make_plot)

# --- File naming helper ---
build_name <- function(i) {
    nm <- NULL
    if ("Specialty" %in% names(df_list[[i]])) nm <- df_list[[i]]$Specialty[1]
    else if ("specialty" %in% names(df_list[[i]])) nm <- df_list[[i]]$specialty[1]
    else if ("org" %in% names(df_list[[i]])) nm <- df_list[[i]]$org[1]

    safe <- if (!is.null(nm)) gsub("[^A-Za-z0-9_\\-]+", "_", nm) else sprintf("plot_%02d", i)
    file.path(out_dir, paste0("rtt_", safe, ".png"))
}

# --- Save plots ---
walk2(p_list, seq_along(p_list), ~ ggsave(
    filename = build_name(.y), plot = .x,
    width = 9, height = 6, dpi = 300, bg = "white"
))

