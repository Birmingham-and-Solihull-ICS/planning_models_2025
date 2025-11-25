# install.packages("devtools")
# devtools::install_github("nhs-bnssg-analytics/NHSRtt")
# devtools::install_github("nhs-bnssg-analytics/RTT_compartmental_modelling")
library(dplyr)
library(NHSRtt)
library(RTTshiny)
library(lubridate)
library(tidyr)

calibration_start <- as.Date("2024-08-01") # change
calibration_end <- as.Date("2025-09-30")
prediction_start <- calibration_end + 1
prediction_end <- as.Date("2031-03-31")
commissioner_code <- "15E"
max_months_waited <- 12 # I am only interested in waiting time bins up to 12 months
target_percentile <- 0.92
target_week <- 18
referral_scenario <- tibble(
  referrals_scenario = "Medium_referrals",
  referral_change = 1
)


period_lkp <- tibble(
  period = seq(
    from = calibration_start,
    to = floor_date(prediction_end, unit = "months"),
    by = "months"
  )
) |>
  mutate(period_id = dplyr::row_number())

monthly_rtt <- NHSRtt::get_rtt_data(
  date_start = calibration_start,
  date_end = calibration_end,
  commissioner_org_codes = commissioner_code,
  specialty_codes = "C_999",
  show_progress = FALSE # can change this to TRUE to see progress
)

processed_rtt <- monthly_rtt |>
  mutate(
    months_waited_id = NHSRtt::convert_months_waited_to_id(
      months_waited,
      max_months_waited = max_months_waited
    ),
    # the internal function on line 53 needs "trust" and "specialty" fields to work (when running this for a batch, these can be replaced with real values)
    trust = "dummy",
    specialty = "dummy"
  ) |>
  left_join(period_lkp, by = "period") |>
  summarise(
    value = sum(value),
    .by = c("trust", "specialty", "period_id", "months_waited_id", "type")
  )

# calculate historical renege rates
renege_target <- processed_rtt |>
  # this is an internal function that does a load of data processing before calibrating the parameters;
  # though in this case it is used to create a big table of monthly data so we can obtain the monthly reneges
  RTTshiny:::calibrate_parameters(
    max_months_waited = 12,
    redistribute_m0_reneges = FALSE,
    referrals_uplift = NULL,
    full_breakdown = TRUE,
    allow_negative_params = TRUE
  ) |>
  dplyr::select("trust", "specialty", "params") |>
  tidyr::unnest("params") |>
  dplyr::mutate(
    reneges = case_when(
      # in modelling, referrals are uplifted where reneges are negative in the first month - so floor reneges to 0
      .data$reneges < 0 & .data$months_waited_id == 0 ~ 0,
      .default = .data$reneges
    )
  ) |>
  summarise(
    renege_proportion = sum(.data$reneges) /
      (sum(.data$reneges) + sum(.data$treatments)),
    .by = c("trust", "specialty", "period_id")
  ) |>
  dplyr::mutate(
    renege_proportion = case_when(
      .data$renege_proportion < 0 ~ NA_real_,
      .default = .data$renege_proportion
    )
  ) |>
  summarise(
    renege_proportion = stats::median(
      .data$renege_proportion,
      na.rm = TRUE
    ),
    .by = c("trust", "specialty")
  ) |>
  mutate(
    renege_proportion = case_when(
      is.na(.data$renege_proportion) ~ 0.15,
      .default = .data$renege_proportion
    )
  )


# calculate the number of months for projection period
forecast_months <- lubridate::interval(
  lubridate::floor_date(
    calibration_end,
    unit = "months"
  ) %m+%
    months(1),
  prediction_end
) %/%
  months(1) +
  1 # the plus 1 makes is inclusive of the final month

# this normally gives a warning about negative reneges
current <- processed_rtt |>
  left_join(period_lkp, by = "period_id") |>
  RTTshiny:::append_current_status(
    max_months_waited = max_months_waited,
    percentile = target_percentile,
    percentile_month = RTTshiny:::convert_weeks_to_months(target_week)
  ) |>
  # add the referrals scenarios
  dplyr::cross_join(referral_scenario) |>
  mutate(id = dplyr::row_number())

# calculate s profiles for the calibration period
s_given <- processed_rtt |>
  left_join(period_lkp, by = "period_id") |>
  RTTshiny:::calculate_s_given(
    max_months_waited = max_months_waited,
    method = "median"
  )

optimised_projections <- current |>
  # add historic s
  left_join(
    s_given,
    by = c("trust", "specialty")
  ) |>
  # add historic renege rates by specialty
  left_join(
    renege_target |>
      dplyr::select("trust", "specialty", "renege_proportion"),
    by = c("trust", "specialty")
  ) |>
  # calculate steady state demand
  mutate(
    referrals_ss = .data$referrals_t1 +
      ((.data$referrals_t1 * .data$referral_change / 100) *
        forecast_months /
        12),
    target = .data$renege_proportion,
    ss_calcs = purrr::pmap(
      list(
        ref_ss = .data$referrals_ss,
        targ = .data$target,
        par = .data$params,
        s = .data$s_given,
        id = .data$id
      ),
      \(ref_ss, targ, par, s, id) {
        # here is where the steady state calculation occurs
        out <- RTTshiny:::append_steady_state(
          referrals = ref_ss,
          target = targ,
          renege_params = par$renege_param,
          percentile = target_percentile,
          target_time = target_week,
          s_given = s,
          method = "lp",
          tolerance = 0.005
        )

        return(out)
      }
    )
  ) |>
  unnest("ss_calcs") |>
  mutate(
    current_vs_ss_wl_ratio = round(
      .data$incompletes_t0 / .data$incompletes_ss,
      2
    ),
    monthly_removals = (.data$incompletes_t0 -
      .data$incompletes_ss) /
      forecast_months,
    referrals_scenario = gsub(
      "_referrals",
      "",
      .data$referrals_scenario
    )
  )

# here is where the counterfactual is calculated
# add in the counterfactual reneges and wl size
wl_t0 <- processed_rtt |>
  left_join(period_lkp, by = "period_id") |>
  filter(
    .data$type == "Incomplete",
    .data$period == max(.data$period)
  ) |>
  select(!c("period", "period_id", "type")) |>
  rename(incompletes = "value") |>
  tidyr::nest(wl_t0 = c("months_waited_id", "incompletes"))


optimised_projections <- optimised_projections |>
  left_join(wl_t0, by = c("trust", "specialty")) |>
  mutate(
    id = dplyr::row_number(),
    counterfactual = purrr::pmap(
      list(
        cap = .data$capacity_t1,
        ref_start = .data$referrals_t1,
        ref_end = .data$referrals_ss,
        inc = .data$wl_t0,
        par = .data$params,
        t = .data$trust,
        sp = .data$specialty,
        id = .data$id
      ),
      \(cap, ref_start, ref_end, inc, par, t, sp, id) {
        counterf <- RTTshiny:::append_counterfactual(
          capacity = cap,
          referrals_start = ref_start,
          referrals_end = ref_end,
          incompletes_t0 = inc,
          renege_capacity_params = par,
          forecast_months = forecast_months,
          target_week = target_week
        )
        return(counterf)
      }
    )
  ) |>
  tidyr::unnest("counterfactual") |>
  dplyr::select(
    !c(
      "params",
      "referral_change",
      "id",
      "renege_proportion",
      "target",
      "wl_ss",
      "wl_t0",
      "s_given",
      "id"
    )
  ) |>
  mutate(
    referrals_counterf = .data$referrals_ss,
    capacity_counterf = .data$capacity_t1
  ) |>
  distinct() |>
  dplyr::relocate(
    dplyr::all_of(
      c(
        "referrals_counterf",
        "capacity_counterf",
        "reneges_counterf",
        "incompletes_counterf",
        "perf_counterf"
      )
    ),
    .before = "referrals_ss"
  ) |>
  dplyr::relocate(
    "referrals_scenario",
    .after = "specialty"
  )

# View(optimised_projections)

write.csv(
  optimised_projections,
  "ad_hoc/commissioning_example/birm_example.csv",
  row.names = FALSE
)
