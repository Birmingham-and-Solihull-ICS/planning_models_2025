set.seed(1)
current_wl <- data.frame(
    Referral = rep(as.Date("30/09/2025", "%d/%m/%Y"), 12000),
    Removal = rep(as.Date(NA), 12000)
)

start_date <- as.Date("2025-10-01")
end_date <- as.Date("2025-10-31")

cpp <- wl_simulator_cpp(start_date, end_date, demand = 1000, capacity = 1100,
                        waiting_list = current_wl)

nhsr <- NHSRwaitinglist::wl_simulator(start_date, end_date, demand = 1000, capacity = 1100,
                                      waiting_list = current_wl)

sum(is.na(cpp$Removal))
sum(is.na(nhsr$Removal))








library(Rcpp)

cppFunction('
#include <Rcpp.h>
using namespace Rcpp;

// [[Rcpp::export]]
DataFrame wl_simulator_cpp(Date start_date, Date end_date,
                           double demand, double capacity,
                           DataFrame waiting_list = DataFrame()) {

  // 1️⃣ Handle NA start_date / end_date
  if (DateVector::is_na(start_date)) {
    start_date = Rcpp::Date(Rcpp::as<std::string>(Rcpp::Function("Sys.Date")()));
  }
  if (DateVector::is_na(end_date)) {
    end_date = start_date + 31;
  }

  int number_of_days = static_cast<int>(end_date - start_date) + 1;

  // 2️⃣ Create date sequence
  DateVector day_seq(number_of_days);
  for (int i = 0; i < number_of_days; ++i) {
    day_seq[i] = start_date + i;
  }

  // 3️⃣ Initialize referral and removal vectors
  NumericVector referral(number_of_days, demand);
  NumericVector removal(number_of_days, NA_REAL);

  // 4️⃣ Handle waiting list: assume "Amount" column exists if DataFrame provided
  NumericVector waiting_amount;
  if (waiting_list.size() > 0) {
    if (!waiting_list.containsElementNamed("Amount")) {
      stop("waiting_list must have a column named 'Amount'");
    }
    waiting_amount = waiting_list["Amount"];
    Rcout << "Waiting list has " << waiting_list.nrows() << " rows." << std::endl;
  }

  // 5️⃣ Daily processing loop
  NumericVector sched(number_of_days, 0.0);
  int p = 0; // pointer in waiting_amount
  for (int day = 0; day < number_of_days; ++day) {
    double remaining_capacity = capacity;

    // Serve waiting list first
    while (p < waiting_amount.size() && remaining_capacity > 0) {
      double serve = std::min(remaining_capacity, waiting_amount[p]);
      waiting_amount[p] -= serve;
      remaining_capacity -= serve;
      removal[day] += serve;
      if (waiting_amount[p] <= 0) p++;  // move to next waiting item
    }

    // Then serve daily referrals
    double serve_referral = std::min(remaining_capacity, referral[day]);
    removal[day] += serve_referral;
    sched[day] = serve_referral;
  }

  // 6️⃣ Return results as DataFrame
  return DataFrame::create(
    Named("Date")     = day_seq,
    Named("Referral") = referral,
    Named("Removal")  = removal
  );
}
')


Rcpp::sourceCpp("wl_simulator.cpp")


current_wl <- data.frame(
    Referral = rep(as.Date("30/09/2025", "%d/%m/%Y"), 12000),
    Removal = rep(as.Date(NA), 12000)
)



# Example call
start_date <- as.Date("2025-10-01")
end_date <- as.Date("2025-10-31")

start_cpp <- Sys.time()
cpp <- wl_simulator_cpp(start_date, end_date, demand = 1000, capacity = 1100
                        , waiting_list = current_wl)
end_cpp <- Sys.time()



start_fast <- Sys.time()
fast <- wl_simulator_fast(start_date, end_date, demand = 1000, capacity = 1100
                          , waiting_list = current_wl)
end_fast <- Sys.time()


start_2 <- Sys.time()
v2 <- wl_simulator(start_date, end_date, demand = 1000, capacity = 1100
                   , waiting_list = current_wl)
end_2 <- Sys.time()


start_nhsr <- Sys.time()
nhsr <- NHSRwaitinglist::wl_simulator(start_date, end_date, demand = 1000, capacity = 1100
                                      , waiting_list = current_wl)
end_nhsr <- Sys.time()



end_cpp - start_cpp
end_fast - start_fast
end_2 - start_2
end_nhsr - start_nhsr


cpp_wl <- wl_queue_size(cpp)
fast_wl <- wl_queue_size(fast)
v2_wl <- wl_queue_size(v2)
nhsr_wl <- wl_queue_size(nhsr)

cpp_wl[32,]
fast_wl[34,]
v2_wl[33,]
nhsr_wl[32,]


b <- cpp |>
    group_by(Removal) |>
    count() |>
    ungroup() |>
    summarise(median(n))


a <- nhsr |>
    group_by(Removal) |>
    count() |>
    ungroup() |>
    summarise(median(n))
